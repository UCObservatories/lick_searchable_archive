"""The views that make up the lick archive query app."""

import logging
logger = logging.getLogger(__name__)

from pathlib import Path

from rest_framework.generics import RetrieveAPIView, GenericAPIView
from rest_framework import status
from rest_framework.parsers import JSONParser
from rest_framework.exceptions import ParseError, APIException, NotFound

from django.http import FileResponse, StreamingHttpResponse

from lick_archive.db.db_utils import create_db_engine
from lick_archive.db.archive_schema import FileMetadata
from lick_archive.utils.django_utils import log_request_debug
from lick_archive.metadata.metadata_utils import parse_file_name

from lick_archive.config.archive_config import ArchiveConfigFile
lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

from lick_archive.apps.query.api import QueryAPIFilterBackend, QueryAPIView
from lick_archive.metadata.data_dictionary import MAX_FILENAME_SIZE, MAX_FILENAME_BATCH

from lick_archive.apps.download.tarfile_stream import TarFileStream

# SQLAlchemy likes its engine to have a global lifetime.
_db_engine = create_db_engine(user=lick_archive_config.database.db_query_user, database=lick_archive_config.database.archive_db)



class DownloadSingleView(QueryAPIView, RetrieveAPIView):
    """A view for getting the header for a specific fits file in the archive."""
    filter_backends = [QueryAPIFilterBackend]
    lookup_url_kwarg = "file"
    lookup_field = "filename"
    required_attributes = ["filename"]
    allowed_result_attributes = ["filename","instrument"]
    allowed_sort_attributes = ["id"]

    def __init__(self):
        super().__init__(_db_engine, FileMetadata)

    def retrieve(self, request, *args, **kwargs):
        log_request_debug(request)

        file_metadata = super().get_object()
        logger.debug(f"Using X-SendFile value of '{file_metadata.filename}'")
        xsendfile_headers = {"X-Sendfile": file_metadata.filename,
                             "Content-Type": lick_archive_config.file_types[file_metadata["instrument"]]}
        response = FileResponse()
        response.status_code = status.HTTP_200_OK
        response.headers = xsendfile_headers
        return response

class DownloadMultiView(QueryAPIView, GenericAPIView):
    """A view for downloading a tarball of multiple files in the archive."""
    filter_backends = [QueryAPIFilterBackend]
    required_attributes = ["filename"]
    allowed_result_attributes = ["filename","file_size"]
    allowed_sort_attributes = ["filename"]
    batch_size = MAX_FILENAME_BATCH
    parser_classes = [JSONParser]

    def __init__(self):
        super().__init__(_db_engine, FileMetadata)

    def post(self, request, *args, **kwargs):
        """Handle a post request to download files. The API expects a JSON list
        of archive filenames."""

        log_request_debug(request)

        # Valiadate the incomming request.
        self._validate_json(request)

        # Validate that the the files in request, and return their full paths.
        files = self._get_validated_files(request.data)
        archive_names = self._get_archive_names(files)
        tarfile_name = self.get_filename(files[0], files[-1])

        tarball_stream = TarFileStream(tarfile_name,files, arcfiles=archive_names, enable_gzip=True)

        headers = {"Content-Type": "application/gzip",
                   "Content-Disposition": f"attachment; filename={tarfile_name}"}
        return StreamingHttpResponse(streaming_content=tarball_stream,status=status.HTTP_200_OK,headers=headers)

    def get_filename(self, first_file : Path, last_file : Path):
        """Create the filename to use for the tarball.
        
        Args:
        first_file: The first file in the sorted list of filenames.
        last_file:   The last file in the sorted list of filenames.
        """

        date1, instr1 = parse_file_name(first_file)
        date2, instr2 = parse_file_name(last_file)

        if date1 == date2:
            date_portion = date1
        else:
            date_portion = date1 + "-" + date2

        if instr1 == instr2:
            instr_portion = instr1
        else:
            instr_portion = instr1 + "-" + instr2
        return f"data-{date_portion}-{instr_portion}.tar.gz"

    def _get_archive_names(self, files : list[Path]):
        """Create the filenames that will be used in the resulting archive file.
        These are a single level directory name that will preserve the uniqueness of each file,
        even if they are from different nights or instruments.
        """
        archive_names = []
        for file in files:
            date_str, instr = parse_file_name(file)          
            archive_names.append(f"data-{date_str}-{instr}/{file.name}")
        return archive_names

    def _validate_json(self, request):
        """Validate the passed in JSON.
        The DRF JSONParser will validate that the request is JSON formatted,
        but this validates that it contains the expected data.
        """

        # Make sure the JSON is a list
        if not isinstance(request.data, list):
            raise ParseError(detail="Expected JSON list of filenames as input")
        
        # Make sure the list doesn't exceed our maximum allowed number of files
        if len(request.data) > lick_archive_config.download.max_tarball_files:
            raise ParseError(detail=f"List of filenames exceeds maximum length of {lick_archive_config.download.max_tarball_files}.")
        
        # Make sure each entry is a string, that it's not emtpy, and not too long.
        for i, file in enumerate(request.data):
            if not isinstance(file,str):
                raise ParseError(detail=f"List of filename contained non-string value at index {i}")
            if len(file) > MAX_FILENAME_SIZE:
                raise ParseError(detail=f"List of filename contained filename longer than {MAX_FILENAME_SIZE} characters at index {i}")
            if len(file) < 0:
                raise ParseError(detail=f"List of filename contained empty filename at index {i}")
        return
    
    def _get_validated_files(self, files : list[str]) -> list[Path]:
        """Validate the incomming list of files. This ensures that the files exist,
        that the user is authorized to receive them, and that maximum size
        constraints are met."""
        
        next_index = 0
        resulting_files = []
        total_size = 0
        # The maximum size in the config file is specified in MiB
        maximum_size = lick_archive_config.download.max_tarball_size * (2**20)

        # Go through the passed in files in batches, sorting each batch for comparison against
        # a sorted query on the file names.
        while next_index < len(files):
            next_batch = sorted(files[next_index:next_index+self.batch_size])
            if len(next_batch) > 0:

                # Prepare a queryset to find the given files, using the Query app's API
                # to properly filter and handle proprietary access
                self.request.validated_query = {"filename": ["in", next_batch],
                                                "sort": ["filename"] }

                queryset = self.filter_queryset(self.get_queryset())
                queryset = queryset.values(*self.allowed_result_attributes)

                # Get the next batch of results
                results = queryset[next_index:next_index+self.batch_size]
                logger.debug(f"Results: {results}")

                # Make sure each desired file was found,m and make sure we don't exceed the maximum allowed combined file size
                for i, file in enumerate(next_batch):
                    if i >= len(results) or file not in results[i]["filename"]:
                        # One of the files wasn't found, either it was invalid or we don't have access to it
                        raise NotFound(detail=f"Filename {file} was not found in the archive or the user does not have permissions to download it.")
                    total_size += results[i]["file_size"]
                    if total_size > maximum_size:
                        raise APIException(detail=f"Total size of all files exceeded maximum of {lick_archive_config.download.max_tarball_size} MiB")
                    resulting_files.append(Path(results[i]["filename"]))
            next_index += self.batch_size

        return resulting_files

