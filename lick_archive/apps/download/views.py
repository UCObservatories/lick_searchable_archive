"""The views that make up the lick archive query app."""

import logging
logger = logging.getLogger(__name__)

from pathlib import Path

from rest_framework.generics import RetrieveAPIView
from rest_framework import status
from django.http import FileResponse

from lick_archive.db.db_utils import create_db_engine
from lick_archive.db.archive_schema import FileMetadata
from lick_archive.utils.django_utils import log_request_debug

from lick_archive.config.archive_config import ArchiveConfigFile
lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

from lick_archive.apps.query.api import QueryAPIFilterBackend, SQLAlchemyQuerySet, QuerySerializer, QueryAPIView

# SQLAlchemy likes its engine to have a global lifetime.
_db_engine = create_db_engine(user=lick_archive_config.database.db_query_user, database=lick_archive_config.database.archive_db)




class DownloadSingleView(QueryAPIView, RetrieveAPIView):
    """A view for getting the header for a specific fits file in the archive."""
    filter_backends = [QueryAPIFilterBackend]
    lookup_url_kwarg = "file"
    lookup_field = "filename"
    required_attributes = ["filename"]
    allowed_result_attributes = ["filename"]
    allowed_sort_attributes = ["id"]

    def __init__(self):
        super().__init__(_db_engine, FileMetadata)

    def retrieve(self, request, *args, **kwargs):
        log_request_debug(request)

        file_metadata = super().get_object()
        logger.debug(f"Using X-SendFile value of '{file_metadata.filename}'")
        xsendfile_headers = {"X-Sendfile": file_metadata.filename,
                             "Content-Type": "image/fits"}
        response = FileResponse()
        response.status_code = status.HTTP_200_OK
        response.headers = xsendfile_headers
        return response
