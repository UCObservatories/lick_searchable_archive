"""The views that make up the lick archive query app."""

import logging
logger = logging.getLogger(__name__)

from pathlib import Path

from rest_framework.generics import ListAPIView, RetrieveAPIView
from rest_framework.exceptions import APIException,NotFound
from rest_framework.renderers import BaseRenderer
from rest_framework import status

from lick_archive.utils.django_utils import log_request_debug
from lick_archive.db.db_utils import create_db_engine
from lick_archive.metadata.data_dictionary import api_capabilities
from lick_archive.db.archive_schema import FileMetadata

from lick_archive.config.archive_config import ArchiveConfigFile
lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

from .sqlalchemy_django_utils import SQLAlchemyQuerySet, SQLAlchemyORMSerializer
from .query_api import QueryAPIFilterBackend, QueryAPIPagination, QuerySerializer, HeaderSerializer

# SQLAlchemy likes its engine to have a global lifetime.
_db_engine = create_db_engine(user=lick_archive_config.database.db_query_user, database=lick_archive_config.database.archive_db)

class QueryView(ListAPIView):
    """View that integrates the archive Query API with SQL Alchemy"""
    pagination_class = QueryAPIPagination
    filter_backends = [QueryAPIFilterBackend]
    serializer_class = SQLAlchemyORMSerializer
    required_attributes = list(api_capabilities['required']['db_name'])
    allowed_sort_attributes = list(api_capabilities['sort']['db_name'])
    allowed_result_attributes = list(api_capabilities['result']['db_name'])


    def get_queryset(self):
        return SQLAlchemyQuerySet(_db_engine, FileMetadata)

    def list(self, request, *args, **kwargs):
        """Performs a query based on a request. The query is handled by the
        QueryAPIPagination, and QueryAPIFilterBackend classes. 
        This class performs post processing of database results.

        The post processing involves:
        Replacing the full path in the filename to the relative path. (Hiding the
        mount point of the archive file system from clients).

        Replacing the filename returned as "header" with a URL that can be used to
        access a plaintext version of the header.
        
        Args:
        request (rest_framework.requests.Request): The request specifying the query.        
        args (list):     Additional arguments to the view.
        **kwargs (dict): Additional keyword arguments to the view.
        
        Return (rest_framework.response.Response): The processed response from the query.
        """
        log_request_debug(request)

        # Validate the query using a serializer
        serializer = QuerySerializer(data=request.query_params, view=self)
        logger.debug(f"QueryParams {request.query_params}")
        try:
            serializer.is_valid(raise_exception=True)
        except Exception as e:
            logger.error(f"QueryParams {request.query_params}", exc_info=True)
            raise

        # Store the validated results in the request to be passed to paginators and filters
        request.validated_query = serializer.validated_data

        # Use the superclass method to utilize the DRF's infrastructure
        response = super().list(request, *args, **kwargs)
        if response.status_code == status.HTTP_200_OK:
            # A count query doesn't have a results entry
            if 'results' in response.data:
                # Filter header URLS to have the propper format and
                # both header and filename entries to be relative paths
                for record in response.data['results']:
                    if "header" in record:
                        filepath = Path(record['header'])
                        relative_path = filepath.relative_to(lick_archive_config.ingest.archive_root_dir)
                        header_url = lick_archive_config.query.file_header_url_format.format(relative_path)
                        record["header"] = header_url
                    if "filename" in record:
                        record["filename"] = str(Path(record['filename']).relative_to(lick_archive_config.ingest.archive_root_dir))
        return response

class PlainTextRenderer(BaseRenderer):
    """A renderer for rendering FITS headers in plain text."""
    media_type = 'text_plain'
    format='txt'
    charset='ascii'

    def render(self, data, accepted_media_type=None, renderer_context=None):
        # Return the plain text if the response is successfull.
        # Otherwise return the status code, reason phrase, and error message returned in the data
        if renderer_context is not None and "response" in renderer_context:
            response = renderer_context["response"]
            if response.status_code == status.HTTP_200_OK:
                return data + "\n" if data[-1] != "\n" else data
            elif isinstance(data, dict):
                return f"Status Code: {response.status_code}: {response.reason_phrase}\n\n{data['detail']}\n"
            else:
                return f"Status Code: {response.status_code}: {response.reason_phrase}\n"
        # If we can't get the response, give up let the django/drf deal with it
        else:
            raise APIException(detail="Internal Error", code=status.HTTP_500_INTERNAL_SERVER_ERROR)

class HeaderView(RetrieveAPIView):
    """A view for getting the header for a specific fits file in the archive."""
    renderer_classes = [PlainTextRenderer]
    filter_backends = [QueryAPIFilterBackend]
    serializer_class = HeaderSerializer
    lookup_url_kwarg = 'file'
    lookup_field = 'filename'
    required_attributes = ['filename']
    allowed_result_attributes = ["header"]
    allowed_sort_attributes = ["id"]

    def get_queryset(self):
        return SQLAlchemyQuerySet(_db_engine, FileMetadata)

    def get_object(self):
        log_request_debug(self.request)

        # Validate request using query serializer
        if self.lookup_url_kwarg not in self.kwargs:
            raise APIException(f"No {self.lookup_url_kwarg} specified.", code=status.HTTP_400_BAD_REQUEST)
        value = self.kwargs[self.lookup_url_kwarg]
        data = {self.lookup_field: value}
        serializer = QuerySerializer(data=data, view=self)
        try:
            serializer.is_valid(raise_exception=True)
        except Exception as e:
            logger.error(f"Failed to validate {self.lookup_field}.", exc_info=True)
            raise

        # Store the validated results in the request to be passed to paginators and filters
        self.request.validated_query = serializer.validated_data

        logger.info(f"Getting object for {self.lookup_field} = {serializer.validated_data[self.lookup_field]}")

        # Let the superclass filter the query set and then use that
        # to get the object.

        try:
            queryset = self.filter_queryset(self.get_queryset())
            results = queryset[0:]
        except Exception as e:
            logger.error(f"Failed to get object from database for {self.lookup_field} = {serializer.validated_data[self.lookup_field]}: {e}", exc_info=True)
            raise  APIException(detail="Failed to query archive database.", code=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if len(results) == 0:
            logger.error(f"{self.lookup_field} = {serializer.validated_data[self.lookup_field]} not found.")
            raise NotFound(detail="File not found")
        elif len(results) > 1:
            logger.error(f"Duplicate matches found for {self.lookup_field} = {serializer.validated_data[self.lookup_field]}, found {len(results)}")
            raise APIException(detail="Failed to query archive database.", code=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return results[0]
 