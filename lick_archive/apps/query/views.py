"""The views that make up the lick archive query app."""

import logging
logger = logging.getLogger(__name__)

from rest_framework.generics import ListAPIView, RetrieveAPIView
from rest_framework.renderers import BaseRenderer
from rest_framework import status
from rest_framework.serializers import BaseSerializer
from rest_framework.exceptions import APIException

from lick_archive.db.db_utils import create_db_engine
from lick_archive.metadata.data_dictionary import api_capabilities
from lick_archive.db.archive_schema import FileMetadata
from lick_archive.config.archive_config import ArchiveConfigFile

lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

from .query_api import QueryAPIMixin, QueryAPIPagination, QueryAPIFilterBackend
from .sqlalchemy_django_utils import SQLAlchemyQuerySet, SQLAlchemyORMSerializer

# SQLAlchemy likes its engine to have a global lifetime.
_db_engine = create_db_engine(user=lick_archive_config.database.db_query_user, database=lick_archive_config.database.archive_db)

class QueryView(QueryAPIMixin, ListAPIView):
    """View that integrates the archive Query API with SQL Alchemy"""
    pagination_class = QueryAPIPagination
    filter_backends = [QueryAPIFilterBackend]
    serializer_class = SQLAlchemyORMSerializer
    required_attributes = list(api_capabilities['required']['db_name'])
    allowed_sort_attributes = list(api_capabilities['sort']['db_name'])
    allowed_result_attributes = list(api_capabilities['result']['db_name'])


    def get_queryset(self):
        return SQLAlchemyQuerySet(_db_engine, FileMetadata)

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

class HeaderSerializer(BaseSerializer):
    """Serialize a metadata row by returning its header"""

    def to_representation(self, instance):
        return instance.header


class HeaderView(QueryAPIMixin, RetrieveAPIView):
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



