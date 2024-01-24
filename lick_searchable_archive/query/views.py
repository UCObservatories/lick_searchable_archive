"""The views that make up the lick archive query app."""

import logging
logger = logging.getLogger(__name__)

from pathlib import Path

from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.renderers import BaseRenderer
from rest_framework import status


from lick_archive.db.db_utils import create_db_engine
from lick_archive.data_dictionary import api_capabilities
from lick_archive.db.archive_schema import Main
from lick_archive.django_utils import log_request_debug
from lick_archive.archive_config import ArchiveConfigFile

lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

from .query_api import QuerySerializer, QueryAPIView
from .sqlalchemy_django_utils import SQLAlchemyQuerySet, SQLAlchemyORMSerializer

# SQLAlchemy likes its engine to have a global lifetime.
_db_engine = create_db_engine(user=lick_archive_config.database.db_query_user, database=lick_archive_config.database.archive_db)

class QueryView(QueryAPIView):
    """View that integrates the archive Query API with SQL Alchemy"""
    serializer_class = SQLAlchemyORMSerializer
    required_attributes = list(api_capabilities['required']['db_name'])
    allowed_sort_attributes = list(api_capabilities['sort']['db_name'])
    allowed_result_attributes = list(api_capabilities['result']['db_name'])


    def get_queryset(self):
        return SQLAlchemyQuerySet(_db_engine, Main)

class PlainTextRenderer(BaseRenderer):
    """A renderer for rendering FITS headers in plain text."""
    media_type = 'text_plain'
    format='txt'
    charset='ascii'

    def render(self, data, accepted_media_type=None, renderer_context=None):
        return data


class HeaderView(APIView):
    """A view for getting the header for a specific fits file in the archive."""
    renderer_classes = [PlainTextRenderer]
    allowed_result_attributes = ["header"]
    allowed_sort_attributes = ["id"]

    def get(self, request, file):
        log_request_debug(request)

        # Validate request using query serializer
        data = {"filename": file}
        serializer = QuerySerializer(data=data, view=self)
        try:
            serializer.is_valid(raise_exception=True)
        except Exception as e:
            logger.error("Failed to validate filename when requesting header.", exc_info=True)
            raise

        full_path = Path(lick_archive_config.ingest.archive_root_dir) / serializer.validated_data['filename']
        logger.info(f"Getting header info for file: {full_path}")

        try:
            queryset = SQLAlchemyQuerySet(_db_engine,Main)
            queryset = queryset.filter(filename__exact=str(full_path))
            queryset = queryset.values("header")
            results = list(queryset)
            if len(results) !=1:
                return Response(data="File was not found.", status=status.HTTP_404_NOT_FOUND)
            
        except Exception as e:
            logger.error(f"Failed to get header from database for {full_path}: {e}", exc_info=True)
            return Response(data="Failed to query archive database.", status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(data=results[0]["header"], status=status.HTTP_200_OK)


