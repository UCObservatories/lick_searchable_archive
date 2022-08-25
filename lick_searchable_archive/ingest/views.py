from datetime import date
import os

import logging

logger = logging.getLogger(__name__)


from django.conf import settings

from rest_framework import generics, views, response, status
from rest_framework.response import Response

from sqlalchemy import select, func

from lick_archive.db.db_utils import create_db_engine, open_db_session, execute_db_statement
from lick_archive.db.archive_schema import Main

from .serializers import IngestSerializer
from .tasks import ingest_new_files


class IngestNewFiles(generics.ListCreateAPIView):
    serializer_class = IngestSerializer
    
    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, many=isinstance(request.data, list))
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        logger.info(repr(serializer.data))
        # Create celery tasks to ingest the metadata
        if isinstance(serializer.data, list):
            ingests = serializer.data
        else:
            ingests = [serializer.data]

        ingest_new_files.s(ingests).apply_async()

        headers = self.get_success_headers(serializer.data)
        return Response(serializer.data, status=status.HTTP_201_CREATED, headers=headers)

class SyncQuery(views.APIView):
    engine = create_db_engine()
    def get(self, request, format=None):
        input_date = request.query_params.get('date')
        input_ins_dir =  request.query_params.get('ins_dir')

        if input_date is None:
            logging.error("SyncQuery called with no date.")
            return Response("SyncQuery missing date parameter.", status=status.HTTP_400_BAD_REQUEST)
        else:
            try:
                validated_date = date.fromisoformat(input_date)
            except ValueError as e:
                logger.error(f"SyncQuery received invalid date: {e}")
                return Response("SyncQuery received date that was not valid.", status=status.HTTP_400_BAD_REQUEST)

        if input_ins_dir is None:
            logger.error(f"SyncQuery missing instrument directory (ins_dir) parameter")
            return Response("SyncQuery missing ins_dir parameter.", status=status.HTTP_400_BAD_REQUEST)
        else:
            if input_ins_dir not in settings.LICK_ARCHIVE_INSTRUMENT_DIRS:
                logger.error(f"SyncQuery received invalid instrument directory (ins_dir) parameter: {input_ins_dir[0:10]}")
                return Response("SyncQuery given an invalid or unsupported ins_dir parameter.")
            else:
                validated_ins_dir = input_ins_dir

        logging.info(f"SyncQuery called on date {validated_date}, instrument dir {validated_ins_dir}.")

        path_query_value = os.path.join(settings.LICK_ARCHIVE_ROOT_DIR,
                                        f"{validated_date.year}-{validated_date.month:02}",
                                        f"{validated_date.day:02}",
                                        validated_ins_dir + "/%")

        result = 0
        try:
            with open_db_session(self.engine) as session:            
                stmt = select(func.count(Main.id)).where(Main.filename.like(path_query_value))
                result = execute_db_statement(session, stmt).scalar()
                if not isinstance(result, int):
                    logger.error(f"SyncQuery got unexpected result from database: {result}")
                    return Response("SyncQuery could not get count from database.", status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except Exception as e:
            logger.error(f"SyncQuery got exception from database: {e}", exc_info=True)
            return Response("SyncQuery could not get count from database.", status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        logging.info(f"SyncQuery found {result} files for {validated_date}/{validated_ins_dir}.")
        return Response({"count": result}, status=status.HTTP_200_OK)

