from datetime import date
import os

import logging

logger = logging.getLogger(__name__)


from django.conf import settings

from rest_framework import generics, views, status
from rest_framework.response import Response


from .serializers import IngestNotificationSerializer
from .tasks import ingest_new_files

class IngestNotifications(generics.CreateAPIView):
    serializer_class = IngestNotificationSerializer
    
    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, many=isinstance(request.data, list))
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        logger.info(repr(serializer))
        # Create celery tasks to ingest the metadata
        if isinstance(serializer.validated_data, list):
            ingests = serializer.validated_data
        else:
            ingests = [serializer.validated_data]

        ingest_new_files.s(ingests).apply_async()

        headers = self.get_success_headers(serializer.validated_data)
        return Response(serializer.validated_data, status=status.HTTP_201_CREATED, headers=headers)

        
