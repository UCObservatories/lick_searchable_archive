from datetime import datetime, timezone, timedelta
import copy
# Create your views here.

from .models import Ingest
from .serializers import IngestSerializer
from .tasks import ingest_new_files
import logging

logger = logging.getLogger(__name__)

from rest_framework import generics, views, response, status
from rest_framework.response import Response

class Notifications(generics.ListCreateAPIView):
    serializer_class = IngestSerializer

    def get_queryset(self):
        return Ingest.objects.all().filter(ingest_date__gt=datetime.now(tz=timezone.utc) - timedelta(days=2))
    
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

"""
class GetRecentIngests(generics.ListAPIView):
    serializer_class = IngestSerializer

    def get_queryset(self):
        return Ingest.objects.filter(ingest_date__gt=datetime.now(tz=timezone.utc) - timedelta(days=2))

class NotifyNewIngest(generics.CreateAPIView):
    serializer_class = IngestSerializer

    def get_serializer(self, **kwargs):

        if 'data' in kwargs:
            if isinstance(kwargs['data'], list):
                kwargs['many'] = True

        return self.serializer_class(kwargs)
"""
"""
class NotifyNewIngest(views.APIView):

    def post(self, request, format=None):
        if isinstance(request.data,list):
            many=True
        else:
            many=False

        serializer = IngestSerializer(data=request.data, many=many)
        if serializer.is_valid():
            serializer.save()
            return response.Response(serializer.data, status=status.HTTP_201_CREATED)

        return response.Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
"""