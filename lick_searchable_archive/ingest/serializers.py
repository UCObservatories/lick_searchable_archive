from rest_framework.serializers import ModelSerializer
from .models import Ingest
class IngestSerializer(ModelSerializer):
    class Meta:
        model = Ingest
        fields = ['ingest_date', 'filename', 'status']
