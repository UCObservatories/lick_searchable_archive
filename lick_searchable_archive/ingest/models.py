from django.db import models
from django.conf import settings

# Create your models here.
class IngestNotification(models.Model):
    ingest_date = models.DateTimeField(auto_now_add=True)
    filename = models.CharField(max_length=1024)    
    status = models.TextField(default='PENDING',editable=False)

    class Meta:
        ordering = ['-ingest_date']

