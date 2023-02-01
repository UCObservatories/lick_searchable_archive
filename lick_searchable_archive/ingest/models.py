from django.db import models
from django.conf import settings

# Create your models here.
class IngestNotification(models.Model):
    ingest_date = models.DateTimeField(auto_now_add=True)
    filename = models.FilePathField(path=settings.LICK_ARCHIVE_ROOT_DIR, allow_folders=False, allow_files=True, recursive=True)    
    status = models.TextField(default='PENDING',editable=False)

    class Meta:
        ordering = ['-ingest_date']

