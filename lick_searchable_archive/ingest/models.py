from django.db import models

# Create your models here.
class Ingest(models.Model):
    ingest_date = models.DateTimeField(auto_now_add=True)
    filename = models.TextField()
    status = models.TextField(default='PENDING',editable=False)

    class Meta:
        ordering = ['-ingest_date']

