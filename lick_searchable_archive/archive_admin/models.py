from django.db import models
from django.conf import settings
from django.contrib.auth.models import User
import datetime

# Create your models here.
class TypeOverride(models.Model):
    night = models.DateField(default=datetime.date.today)
    instrument = models.CharField(max_length=80, choices=[('shane', 'Shane Kast'),('AO', 'ShARCS/AO')])
    pattern = models.CharField(max_length=80)    
    type = models.TextField(max_length=8, choices=[('cal', 'cal'), ('focus', 'focus'),('flat', 'flat'), ('science', 'science'), ('unknown', 'unknown')])

class AccessOverride(models.Model):
    night = models.DateField(default=datetime.date.today)
    instrument = models.CharField(max_length=80, choices=[('shane', 'Shane Kast'),('AO', 'ShARCS/AO')])
    pattern = models.CharField(max_length=80)    
    user = models.ForeignKey(User, on_delete=models.CASCADE, blank=True)
    all_that_night = models.BooleanField(verbose_name="All observers from that night", default=False)
    public = models.BooleanField(verbose_name="Public", default=False)