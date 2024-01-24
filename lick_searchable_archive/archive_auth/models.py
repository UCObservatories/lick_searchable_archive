from django.db import models
from django.conf import settings
from django.contrib.auth.models import AbstractUser
import django.utils
import datetime

class ArchiveUser(AbstractUser):
    obid = models.IntegerField(blank=True,null=True,unique=True)
    stamp = models.DateTimeField(default=django.utils.timezone.now)

class TypeOverride(models.Model):
    night = models.DateField(default=datetime.date.today)
    instrument = models.CharField(max_length=80, choices=[('shane', 'Shane Kast'),('AO', 'ShARCS/AO')])
    pattern = models.CharField(max_length=80)    
    type = models.TextField(max_length=8, choices=[('cal', 'cal'), ('focus', 'focus'),('flat', 'flat'), ('science', 'science'), ('unknown', 'unknown')])

class AccessOverride(models.Model):
    night = models.DateField(default=datetime.date.today)
    instrument = models.CharField(max_length=80, choices=[('shane', 'Shane Kast'),('AO', 'ShARCS/AO')])
    pattern = models.CharField(max_length=80)    
    all_obs_that_night = models.BooleanField(verbose_name="All observers from that night", default=False)
    public = models.BooleanField(verbose_name="Public", default=False)

class Ownerhint(models.Model):
    ownerhint = models.CharField(max_length=150,null=False, blank=False,unique=True)
    access_override = models.ForeignKey(AccessOverride, on_delete=models.CASCADE, blank=True)

