# This boiler plate code is taken from the celery docs at:
# https://docs.celeryq.dev/en/stable/django/first-steps-with-django.html#using-celery-with-django
import os

from celery import Celery

app = Celery('lick_searchable_archive')

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'lick_searchable_archive.settings')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.

app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto load tasks from all registered Django apps
app.autodiscover_tasks()

