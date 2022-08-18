# This boiler plate code is taken from the celery docs at:
# https://docs.celeryq.dev/en/stable/django/first-steps-with-django.html#using-celery-with-django
import os

from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'lick_searchable_archive.settings')

app = Celery('lick_searchable_archive')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.

app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto load tasks from all registered Django apps
app.autodiscover_tasks()

