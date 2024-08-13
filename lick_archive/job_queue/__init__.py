# Make sure celery app is loaded for shared tasks

from .celery import app as celery_app

__all__ = ('celery_app')