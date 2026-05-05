"""
WSGI config for lick_searchable_archive project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.1/howto/deployment/wsgi/
"""

import os
from pathlib import Path

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'lick_archive.lick_archive_site.settings')

application = get_wsgi_application()

#Customize logging for lick archive apps
from lick_archive.utils import django_utils
from django.conf import settings
logfile = Path(settings.ARCHIVE_LOG_DIR, settings.ARCHIVE_APP_LOG)
django_utils.setup_django_logging(logfile, settings.ARCHIVE_LOG_LEVEL)            


