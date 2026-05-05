#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import sys
from pathlib import Path
import logging
import os

#To customize logging for lick archive
from lick_archive.utils import django_utils


def main():
    """Run administrative tasks."""
    # Setup django so the core management code doesn't do it for us
    django_utils.setup_django()

    # Setup logging. We set the umask so that users in the correct group
    # can run manage and write to the log
    os.umask(0o0002)

    from django.conf import settings
    logfile = Path(settings.ARCHIVE_LOG_DIR, "manage.log")
    django_utils.setup_django_logging(logfile, logging.DEBUG, logging.INFO)            

    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
