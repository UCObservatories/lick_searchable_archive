from celery import shared_task
from celery.utils.log import get_task_logger

from .models import IngestNotification
from django.conf import settings

logger=get_task_logger(__name__)

from lick_archive.db.db_utils import create_db_engine, open_db_session, insert_batch, check_exists
from lick_archive.metadata.reader import read_row

_db_engine = create_db_engine(user=settings.LICK_ARCHIVE_INGEST_USER, database=settings.LICK_ARCHIVE_DB)


@shared_task
def ingest_new_files(new_ingests):

    logger.info(f"Starting ingest of {len(new_ingests)} files.")
    session = open_db_session(_db_engine)
    rows_to_add = []
    good_files = []
    failed_files = []
    logger.info(repr(new_ingests))
    for ingest in new_ingests:
        try:
            if not check_exists(_db_engine, ingest['filename'], session=session):
                logger.info(f"Reading metadata for {ingest['filename']}.")
                row = read_row(ingest['filename'])      
                rows_to_add.append(row)
            else:
                logger.info(f"{ingest['filename']} is already in the archive database, skipping.")
                good_files.append(ingest['filename'])      
        except Exception as e:
            logger.error(f"Failed ingesting file {ingest['filename']}.", exc_info=True)
            failed_files.append(ingest['filename'])


    try:
        if len(rows_to_add) > 0:
            logger.info(f"Inserting {len(rows_to_add)} files into the database")
            insert_batch(session, rows_to_add)
            good_files += [row.filename for row in rows_to_add]
        else:
            logger.info("No files to insert.")

    except Exception as e:
        logger.error("Failed inserting metadata into archive datbase.", exc_info=True)
        failed_files = [row.filename for row in rows_to_add]

    logger.info(f"Updating status on {len(good_files)} successful ingests and {len(failed_files)} failed ingests.")
    if len(good_files) > 0:
        results = IngestNotification.objects.filter(filename__in=good_files).update(status='COMPLETE')
        logger.info(f"Update found {results} rows")

    if len(failed_files) > 0:
        results = IngestNotification.objects.filter(filename__in=failed_files).update(status='FAILED')
        logger.info(f"Update found {results} rows")
