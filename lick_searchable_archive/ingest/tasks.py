from functools import partial

from celery import shared_task
from celery.utils.log import get_task_logger

from .models import IngestNotification

logger=get_task_logger(__name__)

from lick_archive.db.db_utils import create_db_engine, open_db_session, check_exists, insert_file_metadata, BatchedDBOperation
from lick_archive.db.archive_schema import FileMetadata
from lick_archive.metadata.reader import read_file
from lick_archive.authorization.override_access import OverrideAccessFile
from archive_auth.models import save_oaf_to_db
from lick_archive.archive_config import ArchiveConfigFile

lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config


_db_engine = create_db_engine(user=lick_archive_config.database.db_ingest_user, database=lick_archive_config.database.archive_db)


@shared_task
def ingest_new_files(new_ingests):

    logger.info(f"Starting ingest of {len(new_ingests)} files.")
    session = open_db_session(_db_engine)
    added_files = []
    logger.info(repr(new_ingests))

    # Process any override access files first
    remaining_files, good_files, failed_files = process_oafs(new_ingests)

    with BatchedDBOperation(_db_engine,lick_archive_config.ingest.insert_batch_size) as insert_batch:
        for file in remaining_files:
            try:               
                if not check_exists(_db_engine, FileMetadata.id, FileMetadata.filename == file, session=session):
                    logger.info(f"Reading metadata for {file}.")
                    file_metadata = read_file(file)      
                    insert_batch.insert(file_metadata)
                    added_files.append(file_metadata)
                else:
                    logger.info(f"{file} is already in the archive database, skipping.")
                    good_files.append(file)      
            except Exception as e:
                logger.error(f"Failed ingesting file {file}.", exc_info=True)
                failed_files.append(file)


    if len(added_files) > 0:
        logger.info(f"Addded {len(added_files)} to archive database.")
    else:
        logger.info("No files to insert.")

    # Gather failures
    failed_files += [failure[0] for failure in insert_batch.failures]
    for file in added_files:
        if file not in failed_files:                    
            good_files.append(file)

    logger.info(f"Updating status on {len(good_files)} successful ingests and {len(failed_files)} failed ingests.")
    if len(good_files) > 0:
        results = IngestNotification.objects.filter(filename__in=good_files).update(status='COMPLETE')
        logger.info(f"Update found {results} rows")

    if len(failed_files) > 0:
        results = IngestNotification.objects.filter(filename__in=failed_files).update(status='FAILED')
        logger.info(f"Update found {results} rows")

def process_oafs(new_ingests):

    remaining_files = []
    good_files = []
    failed_files = []

    for ingest in new_ingests:
        if OverrideAccessFile.check_filename(ingest['filename']):
            try:
                oaf = OverrideAccessFile.from_file(ingest['filename'])
            except Exception as e:
                logger.error(f"Failed to read override access file {ingest['filename']}: {e}", exc_info=True)
                failed_files.append(ingest['filename'])
                continue
            try:
                save_oaf_to_db(oaf)
                good_files.append(ingest['filename'])
                logger.info(f"Successfully ingested override access file {ingest['filename']}")
            except Exception as e:
                logger.error(f"Failed to save override access file {ingest['filename']} to db: {e}", exc_info=True)
                failed_files.append(ingest['filename'])
                continue
        else:
            remaining_files.append(ingest['filename'])

    return remaining_files, good_files, failed_files