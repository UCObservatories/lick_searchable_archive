#!/usr/bin/env python
"""
Retry failures from bulk_ingest_metadata.
"""

import argparse
import sys
import logging
from pathlib import Path
from contextlib import closing

# Setup django before importing any django classes
from lick_archive.utils.django_utils import setup_django, setup_django_logging
setup_django()


from lick_archive.utils.script_utils import setup_logging, get_unique_file
from lick_archive.db.db_utils import create_db_engine, open_db_session, insert_file_metadata, update_file_metadata, find_file_metadata
from lick_archive.utils.resync_utils import ErrorList, SyncType
from lick_archive.db.archive_schema import FileMetadata
from lick_archive.metadata.reader import read_file

logger = logging.getLogger(__name__)

def retry_one_file(error_list, op_type, failed_file):

    try:
        logger.info(f"Reading metadata from {failed_file}.")
        row = read_file(failed_file)
        logger.info(f"Finished reading metadata from {failed_file}.")
    except Exception as e:
        error_list.add_file(failed_file, op_type, str(e))
        return

    try_to_insert_update = False
    try:
        db_engine = create_db_engine()
        if op_type in [SyncType.HEADER_UPDATE, SyncType.UPDATE]:
            logger.info(f"Updating data for {failed_file}")
            # Open a distinct DB session to find the existing internal id to avoid a deadlock with ourselves
            with closing(open_db_session(db_engine)) as session:
                existing_metadata = find_file_metadata(session, FileMetadata.filename==str(failed_file))
                file_id = None if existing_metadata is None else existing_metadata.id

            # Now do the update
            if file_id is None:
                # Try an insert
                logger.error(f"Could not find file {failed_file} for update, will try an insert.")
                try_to_insert_update = True
                op_type = SyncType.INSERT
            else:                   
                with closing(open_db_session(db_engine)) as session:
                    update_file_metadata(session, file_id, row, row.user_access)
                    session.commit()
                    logger.info(f"Finished updating data for {failed_file}.")

        if op_type == SyncType.INSERT or try_to_insert_update:
            logger.info(f"Inserting data for {failed_file}.")

            with closing(open_db_session(db_engine)) as session:
                insert_file_metadata(session, row)
                session.commit()
                logger.info(f"Finished inserting data for {failed_file}.")


    except Exception as e:
        msg = str(e)
        if try_to_insert_update:
            op_type = SyncType.UPDATE
            msg = "Could not find file for update, tried an insert instead. " + msg
        
        error_list.add_file(failed_file, op_type, {e})
        logger.error(f"Failed to retry {failed_file}.", exc_info = True)
    

def main():
    parser = argparse.ArgumentParser(description='Retry failed files from an "ingest_failures" file created by bulk_metadata_ingest.\n'
                                                 'A log file of the ingest is created in bulk_ingest_<timestamp>.log.\n'
                                                 'A new ingest_failures.n.txt will be created for any files that still fail.')
    parser.add_argument("ingest_failures", type=str, help = 'An ingest failures file from bulk metadata retry. Usually named "ingest_failures.n.txt".')
    parser.add_argument("-d", "--dbname", type=str, default='archive', help='Name of the database to connect to. Defaults to "archive".')
    parser.add_argument("-U", "--username", type=str, default='archive', help='Name of the database user to connect with. Defaults ot "archive".')
    parser.add_argument("--log_path", "-l", type=str, help="Directory to write log file to." )
    parser.add_argument("--log_level", "-L", type=str, choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], default="DEBUG", help="Logging level to use.")

    args = parser.parse_args()

    setup_logging(args.log_path, "retry_failures", args.log_level)

    error_file = get_unique_file(Path('.'),"retry_failures", 'txt')
    error_list = ErrorList(error_file)
    logger.info(f"Reading {args.ingest_failures}...")
    try:
        failures = ErrorList.read_failures(args.ingest_failures)
        for failed_file, op_type in failures:   
            retry_one_file(error_list, op_type, failed_file)

    except Exception as e:
        with open(error_file, "a") as f:
            print(f"Failed to read {args.ingest_failures}: {e}", file=f)
        logger.error(f"Failed to read {args.ingest_failures}.", exc_info = True)



if __name__ == '__main__':
    sys.exit(main())
