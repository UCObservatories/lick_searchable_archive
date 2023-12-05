"""
Retry failures from bulk_ingest_metadata.
"""

import argparse
import sys
import logging
import re
from pathlib import Path
from datetime import date, datetime, timezone

from lick_archive.script_utils import setup_logging, parse_date_range, get_files_for_daterange
from lick_archive.db.db_utils import create_db_engine, insert_one, check_exists, insert_batch, open_db_session
from lick_archive.metadata.reader import read_row

logger = logging.getLogger(__name__)

def retry_one_file(error_file, failed_file):

    try:
        logger.info(f"Reading metadata from {failed_file}.")
        row = read_row(failed_file)
        logger.info(f"Finished reading metadata from {failed_file}.")
    except Exception as e:
        with open(error_file, "a") as f:
            print(f"Failed to retry {failed_file}: {e}", file=f)
        logger.error(f"Failed to retry {failed_file}.", exc_info = True)
        return

    try:
        logger.info(f"Inserting data for {failed_file}.")
        engine = create_db_engine()

        if not check_exists(engine, row.filename):
            insert_one(engine, row)
            logger.info(f"Finished inserting data for {failed_file}.")
        else:
            logger.info(f"{failed_file} already exists.")
    except Exception as e:
        with open(error_file, "a") as f:
            print(f"Failed to retry {failed_file}: {e}", file=f)
        logger.error(f"Failed to retry {failed_file}.", exc_info = True)
    
def get_parser():
    """
    Parse bulk_ingest_metadata command line arguments with argparse.
    """
    parser = argparse.ArgumentParser(description='Ingest metadata for Lick data into the archive database.\n'
                                                 'A log file of the ingest is created in bulk_ingest_<timestamp>.log.\n'
                                                 'A separate ingest_failures.n.txt is also created listing files that failed ingesting.')
    parser.add_argument("archive_root", type=str, help = 'Top level directory of the archived Lick data.')
    parser.add_argument("--date_range", type=str, help='Date range of files to ingest. Examples: "2010-01-04", "2010-01-01:2011-12-31". Defaults to all.')
    parser.add_argument("--batch_size", type=int, default=10000, help='Number of rows to insert into the database at once, defaults to 10,000')
    parser.add_argument("--instruments", type=str, nargs="+", help='Which instruments to get metadata from. Defaults to all.')
    parser.add_argument("-d", "--dbname", type=str, default='archive', help='Name of the database to connect to. Defaults to "archive".')
    parser.add_argument("-U", "--username", type=str, default='archive', help='Name of the database user to connect with. Defaults ot "archive".')
    parser.add_argument("--log_path", "-l", type=str, help="Directory to write log file to." )
    parser.add_argument("--log_level", "-L", type=str, choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], default="DEBUG", help="Logging level to use.")
    return parser


def insert_batch_with_retry(engine, session, batch):
    """
    Insert a batch of metadata. Retry the batch one row at a time
    in case of a failure.

    Args:

        engine (sqlalchemy.engine.Engine): The database engine to add the batch to.
        batch (list of lick_archive.db.archive_schema.Main): The metadata objects to add.

    Return (int): The number of successfully inserted rows.
    """
    try:
        insert_batch(session, batch)
        return len(batch)
    except Exception as e:
        try:
            session.rollback()
        except Exception as e:
            logger.error(f"Failed to rollback session.", exc_info=True)

        success_count = 0
        for row in batch:
            try:
                insert_one(engine, row)
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to retry {row.filename}: {e}")        
        return success_count

def main():
    parser = get_parser()
    args = parser.parse_args()

    setup_logging(args.log_path, "resync_date_range", args.log_level)

    try:
        # Setup database, logging, and an ingest_failures file.
        engine = create_db_engine()
        session=open_db_session(engine)
        supported_instruments = ['shane', 'AO']
        (start_date, end_date) = parse_date_range(args.date_range)
        if args.instruments is not None:
            for instrument in args.instruments:
                if instrument not in supported_instruments:
                    logger.error(f"{instrument} is not a supported instrument. It should be one of: {','.join(supported_instruments)}." )
                    return 1
        else:
            args.instruments = supported_instruments

        # Get the files to read metadata from
        files = get_files_for_daterange(args.archive_root, start_date, end_date, args.instruments)

        resync_count = 0
        success_count = 0
        batch = []
        for file in files:
            if check_exists(engine, file, session=session):
                logger.debug(f"Skipping {file} because it already exists.")
            elif file.name.startswith("override") and file.name.endswith("access"):
                logger.debug(f"Skipping override*access file.")
            else:
                resync_count += 1
                try:
                    logger.debug(f"Reading metadata from {file}.")
                    next_row = read_row(file)
                except Exception as e:
                    logger.error(f"Failed to read {file}.", exc_info = True)
                    continue

                logger.info(f"Finished reading metadata from {file}")
                batch.append(next_row)

                # Insert the batch once it's full
                if len(batch) >= args.batch_size:
                    success_count+=insert_batch_with_retry(engine,session,batch)
                    batch = []
        # Insert any left over data that did not fill an entire batch
        if len(batch) > 0:
            success_count+=insert_batch_with_retry(engine,session,batch)
    except Exception as e:
        logging.error("Caught exception at end of main.", exc_info = True)
        return 1
    logger.info(f"Resynced {success_count} of {resync_count} files, {resync_count-success_count} failures.")

                    




if __name__ == '__main__':
    sys.exit(main())
