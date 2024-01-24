#!/usr/bin/env python
"""
Resync file sizes in the metadata database.
"""

import argparse
import sys
import logging

from sqlalchemy import select

from lick_archive.script_utils import setup_logging, parse_date_range, get_files_for_daterange
from lick_archive.db.db_utils import create_db_engine, update_batch, update_one, execute_db_statement, open_db_session
from lick_archive.db.archive_schema import Main
from lick_archive.archive_config import ArchiveConfigFile

logger = logging.getLogger(__name__)

lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

    
def get_parser():
    """
    Parse bulk_ingest_metadata command line arguments with argparse.
    """
    parser = argparse.ArgumentParser(description='Resync file size metadata for Lick data in the archive database.')
                                                 
    parser.add_argument("archive_root", type=str, help = 'Top level directory of the archived Lick data.')
    parser.add_argument("--date_range", type=str, help='Date range of files to ingest. Examples: "2010-01-04", "2010-01-01:2011-12-31". Defaults to all.')
    parser.add_argument("--batch_size", type=int, default=10000, help='Number of rows to update in the database at once, defaults to 10,000')
    parser.add_argument("--instruments", type=str, nargs="+", help='Which instruments to resync from. Defaults to all.')
    parser.add_argument("-d", "--dbname", type=str, default='archive', help='Name of the database to connect to. Defaults to "archive".')
    parser.add_argument("-U", "--username", type=str, default='archive', help='Name of the database user to connect with. Defaults ot "archive".')
    parser.add_argument("--log_path", "-l", type=str, help="Directory to write log file to." )
    parser.add_argument("--log_level", "-L", type=str, choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], default="DEBUG", help="Logging level to use.")
    return parser


def update_batch_with_retry(engine, session, batch):
    """
    Update the file size for a batch of metadata. Retry the batch one row at a time
    in case of a failure.

    Args:

        engine (sqlalchemy.engine.Engine): The database engine to add the batch to.
        session (:obj:`sql.alchemy.orm.session.Session`): The database session to use.
        batch (list of lick_archive.db.archive_schema.Main): The metadata objects to add.

    Return (int): The number of successfully updated rows.
    """
    try:
        update_batch(session, batch, ['id', 'file_size'])
        return len(batch)
    except Exception as e:
        try:
            session.rollback()
        except Exception as e:
            logger.error(f"Failed to rollback session.", exc_info=True)

        success_count = 0
        for row in batch:
            try:
                update_one(engine, row, ['file_size'])
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to retry {row.filename}: {e}")        
        return success_count

def get_file_metadata(session, file):
    stmt = select(Main).where(Main.filename == str(file))

    results = list(execute_db_statement(session, stmt).scalars())
    if len(results) > 1:
        raise ValueError(f"File {file} was not unique in the database!")
    elif len(results) == 0:
        return None
    else:
        return results[0]


def main():
    parser = get_parser()
    args = parser.parse_args()

    setup_logging(args.log_path, "resync_date_range", args.log_level)

    try:
        # Setup database, logging, and an ingest_failures file.
        engine = create_db_engine()
        session=open_db_session(engine)
        (start_date, end_date) = parse_date_range(args.date_range)
        if args.instruments is not None:
            for instrument in args.instruments:
                if instrument not in lick_archive_config.ingest.supported_instruments:
                    logger.error(f"{instrument} is not a supported instrument. It should be one of: {','.join(lick_archive_config.ingest.supported_instruments)}." )
                    return 1
        else:
            args.instruments = lick_archive_config.ingest.supported_instruments

        # Get the files to read metadata from
        files = get_files_for_daterange(args.archive_root, start_date, end_date, args.instruments)

        resync_count = 0
        match_count = 0
        missing_count = 0
        error_count = 0
        batch = []                

        for file in files:
            if file.name.startswith("override") and file.name.endswith("access"):
                logger.debug(f"Skipping override*access file.")
                continue

            try:
                result = get_file_metadata(session, file)
            except Exception as e:
                logger.error(f"Failed to query for {file}.", exc_info = True)
                error_count+=1
                continue

            if result is None:
                logger.info(f"Missing file {file} in database.")
                missing_count +=1
                continue
            try:
                stat_info = file.lstat() if file.is_symlink() else file.stat()
                filesize = stat_info.st_size
            except Exception as e:
                logger.error(f"Failed to stat {file}.", exc_info = True)
                error_count+=1
                continue

            if filesize == result.file_size:
                logger.info(f"Matching file sizes for {file}")
                match_count += 1
            else:
                result.file_size = filesize
                batch.append(result)
                # Update the batch once it's full
                if len(batch) >= args.batch_size:
                    success_count = update_batch_with_retry(engine,session,batch)
                    resync_count += success_count
                    error_count += len(batch) - success_count
                    batch = []

        # Insert any left over data that did not fill an entire batch
        if len(batch) > 0:
            success_count = update_batch_with_retry(engine,session,batch)
            resync_count += success_count
            error_count += len(batch) - success_count
            batch = []
    except Exception as e:
        logging.error("Caught exception at end of main.", exc_info = True)
        return 1
    total = resync_count + match_count + missing_count + error_count

    logger.info(f"For {total} files, {match_count} did not need updating, {resync_count} were updated, {missing_count} were not in the db, and {error_count} failed to resync due to an error.")

                    




if __name__ == '__main__':
    sys.exit(main())
