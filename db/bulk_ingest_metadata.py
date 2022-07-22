"""
Ingest metadata from existing data in the Lick archive in bulk.
"""

import argparse
import sys
from pathlib import Path
from datetime import date, datetime, timezone
import re
import logging

from tenacity import retry, stop_after_delay, wait_exponential

from ingest.reader import read_row
from db_utils import create_db_engine, open_db_session

logger = logging.getLogger(__name__)

def parse_date_range(date_range):
    """
    Parse a date range from the command line. The date range's format is "YYYY-MM-DD" indicating a single day or 
    "YYYY-MM-DD:YYYY-MM-DD" indicating a range.

    Returns:
    start_date: A datetime.date of the start of the date range.
    end_date: A datetime.date of the end of the date range.
    """
    if date_range is not None:
        if ":" in date_range:
            (start_str, end_str) = date_range.split(":")
        else:
            start_str = date_range
            end_str = None

        date_list = start_str.split('-')
        if len(date_list) < 3:
            raise ValueError(f"'{start_str}' is an invalid start date. It should be YYYY-MM-DD.")

        try:
            start_date = date(int(date_list[0]), int(date_list[1]), int(date_list[2]))
        except:
            raise ValueError(f"'{start_str}' is an invalid start date. It should be YYYY-MM-DD.")

        if end_str is None:
            end_date = start_date
        else:
            date_list = end_str.split('-')
            if len(date_list) < 3:
                raise ValueError(f"'{end_str}' is an invalid end date. It should be YYYY-MM-DD.")

            try:
                end_date = date(int(date_list[0]), int(date_list[1]), int(date_list[2]))
            except:
                raise ValueError(f"'{end_str}' is an invalid start date. It should be YYYY-MM-DD.")

        return (start_date, end_date)
    return (None, None)


def get_files_for_ingest(root_dir, start_date, end_date, instruments):
    """
    Scan the lick archive root dir for files that match command line parameters.

    The direcotires in the archive are expected to follow the 'YYYY-MM/DD/instrument/' convention.

    Args:

    root_dir:    (str) The root directory of the archive
    start_date:  (datetime.date) The starting date of the date range returned files should lie within.
                                 None for no date range. 
    end_date:    (datetime.date) The ending date of the date range returned files should lie within
                                 None for no date range.
    instruments: (list of str) A list of instruments to find files for.

    Returns: A generator for the list of matching pathlib.Path objects.
    """
    root_path = Path(root_dir)

    # Go through the month directories
    for month_dir in root_path.iterdir():
        if month_dir.is_dir():
            # This should be a directory of the format YYYY-MM
            match = re.match('^(\d\d\d\d)-(\d\d)$', month_dir.name)
            if match is not None:
                year = int(match.group(1))
                month = int(match.group(2))
                # Go through the day directories
                for day_dir in month_dir.iterdir():
                    if day_dir.is_dir() and re.match('^\d\d$',day_dir.name) is not None:
                        day = int(day_dir.name)
                        # Build a date object and if it's between the requested date range (inclusive), keep searching this
                        # directory
                        current_date = date(year, month, day)
                        if start_date is None or (current_date >= start_date and current_date <= end_date):
                            # Go through instrument directories
                            for instrument_dir in day_dir.iterdir():                                
                                # Return any files found for the requested instruments
                                if instrument_dir.is_dir() and instrument_dir.name in instruments:
                                    for file in instrument_dir.iterdir():
                                        if not file.is_file():
                                            print(f"Unexpected directory or special file: {file}")
                                            continue
                                        yield file
                                

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


def insert_batch(session, batch):
    """Insert a batch of metadata using a database session"""
    print("Inserting batch")
    session.bulk_save_objects(batch)
    session.commit()

@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10))
def insert_one(engine, row):
    """
    Insert one row of metadata using a new database session. This function uses exponential backoff
    retries for deailing with database issues.
    """
    session = open_db_session(engine)
    session.add(row)
    session.commit()

def retry_one_by_one(error_file, engine, batch):
    """
    Retry inserting a batch of metadata one row at a time, in case
    one of the rows failed due to a schema issue rather than an intermittent db issue.
    """
    for row in batch:
        try:
            insert_one(engine, row)
        except Exception as e:
            with open(error_file, "a") as f:
                print(f"Failed to retry {row.filename}: {e}", file=f)
            logger.error(f"Failed to retry {row.filename}: {e}")

def get_unique_file(path, prefix, extension):
    """
    Return a unique filename.

    Args:
    path (pathlib.Path): Path where the unique file will be located.
    prefix (str):  Prefix name for the file.
    extension (str): File extension for the file.

    Returns:
    A filename starting with prefix, ending with extension, that does not currently
    exist.
    """
    unique_file = path.joinpath(prefix + extension)
    n = 1
    while unique_file.exists():
        unique_file = path.joinpath(prefix + f".{n}." + extension)
        n+=1
    return unique_file
        
def setupLogging(log_path, log_level):
    """Setup loggers to send some information to stderr and the configured log level to a file"""

    log_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
    log_file = f"bulk_ingest_{log_timestamp}.log"
    if log_path is not None:
        log_file = Path(log_path).joinpath(log_file)

    # Configure a file handler to write detailed information to the log file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(fmt="{levelname:8} {asctime} {module}:{funcName}:{lineno} {message}", style='{'))

    # Setup a basic formatter for output to stderr
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter())

    logging.basicConfig(handlers=[stream_handler, file_handler], level=logging.DEBUG)


def main(args):

    start_time = datetime.now(timezone.utc)

    setupLogging(args.log_path, args.log_level)
    logger.info(f"Bulk Started Ingest on {args.archive_root}")

    try:
        # Setup database, logging, and an ingest_failures file.
        engine = create_db_engine()
        error_file = get_unique_file (Path("."), "ingest_failures", "txt")
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
        files = get_files_for_ingest(args.archive_root, start_date, end_date, args.instruments)

        # Insert metadata for the specified files in batches
        batch = []
        for file in files:
            try:
                logger.debug(f"Reading metadata from {file}.")
                next_row = read_row(file)
            except Exception as e:
                with open(error_file, "a") as f:
                    print(f"Failed to read {file}: {e}", file=f)
                logger.error(f"Failed to read {file}.", exc_info = True)
                continue

            logger.info(f"Finished reading metadata from {file}")
            batch.append(next_row)

            # Insert the batch once it's full
            if len(batch) >= args.batch_size:
                try:
                    session=open_db_session(engine)
                    insert_batch(session, batch)
                except Exception as e:
                    retry_one_by_one(error_file, engine, batch)
                batch = []
        # Insert any left over data that did not fill an entire batch
        if len(batch) > 0:
            try:
                session=open_db_session(engine)
                insert_batch(session, batch)
            except Exception as e:
                retry_one_by_one(error_file, engine, batch)
    except Exception as e:
        logging.error("Caught exception at end of main.", exc_info = True)
        return 1
    logger.info(f"Bulk Ingest Finished, total time {datetime.now(timezone.utc) - start_time}.")


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    sys.exit(main(args))
