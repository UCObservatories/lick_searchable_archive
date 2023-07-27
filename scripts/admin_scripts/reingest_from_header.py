#!/usr/bin/env python
""" Reingest metadata for archive files using their header data. """
import argparse
import sys
from pathlib import Path
from datetime import date, datetime, timezone
import re
import logging

from sqlalchemy import select

from lick_archive.db.archive_schema import Main, IngestFlags, all_attributes

from lick_archive.metadata.reader import read_hdul
from lick_archive.db.db_utils import create_db_engine, open_db_session, execute_db_statement, update_batch
from lick_archive.script_utils import setup_logging
from lick_archive.metadata.metadata_utils import get_hdul_from_string

logger = logging.getLogger(__name__)

def get_parser():
    """
    Parse command line arguments with argparse.
    """
    parser = argparse.ArgumentParser(description='Re-ingest metadata for files using the header data already in the database.',
                                     epilog="Example:\n    $ echo \"select id from main where filename like '/data/data/2019-05/14%';\" | psql -U -f - > id_file.txt\n    $ reingest_from_header.py id_file.txt\n", 
                                     formatter_class=argparse.RawDescriptionHelpFormatter, exit_on_error=True)
    parser.add_argument("id_file", type=str, help="File name of a file with the database ids to update.")
    parser.add_argument("--db_name", default="archive", type=str, help = 'Name of the database to update. Defaults to "archive"')
    parser.add_argument("--db_user", default="archive", type=str, help = 'Name of the database user. Defaults to "archive"')
    parser.add_argument("--batch_size", type=int, default=10000, help='Number of rows to update in the database at once, defaults to 10,000')
    parser.add_argument("--log_path", "-l", type=str, help="Directory to write log file to." )
    parser.add_argument("--log_level", "-L", type=str, choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], default="DEBUG", help="Logging level to use.")
    return parser

def rebuild_metadata_batch(db_engine, batch):
    """
    Rebuild the SQLAlchemy database object for each file in a batch.

    Args:
        db_engine (:obj:`sqlalchemy.engine.Engine`): The database engine to query the header information from.
        batch (list of int): The database ids of the files to rebuild.

    Return:
        list of dict: The rebuilt metadata for each file in batch. If there are any errors,
                      this list might be smaller than the original batch. 
    """
    metadata_batch = []

    session=open_db_session(db_engine)
    query = select(Main.id, Main.filename, Main.ingest_flags, Main.header).where(Main.id.in_(batch))
    results = execute_db_statement(session, query).all()
    logger.info(f"Regenerating metadata for {len(results)}")
    for row in results:
        try:
            hdul = get_hdul_from_string([row.header])
        except Exception as e:
            logger.error(f"Failed to get hdul from header string for {row.id} / {row.filename}", exc_info=True)
            continue

        ingest_flags = IngestFlags(int(row.ingest_flags,2))
        # Turn off flags not related to opening the fits file, so they can be reset by the re-reading of the header
        ingest_flags &= (IngestFlags.NO_FITS_END_CARD | IngestFlags.NO_FITS_SIMPLE_CARD | IngestFlags.FITS_VERIFY_ERROR | IngestFlags.INVALID_CHAR)
        try:
            metadata = read_hdul(row.filename, hdul, ingest_flags)

            # Make sure the new metadata object knows it's id
            metadata.id = row.id
            metadata_batch.append(metadata)

        except Exception as e:
            logger.error(f"Failed to rebuild metadata for {row.id} / {row.filename}", exc_info=True)
            continue
        logger.debug(f"Successfully read metadata for {row.id} / {row.filename}")
    return metadata_batch

def update_metadata_batch(db_engine, metadata_batch):
    """
    Update the archive database with a batch of metadata.

    Args:
        db_engine (:obj:`sqlalchemy.engine.Engine`): The database engine for the archive database.
        metadata_batch (list of dict): The rebuilt metadata to update with.
    """

    # We don't need to include the header in the update
    attributes = [name for name in all_attributes if name not in ['header']]

    try:
        session = open_db_session(db_engine)
        logger.info("Updating batch of metadata.")
        update_batch(session, metadata_batch, attributes)
    except Exception as e:
        logger.error("Failed to update batch, retyring row by row")
        for metadata in metadata_batch:
            try:
                session = open_db_session(db_engine)
                logger.info(f"Retrying {metadata.id} / {metadata.filename}")
                update_batch(session, [metadata], attributes)
            except Exception as e:
                logger.error(f"Failed to update {metadata.id} / {metadata.filename}", exc_info=True)



    

def main(args):

    setup_logging(args.log_path, "reingest_from_headers", args.log_level)
        
    # Read the id list
    id_list = []
    line = 1
    with open(args.id_file, "r") as f:
        for line in f:
            for id in line.strip().split():
                try:
                    id_list.append(int(id))
                except ValueError as e:
                    print(f"Failed to convert id '{id}' to integer: {e}", file=sys.stderr)
                    return 1
    id_list = list(sorted(set(id_list)))
    if len(id_list) == 0:
        print("No ids to update.", file=sys.stderr)
        return 2

    logger.info(f"Updating {len(id_list)} unique ids.")

    try:
        db_engine = create_db_engine(args.db_user, args.db_name)
    except Exception:
        logger.error(f"Failed to connect to database '{args.db_name}' with user '{args.db_user}'", exc_info=True)
        return 3

    batch = []
    for i in range(len(id_list)):
        batch.append(id_list[i])
        if len(batch) == args.batch_size or i == len(id_list)-1:
            metadata_batch = rebuild_metadata_batch(db_engine, batch)
            update_metadata_batch(db_engine, metadata_batch)            
            batch = []
    
    logger.info(f"Done.")
    return 0


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    sys.exit(main(args))
