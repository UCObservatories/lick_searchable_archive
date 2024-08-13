#!/usr/bin/env python
""" Reingest metadata for archive files using their header data. """
import argparse
import sys
from pathlib import Path
from datetime import datetime, timezone

import logging
logger = logging.getLogger(__name__)


from lick_archive.db.db_utils import create_db_engine, BatchedDBOperation
from lick_archive.utils.script_utils import get_log_path
from lick_archive.db.archive_schema import UserDataAccess, FileMetadata
from lick_archive.authorization.date_utils import get_observing_night
from lick_archive.authorization.user_access import get_public_date
from lick_archive.external import ScheduleDB
# Setup django before importing any django classes
from lick_archive.utils.django_utils import setup_django, setup_django_logging
setup_django()


from lick_archive.utils.resync_utils import SyncType, get_dirs_for_daterange, ErrorList, get_metadata_from_command_line
from lick_archive.authorization.override_access import OverrideAccessFile
from archive_auth.models import ArchiveUser
from django.core.exceptions import ObjectDoesNotExist

def get_parser():
    """
    Parse command line arguments with argparse.
    """
    parser = argparse.ArgumentParser(description='Fix files marked as owned by an unknown user.')

    parser.add_argument("user", type=str, help='The user to assign the file to. This can be the string "public" to make the file public, or an integer observer id, an email address, or full name (firstname.lastname).'  )
    parser.add_argument("--id_file", type=Path, help="A file containing database ids separated by whitespace. Any of these files assigned to an unknown user are updated.")
    parser.add_argument("--ids", type=str, help="A list of database ids. Any of these files assigned to an unknown user are updated.")
    parser.add_argument("--files", type=str, help="A list of filenames. Any of these files assigned to an unknown user are updated.")
    parser.add_argument("--date_range", type=str, help='Date range of files to ingest. Examples: "2010-01-04", "2010-01-01:2011-12-31". Defaults to all. Any files within this date range that are assigned to an unknown user are updated.')
    parser.add_argument("--instruments", type=str, default='all', nargs="*", help='Which instrument subdirectories to get metadata from. Defaults to all.')
    
    parser.add_argument("--db_name", default="archive", type=str, help = 'Name of the database to update. Defaults to "archive"')
    parser.add_argument("--db_user", default="archive", type=str, help = 'Name of the database user. Defaults to "archive"')
    parser.add_argument("--batch_size", type=int, default=10000, help='Number of rows to update in the database at once, defaults to 10,000')
    parser.add_argument("--log_path", "-l", type=str, help="Directory to write log file to." )
    parser.add_argument("--log_level", "-L", type=str, choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], default="DEBUG", help="Logging level to use.")
    return parser
    

def main(args):

    try:
        # Setup logging and an ingest_failures file.
        start_time = datetime.now(timezone.utc)
        log_path = get_log_path("fix_unknown")
        setup_django_logging(log_path, args.log_level,stdout_level="INFO")

        obid = parse_and_validate_obid(args)
        if obid is None:
            return 2

        # Setup the database connection    
        db_engine = create_db_engine(args.db_user, args.db_name)

        # Get the metadata specified on command line
        metadata = get_metadata_from_command_line(db_engine, args)

        if metadata is None:
            return 1

        # Update the files information in batches
        with BatchedDBOperation(db_engine, args.batch_size) as batch:
            for file_metadata in metadata:
                new_user_access = []
                update_needed = False
                for user_access_info in file_metadata.user_access:
                    if user_access_info.obid == ScheduleDB.UNKNOWN_USER:

                        if obid == ScheduleDB.PUBLIC_USER:
                            # Force public date to the current date
                            public_date = get_observing_night(datetime.now(tz=timezone.utc))
                            new_reason = user_access_info.reason + f"\nUnknown user set to public by fix_unknown.py"
                        else:
                            # Get the new public_date based on the user
                            public_date, reason, public = get_public_date(file_metadata, get_observing_night(file_metadata.obs_date), [obid])
                            new_reason = user_access_info.reason + f"\nUnknown user set to {obid} by fix_unknown.py\n" + reason

                        file_metadata.public_date = public_date                       
                        new_user_access_info = UserDataAccess(file_id = file_metadata.id, obid=obid, reason=new_reason)
                        new_user_access.append(new_user_access_info)
                        update_needed = True
                    else:
                        new_user_access.append(user_access_info)
            
                if update_needed:
                    logger.info(f"Updating {file_metadata.filename} / {file_metadata.id}")                    
                    for ua in new_user_access:
                        logger.info(f"obid: {ua.obid} reason:\n{ua.reason}")

                    batch.update(file_metadata.id, file_metadata, new_user_access)

        logger.info(f"Updated {batch.success} of {batch.total} files with {batch.total - batch.success} failures and {batch.success_retries} successful retries.")
        logger.info(f"Duration: {datetime.now(timezone.utc) - start_time}")

    except Exception as e:
        logging.error("Caught exception at end of main.", exc_info = True)
        return 1

    return 0

def parse_and_validate_obid(args :argparse.Namespace):
    """Get the observer id and public date to use for unknown files given our command line arguments."""

    try:
        if args.user is not None and len(args.user) > 0:
            if args.user=="public":
                logger.info("Setting to public user")
                return ScheduleDB.PUBLIC_USER

            try:
                obid = int(args.user)
                is_int = True
            except ValueError:
                is_int = False

            if is_int:
                # It converted to an integer, is it valid, and does it exist in the DB
                if obid >= 0:
                    try:
                        user = ArchiveUser.objects.get(obid=obid)
                        logger.info(f"Setting to obid {obid}")
                        return obid
                    except ObjectDoesNotExist as e:
                        # We'll tr the string as a username/email before giving upo
                        pass

            else:
                # Maybe it's an e-mail address?
                users = list(ArchiveUser.objects.filter(email=args.user))
                if len(users)==1:
                    # Found one user, return it's id
                    obid = users[0].obid
                    logger.info(f"Setting to obid {obid} based on email: {args.user}")
                    return obid

                # Maybe it's a first.lastname
                if "." in args.user:
                    split_name = args.user.lower().rsplit(".",maxsplit=1)
                    if len(split_name) == 2:
                        first_name, last_name = split_name
                        users = list(ArchiveUser.objects.filter(first_name__iexact=first_name, last_name__iexact=last_name))
                        if len(users)==1:
                            obid = users[0].obid
                            logger.info(f"Setting to obid {obid} based on first name {first_name} last name: {last_name}")
                            return obid
    except Exception as e:
        logger.error("Caught exception parsing user argument.", exc_info=True)

    logger.error(f"Failed to resolve user argument '{args.user}' into an observer id.")
    return None

                

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    sys.exit(main(args))
