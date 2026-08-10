#!/usr/bin/env python
""" Reingest metadata for archive files using their header data. """
import argparse
import sys
from pathlib import Path
from datetime import datetime, timezone

import logging
logger = logging.getLogger(__name__)


from lick_archive.db.db_utils import convert_object_to_python, create_db_engine, open_db_session, BatchedDBOperation
from lick_archive.utils.script_utils import get_log_path, get_unique_file
# Setup django before importing any django classes
from lick_archive.utils.django_utils import setup_django, setup_django_logging
setup_django()

from contextlib import closing

from lick_archive.utils.resync_utils import SyncType, get_dirs_for_daterange, ErrorList, get_metadata_from_command_line
from lick_archive.authorization import user_access
from lick_archive.authorization.override_access import OverrideAccessFile
from lick_archive.apps.archive_auth.api import save_oaf_to_db, get_override_file

from lick_archive.config.archive_config import ArchiveConfigFile
lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config


def get_parser():
    """
    Parse command line arguments with argparse.
    """
    parser = argparse.ArgumentParser(description='Re-evaluate the authoriztion of files and re-ingest any override access files.')

    parser.add_argument("--id_file", type=Path, help="A file containing database ids separated by whitespace.")
    parser.add_argument("--ids", nargs="+", type=int, help="A list of database ids.")
    parser.add_argument("--files", type=str, nargs="+", help="A list of filenames.")
    parser.add_argument("--date_range", type=str, help='Date range of files to ingest. Examples: "2010-01-04", "2010-01-01:2011-12-31". Defaults to all.')
    parser.add_argument("--instruments", type=str, default='all', nargs="*", help='Which instruments to get metadata from. Defaults to all.')
    
    parser.add_argument("--no_override", default=False, action="store_true", help="Do not sync override.access files.")
    parser.add_argument("--only_override", default=False, action="store_true", help="Only sync override.access files.")
    parser.add_argument("--dry_run", default=False, action="store_true", help="Do a dry run in which changes are not actually made, only logged.")
    
    parser.add_argument("--db_name", default="archive", type=str, help = 'Name of the database to update. Defaults to "archive"')
    parser.add_argument("--db_user", default="archive", type=str, help = 'Name of the database user. Defaults to "archive"')
    parser.add_argument("--batch_size", type=int, default=10000, help='Number of rows to update in the database at once, defaults to 10,000')
    parser.add_argument("--log_path", "-l", type=str, help="Directory to write log file to." )
    parser.add_argument("--log_level", "-L", type=str, choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], default="DEBUG", help="Logging level to use.")
    return parser
    

def main(args):

    # Setup logging and an ingest_failures file.
    try:
        start_time = datetime.now(timezone.utc)
        log_path = get_log_path("resync_auth")
        setup_django_logging(log_path, args.log_level,stdout_level="INFO")
        error_file = get_unique_file (Path("."), "resync_failures", "txt")
        error_list=ErrorList(error_file)

        if args.no_override and args.only_override:
            logger.error("--no_override and --only_override are mutually exclusive.")
            return 1

        # Resync any override access files
        total_oaf = 0
        successful_oaf = 0
        synced_oaf_paths = set()

        # Setup the database connection    
        db_engine = create_db_engine(args.db_user, args.db_name)

        # Get the metadata specified on command line
        metadata = get_metadata_from_command_line(db_engine, args)

        if metadata is None:
            return 1

        # Update the auth information in batches
        with BatchedDBOperation(db_engine, args.batch_size) as batch:
            for file_metadata in metadata:
                if file_metadata is None:
                    # One of the datasets could not be found
                    continue
                else:
                    logger.info(f"Processing metadata {file_metadata.id}")
                # Make sure all override access files in a path have been synced before 
                # re-running the authorization code on any files in it
                if not args.no_override:
                    file_path = Path(file_metadata.filename).parent
                    if file_path not in synced_oaf_paths:
                        total, success = resync_override_access_files(args, file_path, error_list)
                        total_oaf += total
                        successful_oaf += success
                        synced_oaf_paths.add(file_path)
                if args.only_override:
                    continue
                # Re-generate auth metadata
                if args.dry_run:
                    # Save off the original metadata as a dict
                    orig_metadata = convert_object_to_python(file_metadata)

                try:
                    new_metadata = user_access.set_auth_metadata(file_metadata)
                except Exception as e:
                    msg = f"Failed to regenerate auth metadata for file {file_metadata.filename}"
                    logger.error(msg, exc_info=True)
                    error_list.add_file(file_metadata.filename, SyncType.UPDATE,str(e))
                    continue

                if args.dry_run:
                    compare_file_metadata(orig_metadata, convert_object_to_python(new_metadata))
                else:
                    batch.update(file_metadata.id, new_metadata, new_metadata.user_access)

        logger.info(f"Updated {batch.success} of {batch.total} files with {batch.total - batch.success} failures and {batch.success_retries} successful retries.")
        logger.info(f"Updated {successful_oaf} of {total_oaf} override access files with {total_oaf - successful_oaf} failures.")
        logger.info(f"Duration: {datetime.now(timezone.utc) - start_time}")

    except Exception as e:
        logging.error("Caught exception at end of main.", exc_info = True)
        return 1

    return 0

def resync_override_access_files(args :argparse.Namespace, file_path: Path, error_list : ErrorList):
    logger.info(f"Resyncing Override Access Files: {args.date_range} : {args.instruments}")
    total = 0
    successful = 0
    for file in file_path.iterdir():
        if OverrideAccessFile.check_filename(file):
            total+=1
            try:            
                access_file = OverrideAccessFile.from_file(file)
            except ValueError as e:
                logger.error(f"Invalid override access file {file} in dir {dir}: {e}")
                error_list.add_file(file,SyncType.OVERRIDE_FILE,str(e))
                continue

            if args.dry_run:
                try:
                    orig_access_file = get_override_file(filepath=file)
                except:
                    logger.info(f"{file} could not be found in database.")
                else:
                    compare_oaf(file, orig_access_file, access_file)
            else:
                try:
                    save_oaf_to_db(access_file)
                    successful+=1
                except Exception as e:
                    msg = f"Failed to save {access_file} to db: {e.__class__.__name__}: {e}"
                    error_list.add_file(file,SyncType.OVERRIDE_FILE,msg)
                    logging.error(msg,exc_info=True)
    return total, successful

def compare_oaf(file, db_oaf, file_oaf):
    if db_oaf.observing_night != file_oaf.observing_night:
        logger.info(f"{file} has different observing night: db night: {db_oaf.observing_night} file night: {file_oaf.observing_night}")
        return
    if db_oaf.instrument_dir != file_oaf.instrument_dir:
        logger.info(f"{file} has different instrument dir: db dir: {db_oaf.instrument_dir} file dir: {file_oaf.instrument_dir}")
        return
    if db_oaf.sequence_id != file_oaf.sequence_id:
        logger.info(f"{file} has different seq id: db seq id: {db_oaf.sequence_id} file seq id: {file_oaf.sequence_id}")
        return

    db_rule_len=  len(db_oaf.override_rules)
    file_rule_len = len(file_oaf.override_rules)
    equal_len = db_rule_len == file_rule_len
    if db_rule_len > file_rule_len:
        smaller_name = "file"
        smaller_len = file_rule_len
        smaller_rules = file_oaf.override_rules
        bigger_rules = db_oaf.override_rules
        bigger_len = db_rule_len
        bigger_name = "db"
    elif db_rule_len <= file_rule_len:
        smaller_name = "db"
        smaller_len = db_rule_len
        smaller_rules = db_oaf.override_rules
        bigger_rules = file_oaf.override_rules
        bigger_name = "file"
        bigger_len = file_rule_len

    if not equal_len:
        logger.info(f"{file} has more rules in the {bigger_name}({bigger_len}) than the {smaller_name}({smaller_len})")

    # Compare the rules in common to both the bigger and smaller rule lists
    for i, small_rule in enumerate(smaller_rules):
        big_rule = bigger_rules[i]
        if small_rule.pattern != big_rule.pattern:
            logger.info(f"{file} Rule {i} patterns differ. {smaller_name}: '{small_rule.pattern}' {bigger_name}: '{big_rule.pattern}'")

        if small_rule.obstype != big_rule.obstype:
            logger.info(f"{file} Rule {i} have different obstypes: {smaller_name}: {small_rule.obstype} {bigger_name}: {big_rule.obstype}")

        if small_rule.ownerhints != big_rule.ownerhints:
            logger.info(f"{file} Rule {i} have different ownerhints:")
            logger.info(f"{smaller_name} ownerhints: {','.join(small_rule.ownerhints)}")
            logger.info(f"{bigger_name} ownerhints: {','.join(big_rule.ownerhints)}")

    if not equal_len:
        for i in range(smaller_len,bigger_len):
            rule = bigger_rules[i]
            logger.info(f"{file} {bigger_name} has rule {i} {rule.pattern} {rule.obstype} {','.join(rule.ownerhints)}")
    
def compare_file_metadata(db_metadata, new_metadata):
    relative_filename = Path(db_metadata['filename']).relative_to(lick_archive_config.ingest.archive_root_dir)
    new_rel_filename = Path(new_metadata['filename']).relative_to(lick_archive_config.ingest.archive_root_dir)
    if relative_filename != new_rel_filename:
        logger.info(f"These are different files: {relative_filename} and {new_rel_filename}")
        return

    for key in db_metadata.keys():

        db_value = db_metadata.get(key,None)
        new_value = new_metadata.get(key,None)
        if key == 'user_access':
            if db_value is None and new_value is not None:
                new_users = [str(ua['obid']) for ua in new_metadata['user_access']].sort()
                logger.info(f"{relative_filename} db has no uda records, new file has {len(new_users)}: {','.join(new_users)}")
            elif db_value is not None and new_value is None:
                db_users = [str(ua['obid']) for ua in db_metadata['user_access']].sort()
                logger.info(f"{relative_filename} new file has no uda records, db has {len(db_users)}: {','.join(db_users)}")
            else:
                # Compare what users can access each
                db_users = [str(ua['obid']) for ua in db_metadata['user_access']].sort()
                new_users = [str(ua['obid']) for ua in new_metadata['user_access']].sort()
                if db_users != new_users:
                    logger.info(f"{relative_filename} has different assigned users. Orig {len(db_users)} users: {','.join(db_users)}, new {len(new_users)} users: {','.join(new_users)}")
        else:
            # SPoints don't compare well, so convert to strings first
            if key == 'coord':
                db_value = str(db_value)
                new_value = str(new_value)
            elif key == 'coversheet':
                # This can be a semicolon separated list
                db_value = None if db_value is None else db_value.split(';').sort()
                new_value = None if new_value is None else new_value.split(';').sort()

            if db_value != new_value:
                if key == 'filename':
                    # We'll won't consider different root file systems if the actual files are the same
                    continue
                elif key == "header":
                    # Don't print out the headers, they're huge
                    logger.info(f"{relative_filename} has different headers.")
                else:
                    logger.info(f"{relative_filename} has different {key} values: orig value: '{db_value}' new value: '{new_value}'")


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    sys.exit(main(args))
