import argparse
from pathlib import Path
import sys
import os
from datetime import date, timedelta
from contextlib import closing

from ext_test_common import TEST_USER, TEST_INSTR, PUBLIC_FILE, PRIVATE_FILE, disable_user, remove_override_access

from lick_archive.config.archive_config import ArchiveConfigFile
lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

# Setup django before importing any django classes
from lick_archive.utils.django_utils import setup_django, setup_django_logging
setup_django()

from lick_archive.db import db_utils
from lick_archive.db.archive_schema import FileMetadata
from sqlalchemy import select

def main():
    parser = argparse.ArgumentParser(description='Cleanup data left over from external system tests.')
    args = parser.parse_args()

    setup_django_logging(Path.cwd() / "server_init.log", "INFO")

    # Step 1: Enable the test_user account
    print("Disabling test_user")
    disable_user(TEST_USER)

    # Step 2: Remove the propreitary test file
    # In case of date roll-over, we look at the previous day and current day for the file
    print("Removing proprietary test files")
    ingest_dates = [date.today() - timedelta(days=1), date.today()]
    for private_ingest_date in ingest_dates:
        private_test_file = lick_archive_config.ingest.archive_root_dir / private_ingest_date.strftime(f"%Y-%m/%d/") / TEST_INSTR / PRIVATE_FILE
        if private_test_file.exists():
            print(f"Removing {private_test_file}")
            os.remove(private_test_file)

    # Step 3: Remove propreitary test file from the database
    print("Remove metadata entries for proprietary test file")
    for private_ingest_date in ingest_dates:
        private_test_file = lick_archive_config.ingest.archive_root_dir / private_ingest_date.strftime(f"%Y-%m/%d/") / TEST_INSTR / PRIVATE_FILE
        remove_metadata(private_test_file)

    # Step 4: Remove the override access file that gave the test_user account access to the proprietary test file
    print("Removing override access for proprietary test file")
    for private_ingest_date in ingest_dates:
        remove_override_access(private_ingest_date, TEST_INSTR, TEST_USER, PRIVATE_FILE)


def remove_metadata(filename : Path|str):
    query = select(FileMetadata).where(FileMetadata.filename == str(filename))
    db_engine = db_utils.create_db_engine(database = lick_archive_config.database.archive_db)
    with closing(db_utils.open_db_session(db_engine)) as session:
        file_metadata = db_utils.find_file_metadata(session,query)
        if file_metadata is not None:
            print(f"Removing file metadata for {file_metadata.filename}")
            session.delete(file_metadata)
            session.commit()
    


if __name__ == '__main__':
    sys.exit(main())
