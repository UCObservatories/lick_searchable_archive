import argparse
from pathlib import Path
import sys
import shutil
from datetime import date
from time import sleep

from ext_test_common import TEST_USER, TEST_INSTR, PUBLIC_FILE, PRIVATE_FILE, TEST_USER_OWNERHINT, enable_user, add_override_access

from lick_archive.config.archive_config import ArchiveConfigFile
lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

# Setup django before importing any django classes
from lick_archive.utils.django_utils import setup_django, setup_django_logging
setup_django()

from lick_archive.db import db_utils
from lick_archive.db.archive_schema import FileMetadata

def main():
    parser = argparse.ArgumentParser(description='Setup external system tests by creating users, ingesting new metadata etc.')
    args = parser.parse_args()

    setup_django_logging(Path.cwd() / "server_init.log", "INFO")

    # Step 1: Enable the test_user account
    print("Enabling test_user")
    enable_user(TEST_USER)

    # Step 2: Create/ingest an override access file that gives the test_user account access to a known public test file.
    print("Adding override access for proprietary test file")
    private_ingest_date = date.today()
    add_override_access(private_ingest_date, TEST_INSTR, TEST_USER_OWNERHINT, PRIVATE_FILE)

    # Step 3: Re-ingest the known public test file with the new name, such that it should only be accessible by the test_user        
    private_test_file = Path(private_ingest_date.strftime(f"%Y-%m/%d/")) / TEST_INSTR / PRIVATE_FILE
    test_file_full_path =  lick_archive_config.ingest.archive_root_dir / private_test_file
    if not private_test_file.exists():
        print(f"Ingesting private test file {private_test_file}")
        shutil.copy2(lick_archive_config.ingest.archive_root_dir / PUBLIC_FILE,
                     test_file_full_path)
    else:
        print(f"Private test file '{private_test_file}' already exists.")

    print(f"Waiting up to 5 minutes for private test file to ingest")
    db_engine = db_utils.create_db_engine(database=lick_archive_config.database.archive_db)
    minutes = 0
    while minutes < 5:
        if db_utils.check_exists(db_engine, FileMetadata.filename, FileMetadata.filename == str(test_file_full_path)):
            print(f"Private test file {private_test_file} successfully ingested.")
            return 0
        sleep(60)
        minutes+=1
        print(f"Still waiting for private test file to ingest, {5-minutes} minutes left")
    print(f"Gave up waiting for {private_test_file} to ingest, something may be broken...")
if __name__ == '__main__':
    sys.exit(main())
