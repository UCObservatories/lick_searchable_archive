"""Utilities for accessing the Lick Observatory schedule database."""


import logging
logger = logging.getLogger(__name__)

from sqlalchemy.schema import MetaData,Table
from sqlalchemy import select

from lick_archive.db import db_utils
from lick_archive.archive_config import ArchiveConfigFile

class ScheduleDB:
      
    def __init__(self, lick_archive_config : ArchiveConfigFile):
        """Initializes a connection to the schedule database and reads the definition
        for the needed tables.
        
        Args:
            lick_archive_config: The archive's configuration information, needed to find
                                 the connection information to the schedule database.
        """
        user_information = self._read_user_information(lick_archive_config.ingest.sched_db_user_info)
        self._sched_db_engine = db_utils.create_db_engine(url=f"postgresql://{user_information}@{lick_archive_config.ingest.sched_db_host}/{lick_archive_config.ingest.sched_db_name}")

        # Use SQLAlchemy's reflection to get the database tables without having to 
        # declare every column/type here
        self._sched_db_metadata = MetaData()
        self._observers_table = Table("observers", self._sched_db_metadata, autoload_with=self._sched_db_engine)

    def _read_user_information(self, filename:str) -> str:
        """
        Read the username/password for the schedule database from the given text file.
        The information should be formatted: "username:password".
        """
        try:
            with open(filename, "r") as user_info:
                for line in user_info:
                    if ":" in line.strip():
                        user_info=line.strip()
                        return user_info
        except Exception as e:
            logger.error(f"Failed to read user schedule db user information from {filename}.",exc_info=True)
        
        raise RuntimeError(f"Could not read schedule db user information. Make sure {filename} exists, is readable, and contains '<username>:<password>'")

    def get_observers(self) -> list[dict]:
        """Return all of the oberservers in the schedule database.
        
        Return:
            A list of dictionary objects for each observer, with each column being a key/value pair.
        """
           
        session = db_utils.open_db_session(self._sched_db_engine)
        return db_utils.execute_db_statement(session, select(self._observers_table)).mappings().all()