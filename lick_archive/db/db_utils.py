"""
Helper functions for connecting the archive database with SQL Alchemy
"""
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

from tenacity import retry, stop_after_delay, wait_exponential

from lick_archive.db.archive_schema import Main

import logging
logger = logging.getLogger(__name__)

@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10))
def create_db_engine():
    """Create a database engine object for the Lick archive database. 
    Uses exponential backoff to deal with connection issues.
    """
    logger.debug("Connecting to database")
    engine = create_engine('postgresql://archive@/archive')
    return engine

@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10))
def open_db_session(engine):
    """Open a database session object for the Lick archive database. 
    Uses exponential backoff to deal with connection issues.
    """
 
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10))
def insert_one(engine, row):
    """
    Insert one row of metadata using a new database session. This function uses exponential backoff
    retries for deailing with database issues.
    """
    logger.info(f"Inserting row.")
    session = open_db_session(engine)
    session.add(row)
    session.commit()

@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10))
def insert_batch(session, batch):
    """Insert a batch of metadata using a database session"""
    logger.info(f"Inserting batch of length {len(batch)}")
    session.bulk_save_objects(batch)
    session.commit()


@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10))
def check_exists(engine, filename, session = None):
    """
    Check if a file has already been inserted. 
    """
    if session is None:
        session = open_db_session(engine)

    # We do a select count()... and see if the result is one. There's a unique constraint
    # on filename so it should always be 1 or 0
    stmt = select(func.count(Main.id)).where(Main.filename == filename)
    logger.debug(f"Running SQL: {stmt.compile()}")
    return session.execute(stmt).scalar() == 1

@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10))
def execute_db_statement(session, stmt):    
    logger.debug(f"Running SQL: {stmt.compile()}")
    return session.execute(stmt)