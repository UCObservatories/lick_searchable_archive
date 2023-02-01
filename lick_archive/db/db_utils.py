"""
Helper functions for connecting the archive database with SQL Alchemy
"""
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
import psycopg2
from tenacity import retry, stop_after_delay, wait_exponential, retry_if_not_exception_type, after_log

from lick_archive.db.archive_schema import Main

import logging
logger = logging.getLogger(__name__)

@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10), after=after_log(logger, logging.DEBUG))
def create_db_engine(user='archive', database='archive'):
    """Create a database engine object for the Lick archive database. 
    Uses exponential backoff to deal with connection issues.
    """
    logger.debug("Connecting to database")
    engine = create_engine(f'postgresql://{user}@/{database}')
    logger.debug("Connected to database")
    return engine

@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10), after=after_log(logger, logging.DEBUG))
def open_db_session(engine):
    """Open a database session object for the Lick archive database. 
    Uses exponential backoff to deal with connection issues.
    """
    logger.debug("Opening session")
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.debug("Session opened")
    return session

@retry(retry=retry_if_not_exception_type(psycopg2.IntegrityError) & retry_if_not_exception_type(psycopg2.ProgrammingError), reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10), after=after_log(logger, logging.DEBUG))
def insert_one(engine, row):
    """
    Insert one row of metadata using a new database session. This function uses exponential backoff
    retries for deailing with database issues. We do not retry UniqueViolations because such a failure
    will never succeed.
    """
    logger.info(f"Inserting row.")
    session = open_db_session(engine)
    session.add(row)
    session.commit()
    logger.debug("Row inserted")

@retry(retry=retry_if_not_exception_type(psycopg2.IntegrityError) & retry_if_not_exception_type(psycopg2.ProgrammingError), reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10), after=after_log(logger, logging.DEBUG))
def insert_batch(session, batch):
    """Insert a batch of metadata using a database session. This function uses exponential backoff
    retries for deailing with database issues. We do not retry UniqueViolations because such a failure
    will never succeed."""
    logger.info(f"Inserting batch of length {len(batch)}")
    session.bulk_save_objects(batch)
    session.commit()
    logger.debug("Batch inserted.")

@retry(retry=retry_if_not_exception_type(psycopg2.IntegrityError) & retry_if_not_exception_type(psycopg2.ProgrammingError), reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10), after=after_log(logger, logging.DEBUG))
def check_exists(engine, filename, session = None):
    """
    Check if a file has already been inserted. 
    """
    if session is None:
        session = open_db_session(engine)

    # We do a select count()... and see if the result is one. There's a unique constraint
    # on filename so it should always be 1 or 0
    stmt = select(func.count(Main.id)).where(Main.filename == filename)
    
    logger.debug(f"Running Exists SQL: {stmt.compile()}")
    result = session.execute(stmt).scalar() == 1
    logger.debug(f"Exists SQL complete. Result {result}")
    return result

@retry(retry=retry_if_not_exception_type(psycopg2.IntegrityError) & retry_if_not_exception_type(psycopg2.ProgrammingError), reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10), after=after_log(logger, logging.DEBUG))
def execute_db_statement(session, stmt):    

    logger.debug(f"Running SQL: {stmt.compile()}")
    
    in_outside_transaction = session.in_transaction()

    if in_outside_transaction:
        logger.debug("Running in outside transaction")

    try:
        result = session.execute(stmt)
    except Exception as e:
        # Python DB stuff always starts a transaction. But if something fails it can't be retried 
        # without rolling back the entire transaction.
        # If we're in an outside transaction, this must be done by the caller, as they know what
        # other statements need to be retried. Otherwise we can do the rollback here to allow the
        # tenacity retries to work (or at least consistently give the same failure).
        if not in_outside_transaction:
            session.rollback()
        raise

    if not in_outside_transaction:
        # Commit the automatically started transaction. The session is left in the same state it 
        # was as when this function was called
        session.commit()

    logger.debug(f"SQL complete.")
    return result

