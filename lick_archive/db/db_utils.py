"""
Helper functions for connecting the archive database with SQL Alchemy
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tenacity import retry, stop_after_delay, wait_exponential

from lick_archive.db.archive_schema import Main

@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10))
def create_db_engine():
    """Create a database engine object for the Lick archive database. 
    Uses exponential backoff to deal with connection issues.
    """
    print("Connecting to database")
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
    session = open_db_session(engine)
    session.add(row)
    session.commit()

@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10))
def insert_batch(session, batch):
    """Insert a batch of metadata using a database session"""
    print("Inserting batch")
    session.bulk_save_objects(batch)
    session.commit()


@retry(reraise=True, stop=stop_after_delay(60), wait=wait_exponential(multiplier=1, min=4, max=10))
def check_exists(engine, filename, session = None):
    """
    Check if a file has already been inserted. 
    """
    if session is None:
        session = open_db_session(engine)

    q = session.query(Main.id).filter(Main.filename == filename)
    return session.query(q.exists()).scalar()

