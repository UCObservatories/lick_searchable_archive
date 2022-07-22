"""
Helper functions for connecting the archive database with SQL Alchemy
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tenacity import retry, stop_after_delay, wait_exponential


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

