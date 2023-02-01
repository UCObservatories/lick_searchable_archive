from astropy.io import fits
import contextlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class MockHDU:
    """Mock HDU object for unit testing. 
       If any of our code starts touching data, a real HDU object may be needed
    """
    def __init__(self, header):
        self.header = header

def get_hdul_from_text(text_files):
    """
    Build a mock HDU list from headers written to text files
    """
    hdul = [] 
    for file in text_files:
        hdul.append(MockHDU(fits.Header.fromfile(file, sep='\n', endcard=False, padding=False)))

    return hdul


class MockDatabase(contextlib.AbstractContextManager):

    def __init__(self, base_class, rows=None):

        self.base_class = base_class

        # Create an in memory engine
        self.engine = create_engine('sqlite://')

        # Create the schema
        self.base_class.metadata.create_all(self.engine)

        if rows is not None:
            # Session for inserting rows
            self.Session = sessionmaker(bind=self.engine)
            session = self.Session()

            session.bulk_save_objects(rows)
            session.commit()
            session.close()


    def __exit__(self, exc_type, exc_value, traceback):
        self.base_class.metadata.drop_all(self.engine)
        return False
