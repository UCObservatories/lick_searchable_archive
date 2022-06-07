from astropy.io import fits
from pathlib import Path


class MetadataReader:

    @classmethod
    def can_read(cls, file_path, hdul):
        return False
    
    def read_row(self, file_path, hdul):
        return None
    
