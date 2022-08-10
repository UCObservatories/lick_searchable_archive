"""
Common utility functions for ingesting metadata from fits files
"""

from pathlib import Path
from pgsphere import SPoint
import logging
from datetime import datetime, timezone

def safe_header(header, key):
    """Read a keyword from a header, returning None if it's not there."""
    if key in header:
        return header[key]
    else:
        return None

def safe_strip(string_or_none):
    """Strip the leading and trailing whitespace from a string, ignoring None values."""
    if string_or_none is not None:
        return string_or_none.strip()

def parse_file_date(filename):
    """
    Parse lick archive filenames to get the date of the file was stored under.
    The format of the filename is expected to be 'YYYY-MM/DD/instrument/file
    """
    day = filename.parent.parent.name
    year_month = filename.parent.parent.parent.name
    return f'{year_month}-{day}'

def get_shane_lamp_status(header):
    """Translate the LAMPSTAX header keywords in shane files to an array
       of booleans."""
    lamp_names = [ '1', '2', '3', '4', '5',
                   'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K']

    try:
        lamp_status = [ (isinstance(header[f'LAMPSTA{name}'], bool) and header[f'LAMPSTA{name}']) or (isinstance(header[f'LAMPSTA{name}'], str) and header[f'LAMPSTA{name}'].lower()=='on') for name in lamp_names]
    except KeyError:
        lamp_status = None
    return lamp_status

def get_ra_dec(header):
    """Read RA and DEC coordinates from a fits header, prioritizing the
       WCS keywords first, and falling back to 'RA' and 'DEC' if those
       are not set.
       
       Returns:
       
       ra (str): The RA in string format
       dec (str): The DEC in string format
       coord (SPoint): The coordinates as a SPoint object suitable
                       for inserting into a pgsphere column
    """
    ra = None
    dec = None
    coord = None

    if ('CRVAL1S' in header and 'CRVAL2S' in header and 
        'CTYPE1S' in header and 'CTYPE2S' in header and 
        'WCSNAMES' in header):
        # Make sure the WCS is really celestial
        # Note the FITS standard says the first four characters
        # are for type and are padded with hyphens
        if (header['WCSNAMES'] == "Celestial coordinates" and
            header['CTYPE1S'].startswith("RA--") and
            header['CTYPE2S'].startswith("DEC-")):

            ra  = header['CRVAL1S']
            dec = header['CRVAL2S']
            coord = SPoint.convert(ra, dec)

    if (ra is None and 
        'CRVAL1' in header and 'CRVAL2' in header and 
        'CTYPE1' in header and 'CTYPE2' in header and 
        'WCSNAME' in header):
        # Make sure the WCS is really celestial
        # Note the FITS standard says the first four characters
        # are for type and are padded with hyphens
        if (header['WCSNAME'] == "Celestial coordinates" and
            header['CTYPE1'].startswith("RA--") and
            header['CTYPE2'].startswith("DEC-")):

            ra  = header['CRVAL1']
            dec = header['CRVAL2']

            coord = SPoint.convert(ra, dec)            

    if ra is None and 'RA' in header and 'DEC' in header:
        ra = header['RA']
        dec = header['DEC']
        if ":" in ra:
            coord = SPoint.convert_hmsdms(ra, dec)
        else:
            coord = SPoint.convert(ra, dec)

    if isinstance(ra, str):
        ra = ra.strip()

    if isinstance(dec, str):
        dec = dec.strip()
    return (ra, dec, coord)

def setup_logging(log_path, log_name, log_level):
    """Setup loggers to send some information to stderr and the configured log level to a file"""

    log_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
    log_file = f"{log_name}_{log_timestamp}.log"
    if log_path is not None:
        log_file = Path(log_path).joinpath(log_file)

    # Configure a file handler to write detailed information to the log file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(fmt="{levelname:8} {asctime} {module}:{funcName}:{lineno} {message}", style='{'))

    # Setup a basic formatter for output to stderr
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter())

    logging.basicConfig(handlers=[stream_handler, file_handler], level=logging.DEBUG)


def get_unique_file(path, prefix, extension=""):
    """
    Return a unique filename.

    Args:
    path (pathlib.Path): Path where the unique file will be located.
    prefix (str):  Prefix name for the file.
    extension (str): File extension for the file. Defaults to empty.

    Returns:
    A filename starting with prefix, ending with extension, that does not currently
    exist.
    """
    if extension != "" and not extension.startswith('.'):
        extension = '.' + extension            

    unique_file = path.joinpath(prefix + extension)
    n = 1
    while unique_file.exists():
        unique_file = path.joinpath(prefix + f".{n}" + extension)
        n+=1
    return unique_file
