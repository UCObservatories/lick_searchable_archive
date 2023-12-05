"""
Common utility functions for reading metadata from fits files
"""

import logging
logger = logging.getLogger(__name__)
import calendar
from datetime import date, timedelta, datetime

from lick_archive.db.pgsphere import SPoint
from astropy.io import fits



def calculate_public_date(file_date : date|datetime, proprietary_period : str) -> date:
    """Calculate the date a file's proprietary period 
    expires given it's date and the proprietary period.
    
    This function attempts to be friendly in terms of how the propreitary
    period is specified. It attempts to do intuitive arithmetic 
    by years or months, and accepts units as singular or plural strings.

    For example::

        2023-01-01 + "1 month"  = 2023-02-01
        2024-01-31 + "1 month"  = 2024-03-01  # Skip to next month because Feb 2024 doesn't have 31 days
        2024-01-31 + "2 months" = 2024-03-31
        2024-02-29 + "1 years"  = 2025-03-01  # Feb 2025 is not a leap year, so skip to the next month
        2024-02-29 + "4 years"  = 2028-02-29  

    Args:
        file_date:  The observation date of the file.
        period:     The proprietary period as a string. The format of this string is "<n> <units>" where
                    n is an integer, and <units> is one of "years", "year", "month", "months", "days", "day".
                    Units is case insensitive.

    Return:
        The date the files becomes public.
    """

    if isinstance(file_date, datetime):
        file_date = file_date.date()

    # Parse out the period and units
    period_list = proprietary_period.split()
    if len(period_list) !=2:
        raise ValueError(f"Invalid proprietary period {proprietary_period}")
    
    try:
        period = int(period_list[0])
    except ValueError:
        raise ValueError(f"Proprietary period does not contain a valid positive integer {proprietary_period}")

    if period < 1:
        raise ValueError(f"Proprietary period must be a positive integer {proprietary_period}")            

    units=period_list[1]

    # If the units are in days, just use the built intimedelta
    if units.lower() in ['days', 'day']:
        return file_date + timedelta(days=period)
    else:
        # Figure out the total # of years and months in the period
        if units.lower() in ['years', 'year']:
            period_years = period
            period_months = 0
        elif units.lower() in ['months', 'month']:
            period_years = int(period/12)
            period_months = period%12
        else:
            raise ValueError(f"Incorrect proprietary period units given: {proprietary_period}")

        public_year = file_date.year + period_years
        public_month = file_date.month + period_months

        # Wrap months around to the next year
        if public_month > 12:
            public_year += 1
            public_month -= 12

        # Make sure the date isn't beyond the end of the month
        # I wasn't sure how to handle this, so I decided to make it
        # the first of the next month, so that Feb 29
        # is handled as March 1st on subsequent non-leap years
        weekday_of_first, days_in_month = calendar.monthrange(public_year,public_month)
        if file_date.day > days_in_month:
            public_day = 1
            public_month += 1
            if public_month==13:
                public_year += 1
                public_month = 1
        else:
            public_day = file_date.day

        return date(year=public_year, month=public_month, day=public_day)


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

    if ra is None and 'RA' in header and 'DEC' in header:
        ra = header['RA']
        dec = header['DEC']

    if isinstance(ra, str):
        ra = ra.strip()

    if isinstance(dec, str):
        dec = dec.strip()

    if ra is not None and dec is not None:
        try:
            coord = SPoint(ra, dec)
            if coord.ra is None or coord.dec is None:
                coord = None
        except Exception as e:
            logger.info("Failed to create SPoint from ra/dec: {e}",exc_info=True)
            coord = None
    return (ra, dec, coord)

class _MockHDU:
    """Mock HDU object for unit testing. 
    If any of our code starts touching data, a real HDU object may be needed
    """
    def __init__(self, header):
        self.header = header

def get_hdul_from_text(text_files):
    """
    Build a simulated HDU list from headers written to text files
    """

    hdul = [] 
    for file in text_files:
        hdul.append(_MockHDU(fits.Header.fromfile(file, sep='\n', endcard=False, padding=False)))

    return hdul

def get_hdul_from_string(string_list):
    """
    Build a simulated HDU list from headers written to text files
    """

    hdul = [] 
    for header_string in string_list:
        hdul.append(_MockHDU(fits.Header.fromstring(header_string, sep='\n')))

    return hdul
