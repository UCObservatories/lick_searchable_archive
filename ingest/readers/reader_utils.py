from pathlib import Path
from pgsphere import SPoint

def safe_header(header, key):
    if key in header:
        return header[key]
    else:
        return None

def parse_file_date(filename):
    file_path = Path(filename)
    day = filename.parent.parent.name
    year_month = filename.parent.parent.parent.name
    return f'{year_month}-{day}'

def get_shane_lamp_status(header):
    lamp_names = [ '1', '2', '3', '4', '5',
                   'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K']

    try:
        lamp_status = [ (isinstance(header[f'LAMPSTA{name}'], bool) and header[f'LAMPSTA{name}']) or (header[f'LAMPSTA{name}'].lower()=='on') for name in lamp_names]
    except KeyError:
        lamp_status = None
    return lamp_status

def get_ra_dec(header):

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

    return (ra, dec, coord)
    

