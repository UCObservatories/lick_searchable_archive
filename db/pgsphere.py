"""Adds SQL Alchemy support for the pgsphere SPoing custom postgres data type. 
This datatype allows sptial index support for RA/DEC cone searches.
"""
from sqlalchemy import func
from sqlalchemy.types import UserDefinedType

from astropy.coordinates import SkyCoord

class SPoint(UserDefinedType):

    @classmethod
    def convert(cls, ra, dec):
        """Convert ra/dec in decimal degrees to an SPoint."""
        return f'({ra}d, {dec}d)'

    @classmethod
    def convert_hmsdms(cls, ra, dec):
        """Convert ra/dec in hms format to an SPoint"""
        split_ra = ra.split(":")
        if len(split_ra) != 3:
            raise ValueError(f"RA value {ra} is not valid hms format.")

        split_dec = dec.split(":")
        if len(split_dec) != 3:
            raise ValueError(f"DEC value {dec} is not valid dms format.")

        # Use Astropy SkyCoord to deal with weird coordinates, like hms with 60 as the seconds
        coord = SkyCoord(f'{split_ra[0]}h {split_ra[1]}m {split_ra[2]}s', f'{split_dec[0]}d {split_dec[1]}m {split_dec[2]}s')
        (output_ra, output_dec) = coord.to_string(style='decimal').split()

        return cls.convert(output_ra, output_dec)

    def get_col_spec(self):
        return "SPOINT"

    def bind_expression(self, bindvalue):
        return func.spoint(bindvalue)


