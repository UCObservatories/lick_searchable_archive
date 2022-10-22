"""Adds SQL Alchemy support for the pgsphere SPoint custom postgres data type. 
This datatype allows sptial index support for RA/DEC cone searches.
"""
import logging

from sqlalchemy import func
from sqlalchemy.types import UserDefinedType

from astropy.coordinates import SkyCoord

logger = logging.getLogger(__name__)

class SPoint(UserDefinedType):
    """ SQLAlchemy user defined type for the pgsphere SPoint datatype. This
    type allows for spherical coordinates.
    """

    @classmethod
    def convert(cls, ra, dec):
        """Convert ra/dec in decimal degrees to an SPoint.
        Args:
            ra  (str)- A right asscension in [+|-]DDD.DDD format.
            dec (str)- A declination in [+|-]DDD.DDD format.

        Returns: (str) - The coordinates converted to format usable by the pgsphere plugin for PostgreSQL.
                         Will return None if the coordinates had some error that prevented them from being
                         converted.
        """
        try:
            float(ra)
            float(dec)
        except Exception as e:
            # If the values can't be converted to floats, they're invalid
            logger.error(f"Could not convert RA/DEC {ra}/{dec} to an SPoint: {e}", exc_info=True)
            return None
        return f'({ra}d, {dec}d)'

    @classmethod
    def convert_hmsdms(cls, ra, dec):
        """Convert ra/dec in hms format to an SPoint.
        Args:
            ra  (str)- A right asscension in [+|-]HH:MM:SS.SSS format
            dec (str)- A declination in [+|-]DD:HH:MM:SS.SSS format

        Returns: (str) - The coordinates converted to format usable by the pgsphere plugin for PostgreSQL.
                         Will return None if the coordinates had some error that prevented them from being
                         converted.
        """
        split_ra = ra.split(":")
        if len(split_ra) != 3:
            logger.error(f"RA value {ra} is not valid hms format.")
            return None

        split_dec = dec.split(":")
        if len(split_dec) != 3:
            logger.error(f"DEC value {dec} is not valid dms format.")
            return None

        # Use Astropy SkyCoord to deal with weird coordinates, like hms with 60 as the seconds
        try:
            coord = SkyCoord(f'{split_ra[0]}h {split_ra[1]}m {split_ra[2]}s', f'{split_dec[0]}d {split_dec[1]}m {split_dec[2]}s')
            (output_ra, output_dec) = coord.to_string(style='decimal').split()

            return cls.convert(output_ra, output_dec)
        except Exception as e:
            logger.error(f"Astropy SkyCoord could not parse RA/DEC {ra}/{dec} as hms/dms format.")
            return None

    def get_col_spec(self):
        return "SPOINT"

    def bind_expression(self, bindvalue):
        return func.spoint(bindvalue)


