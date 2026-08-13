"""
MetadataReader implementation for Shane Kast data.
"""

from datetime import datetime, date
import logging
from pathlib import Path
from pickle import INST

from astropy.io.fits import HDUList

from dateutil.parser import parse

from lick_archive.metadata.abstract_reader import AbstractReader
from lick_archive.metadata.metadata_utils import safe_header, safe_strip, parse_file_name, get_ra_dec, validate_header
from lick_archive.db.archive_schema import  FileMetadata
from lick_archive.metadata.data_dictionary import FrameType, IngestFlags, Instrument, Telescope

logger = logging.getLogger(__name__)

class APFReader(AbstractReader):
    """
    Reader implementation for Nickel images.
    """
    @classmethod
    def can_read(cls, file_path : Path, hdul : HDUList) -> bool:
        """
        Determine if a file is Nickel.

        Args:

        file_path (pathlib.Path): 
            Path to the file to check. This should be in the Lick Archive directory
            format (YYYY-MM/DD/<instrument>/<file>).

        hdul (None or astropy.io.fits.HDUList): 
            An HDUList from the file.

        Returns (bool): True if the file is supported, False if it is not.
        """

        # Look for the nickel directory name
        if "APF" == file_path.parent.name:
            return True

        return False
    


    def determine_frame_type(self, obs_date: datetime, decker: str | None, obstype : str | None, object : str | None) -> tuple[FrameType, IngestFlags]:
        """
        Determine the frame type based on exposure time, lamps and object name.
        Parts of this logic was adapted from PypeIt

        Args:
        obs_date: The observation date/time of the file.
        decker:   The DECKRNAM keyword from the file's header.
        obstype:  The OBSTYPE keyword from the file's header.
        object:   The OBJECT keyword from the file's header.

        Returns (FrameType, IngestFlags): A tuple with the frame type, and any ingest flags set
                                          while determining the frame type.
        """
        ingest_flags = IngestFlags.CLEAR
        if obs_date.date() < date(year=2012,month=1,day=1):
            # Products pre-2012 are too inconsistent to determine the frame type
            frame_type=FrameType.unknown
            ingest_flags = IngestFlags.OLD
        
        elif object is None:
            frame_type = FrameType.unknown
            ingest_flags = IngestFlags.NO_OBJECT_IN_HEADER
        elif 'bias' in object.lower():
            frame_type = FrameType.bias
        elif 'dark' in object.lower():
            frame_type = FrameType.dark
        elif 'wideflat' in object.lower() or 'narrowflat' in object.lower() or 'iodine' in object.lower():
            frame_type = FrameType.flat
        elif 'ThAr' in object or 'Th Ar' in object:
            frame_type = FrameType.arc
        elif 'pinhole' in object.lower():
            if decker is None or decker == '':
                # Older data without a DECKRNAM, we'll trust the OBJECT value
                frame_type = FrameType.pinhole
            elif decker == 'Pinhole':
                frame_type = FrameType.pinhole
            else:
                frame_type = FrameType.unknown
                ingest_flags = IngestFlags.UNKNOWN_FORMAT
        elif obstype == 'DARK':
            frame_type = FrameType.dark
        else:
            frame_type = FrameType.science
        return (frame_type, ingest_flags)

    def read_row(self, file_path : Path, hdul : HDUList, ingest_flags : IngestFlags = IngestFlags.CLEAR) -> FileMetadata:
        """Read an SQL Alchemy row of metadata from a file.

        Args:

        file_path (pathlib.Path): 
            The path of the file to read. This should be in the Lick Archive directory
            format (YYYY-MM/DD/<instrument>/<file>).

        hdul (None or astropy.io.fits.HDUList): 
            An HDUList from the file.

        ingest_flags (archive_schema.IngestFlags):
            Any ingest bit flags that were set during the process of opening a FITS file.

        Returns (archive_schema.FileMetadata): A row of metadata read from the file.

        Raises: Exception raised if the file is corrupt or lacks required metadata.
        """

        header = hdul[0].header
       
        m = FileMetadata()
        m.telescope = Telescope.APF

        # Try to determine the instrument type, first try "VERSION",
        # then "INSTRUME"
        m.instrument = Instrument.APF

        # Older files use "DATE-STA" newerones 'DATE-BEG'
        obs_date = safe_header(header, 'DATE-BEG')
        if obs_date is None:
            obs_date = safe_header(header, 'DATE-STA')

        if obs_date is not None:
            try:
                # Parse the observation date as an iso date, adding +00:00 to make it UTC
                m.obs_date = parse(obs_date + "+00:00")
            except Exception as e:
                logger.warning(f"Failed to parse observation date {obs_date + '00:00'}")

        if m.obs_date is None:
            logger.debug(f"Used file path for date for file {file_path}.")
            filename_date, instr = parse_file_name(file_path)
            # Use noon Lick time (aka UTC-8)
            m.obs_date = parse(f"{filename_date}T12:00:00-08:00")
            ingest_flags = ingest_flags | IngestFlags.USE_DIR_DATE

        m.exptime           = safe_header(header, 'EXPTIME')

        (m.ra, m.dec, m.coord) = get_ra_dec(header)
        if m.coord is None:
            ingest_flags = ingest_flags | IngestFlags.NO_COORD

        tobject = safe_strip(safe_header(header,'TOBJECT'))
        object = safe_strip(safe_header(header,'OBJECT'))

        # Use TOBJECT if it's given which it should be for newer data that has a target.
        # For other data (either old stuff or calibrations) use OBJECT
        m.object = object if tobject is None or tobject == '' else tobject

        decker = safe_strip(safe_header(header, 'DECKRNAM'))
        if decker is None:
            m.decker = "Unknown"
        else:
            m.decker = decker
        

        m.program = safe_strip(safe_header(header,'PROGRAM'))

        # Some observer strings have newlines in them
        m.observer = safe_strip(safe_header(header,'OBSERVER'))


        m.filename = str(file_path)

        (m.frame_type, frame_flags) = self.determine_frame_type(m.obs_date, m.decker, safe_strip(safe_header(header, 'OBSTYPE')), object)
        ingest_flags |= frame_flags

        # Save the header for future updates, and 
        # check for an invalid \x00 in the header string, which the DB rejects
        m.header = header.tostring(sep='\n', endcard=False, padding=False)
        valid, fixed_header = validate_header(m.header)
        if not valid:
            m.header = fixed_header
            ingest_flags |= IngestFlags.INVALID_CHAR

        m.ingest_flags = f'{ingest_flags:032b}'            
        return m

        
