"""
MetadataReader implementation for Shane Kast data.
"""

from datetime import datetime
import logging
from pathlib import Path

from astropy.io.fits import HDUList

from dateutil.parser import parse

from lick_archive.metadata.abstract_reader import AbstractReader
from lick_archive.metadata.metadata_utils import safe_header, safe_strip, parse_file_name, get_ra_dec, validate_header
from lick_archive.db.archive_schema import  FileMetadata
from lick_archive.metadata.data_dictionary import FrameType, IngestFlags, Instrument, Telescope

logger = logging.getLogger(__name__)

class NickelReader(AbstractReader):
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
        if "nickel" == file_path.parent.name:
            return True

        return False
    


    def determine_frame_type(self, exptime : float, obstype : str | None, object : str | None) -> FrameType:
        """
        Determine the frame type based on exposure time, lamps and object name.
        Parts of this logic was adapted from PypeIt

        Args:
        exptime:  Exposure time in seconds.
        obstype:  The OBSTYPE keyword from the file's header.
        object:   The OBJECT keyword from the file's header.

        Returns (FrameType, IngestFlags): A tuple with the frame type, and any ingest flags set
                                          while determining the frame type.
        """
        ingest_flags = IngestFlags.CLEAR
        frame_type = FrameType.unknown

        # Use the OBSTYPE header card to look for darks
        if obstype is None:
            logger.debug("There's no OBSTYPE in the header, but OBJECT may still be used to determine hte frame type.")
            ingest_flags = ingest_flags | IngestFlags.NO_OBSTYPE
        elif obstype == "DARK":
            if exptime == 0:
                # Treat zero exposure time darks as bias frames
                frame_type = FrameType.bias
            else:
                frame_type = FrameType.dark
            return (frame_type, ingest_flags)
        
        if object is None:
            logger.debug("Cannot determine frame type because there's no OBJECT.")
            ingest_flags = ingest_flags | IngestFlags.NO_OBJECT_IN_HEADER
            # Need object to determine the frame type
            frame_type = FrameType.unknown
        else:
            object = object.lower()
            if "bias" in object:
                if exptime == 0:
                    frame_type = FrameType.bias
                else:
                    frame_type = FrameType.flat
            # Flat should come before lamp, because some objects are "flat field lamp"
            elif "flat" in object:
                frame_type = FrameType.flat
            elif "lamp" in object or "hg" in object:
                frame_type = FrameType.arc
            elif "dark" in object:
                # Dark in the object name but the OBSTYPE isn't DARK? Treat that as unknown
                frame_type = FrameType.unknown
            elif "focus" in object:
                frame_type = FrameType.focus
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
        m.telescope = Telescope.NICKEL

        # Try to determine the instrument type, first try "VERSION",
        # then "INSTRUME"
        m.instrument = None
        instr = safe_header(hdul[0].header,'VERSION')

        if instr is None:
            instr = safe_header(hdul[0].header,'INSTRUME')

        # Look for nickel direct or nickel spectrograph
        if instr is not None:
            if "nickel" in instr.lower():
                if "direct" in instr.lower():
                    m.instrument = Instrument.NICKEL_DIR
                elif "spectrograph" in instr.lower():
                    m.instrument = Instrument.NICKEL_SPEC
                else:
                    logger.warning(f"Unrecognized instrument for Nickel telescope. Found: '{instr}'.")    
            elif "villages" in instr.lower():
                m.instrument = Instrument.VILLAGES
            else:
                logger.warning(f"Unrecognized instrument for Nickel telescope. Found: '{instr}'.")

        if m.instrument is None:
            logger.warning(f"Unknown instrument for Nickel telescope.")
            m.instrument = Instrument.UNKNOWN

        # Older files use "DATE-OBS", newer ones 'DATE' or 'DATE-BEG'
        obs_date = safe_header(header, 'DATE-OBS')
        if obs_date is None:
            obs_date = safe_header(header, 'DATE')
            if obs_date is None:
                obs_date = safe_header(header,'DATE-BEG')

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

        m.object            = safe_strip(safe_header(header, 'OBJECT'))
        m.slit_name         = None
        m.airmass           = safe_header(header, 'AIRMASS')
        m.beam_splitter_pos = None
        m.grism             = None
        m.grating_name      = None
        m.grating_tilt      = None

        m.apername = None

        # Filter names are some times slightly different in the header,
        # but I worry the mapping of FILTORD to filters may change in the future.
        # So I strip any extra spaces, single quotes, and convert to upper case
        filter_name = safe_strip(safe_header(header, 'FILTNAM'))
        if filter_name is not None:
            filter_name.strip("'")
            m.filter1 = filter_name.upper()

        m.filter2 = None
        m.sci_filter = None
        m.program = safe_strip(safe_header(header,'PROGRAM'))

        # Some observer strings have newlines in them
        observer = safe_strip(safe_header(header,'OBSERVER'))
        if observer is not None and not observer.isprintable():
            observer = [c if c.isprintable() else " " for c in observer]
        m.observer = observer


        m.filename = str(file_path)

        (m.frame_type, frame_flags) = self.determine_frame_type(m.exptime, safe_strip(safe_header(header, 'OBSTYPE')), m.object)
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

        
