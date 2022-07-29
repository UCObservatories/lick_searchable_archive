"""
MetadataReader implementation for Shane AO/ShARCS data.
"""
from datetime import datetime, date
import logging

from sqlalchemy.dialects.postgresql import BIT
from sqlalchemy import cast

from ingest.metadata_reader import MetadataReader
from ingest.ingest_utils import safe_header, parse_file_date, get_shane_lamp_status, get_ra_dec
from archive_schema import  Main, FrameType, IngestFlags

logger = logging.getLogger(__name__)

class ShaneAO_ShARCS(MetadataReader):
    @classmethod
    def can_read(cls, file_path, hdul):
        if "AO" in str(file_path.parent):
            filename_date = parse_file_date(file_path)
            file_date_parts = filename_date.split("-")
            file_date = date(year=int(file_date_parts[0]), month=int(file_date_parts[1]), day=int(file_date_parts[2]))
            # Based inspecting data in the archive, there's no ShARCS data before april 2014
            # This differentiates it from older IRCAL data
            if file_date >= date(year=2014, month=4, day=1):
                return True

        return False
    
    def determine_frame_type(self, object, filter2, lamps):

        ingest_flags = IngestFlags.CLEAR
        if filter2 == "Blank25":
            frame_type = FrameType.dark

        elif lamps is None or not any(lamps):
            # Log why we are using object
            if lamps is None:
                ingest_flags = ingest_flags | IngestFlags.NO_LAMPS_IN_HEADER
                logger.debug("Could not find lamps, using OBJECT to determine frame type.")
            else:
                logger.debug("Lamps are off, using OBJECT to double check frame type.")

            if object is not None:
                if "dark" in object.lower():
                    frame_type = FrameType.dark
                elif "flat" in object.lower():
                    frame_type = FrameType.flat
                elif "bias" in object.lower():
                    frame_type = FrameType.bias
                elif len(object.strip()) > 0:
                    frame_type = FrameType.science
                else:
                    if lamps is not None:
                        # If lamps are specified in the header but are all off, 
                        # count it as a science image even if the object is empty
                        frame_type = FrameType.science
                    else:
                        # No lamps in the header and an empty object, treat it as unknown
                        frame_type = FrameType.unknown                        
                    ingest_flags = ingest_flags | IngestFlags.NO_OBJECT_IN_HEADER

            else:
                ingest_flags = ingest_flags | IngestFlags.NO_OBJECT_IN_HEADER
                frame_type = FrameType.unknown

        elif any([lamps[i] for i in range(0, 5)]):
            # If any dome lights are on this is considered a flat
            frame_type = FrameType.flat
            
        elif any([lamps[i] for i in range(5, 16)]):
            # Check for arc lights
            frame_type = FrameType.arc
        else:
            frame_type = FrameType.unknown

        return (frame_type, ingest_flags)

    def read_row(self, file_path, hdul, ingest_flags = IngestFlags.CLEAR):
        header = hdul[0].header
        m = Main()
        m.telescope = 'Shane'
        m.instrument = 'ShaneAO/ShARCS'

        # Parse the observation date as an iso date, adding +00:00 to make it UTC
        
        date_beg = safe_header(header, 'DATE-BEG')
        if date_beg is not None:
            logger.debug("Found DATE-BEG")
            m.obs_date = datetime.strptime(date_beg + "+00:00", '%Y-%m-%dT%H:%M:%S.%f%z')
        else:
            ingest_flags = ingest_flags | IngestFlags.AO_NO_DATE_BEG
            # Check for weird out of sync DATE-OBS
            filename_date = parse_file_date(file_path)
            m.obs_date = None
            date_obs = safe_header(header, 'DATE-OBS')
            if date_obs is not None and date_obs == filename_date:
                time_obs = safe_header(header, 'TIME-OBS')
                if time_obs is not None:
                    ingest_flags = ingest_flags | IngestFlags.AO_USE_DATE_OBS
                    m.obs_date = datetime.strptime(f"{date_obs}T{time_obs}+00:00", '%Y-%m-%dT%H:%M:%S.%f%z')
                    logger.debug("Did not find DATE-BEG, but DATE-OBS/TIME-OBS seem sane, using those")
            else:
                logger.debug("DATE-OBS is on a different day than the directory name, not using.")
            if m.obs_date is None:
                logger.debug("Using directory date for observation date.")
                ingest_flags = ingest_flags | IngestFlags.USE_DIR_DATE
                m.obs_date = datetime.strptime(f"{filename_date}T00:00:00+00:00", '%Y-%m-%dT%H:%M:%S%z')

        m.coadds_done = safe_header(header, 'COADDONE')
        m.true_int_time = safe_header(header, 'TRUITIME')
        if m.true_int_time is not None and m.coadds_done is not None:
            m.exptime = m.true_int_time * m.coadds_done
        else:
            m.exptime           = None    
        
        (m.ra, m.dec, m.coord) = get_ra_dec(header)
        if m.coord is None:
            ingest_flags = ingest_flags | IngestFlags.NO_COORD

        m.object            = safe_header(header, 'OBJECT')
        m.slit_name         = None
        m.airmass           = safe_header(header,'AIRMASS')
        m.beam_splitter_pos = None
        m.grism             = None
        m.grating_name      = None
        m.grating_tilt      = None
        m.filename = str(file_path)
        m.apername = safe_header(header,'APERNAM')
        m.filter1 = safe_header(header,'FILT1NAM')
        m.filter2 = safe_header(header,'FILT2NAM')
        m.sci_filter = safe_header(header,'SCIFILT')
        m.program = safe_header(header,'PROGRAM')
        m.observer = safe_header(header,'OBSERVER')
        lamp_status = get_shane_lamp_status(header)
        (m.frame_type, frame_flags) = self.determine_frame_type(m.object, m.filter2, lamp_status)
        ingest_flags |= frame_flags
        m.ingest_flags = f'{ingest_flags:032b}'
        m.header = header.tostring(sep='\n', endcard=False, padding=False)

        return m

