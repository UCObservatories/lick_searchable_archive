
from datetime import datetime, date
import logging

from readers.metadata_reader import MetadataReader
from readers.reader_utils import safe_header, parse_file_date, get_shane_lamp_status, get_ra_dec
from archive_schema import  Main, FrameType

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

        if filter2 == "Blank25":
            return FrameType.dark

        if lamps is None or not any(lamps):
            logger.debug("Could not find lamps, using OBJECT to determine frame type.")
            if object is not None:
                if "dark" in object.lower():
                    return FrameType.dark
                elif "flat" in object.lower():
                    return FrameType.flat
                elif "bias" in object.lower():
                    return FrameType.bias
                elif len(object) > 1:
                    return FrameType.science
        else:
            if any([lamps[i] for i in range(0, 5)]):
                # If any dome lights are on this is considered a flat
                return FrameType.flat
            
            elif any([lamps[i] for i in range(5, 16)]):
                return FrameType.arc

        return FrameType.unknown

    def read_row(self, file_path, hdul):
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
            # Check for weird out of sync DATE-OBS
            filename_date = parse_file_date(file_path)
            m.obs_date = None
            date_obs = safe_header(header, 'DATE-OBS')
            if date_obs is not None and date_obs == filename_date:
                time_obs = safe_header(header, 'TIME-OBS')
                if time_obs is not None:
                    m.obs_date = datetime.strptime(f"{date_obs}T{time_obs}+00:00", '%Y-%m-%dT%H:%M:%S.%f%z')
                    logger.debug("Did not find DATE-BEG, but DATE-OBS/TIME-OBS seem sane, using those")
            else:
                logger.debug("DATE-OBS is on a different day than the directory name, not using.")
            if m.obs_date is None:
                logger.debug("Using directory date for observation date.")
                m.obs_date = datetime.strptime(f"{filename_date}T00:00:00+00:00", '%Y-%m-%dT%H:%M:%S%z')

        m.coadds_done = safe_header(header, 'COADDONE')
        m.true_int_time = safe_header(header, 'TRUITIME')
        if m.true_int_time is not None and m.coadds_done is not None:
            m.exptime = m.true_int_time * m.coadds_done
        else:
            m.exptime           = None    
        
        (m.ra, m.dec, m.coord) = get_ra_dec(header)

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
        m.frame_type = self.determine_frame_type(m.object, m.filter2, lamp_status)
        return m

