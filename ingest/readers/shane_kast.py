from datetime import datetime
import logging

from readers.metadata_reader import MetadataReader
from readers.reader_utils import safe_header, parse_file_date, get_shane_lamp_status, get_ra_dec
from archive_schema import  Main, FrameType
from pgsphere import SPoint

logger = logging.getLogger(__name__)

class ShaneKastReader(MetadataReader):
    @classmethod
    def can_read(cls, file_path, hdul):
        if "shane" in str(file_path.parent):
            if safe_header(hdul[0].header,'VERSION' ) in ['kastr', 'kastb']:
                return True
        return False
    

    def determine_frame_type(self, exptime, lamps, object):

        if lamps is None:
            logger.debug("No lamps information, using OBJECT to determine frame type.")
            if object is not None:
                if 'dark' in object.lower():
                    return FrameType.dark
                elif 'flat' in object.lower():
                    return FrameType.flat
                elif 'bias' in object.lower():
                    return FrameType.bias
                elif len(object) > 1:
                    return FrameType.science

        else:

            if not any(lamps):
                if exptime > 1:
                    return FrameType.science

            if exptime <= 1:
                return FrameType.bias
            else:
                if any([lamps[i] for i in range(0, 5)]):
                    # If any dome lights are on this is considered a flat
                    return FrameType.flat
                
                elif any([lamps[i] for i in range(5, 16)]):
                    if exptime <= 61:
                        return FrameType.arc

        return FrameType.unknown


    def read_row(self, file_path, hdul):
        header = hdul[0].header
        m = Main()
        m.telescope = 'Shane'
        # All of the shane kast examples I've seen have 
        # VERSION set, so this is intended to throw an
        # exception if there's no VERSION to differentiate any older
        # date files in the "shane" directory that aren't from kast.
        instrument = header['VERSION'] 
        if instrument == "kastb":
            m.instrument = "Kast Blue"
        elif instrument == "kastr":
            m.instrument = "Kast Red"
        else:
            raise ValueError(f"Unrecognized instrument for Shane telescope: '{instrument}'.")

        date_obs = safe_header(header, 'DATE-OBS')
        if date_obs is None:
            logger.debug(f"Used file path for date for file {file_path}.")
            filename_date = parse_file_date(file_path)
            m.obs_date = datetime.strptime(f"{filename_date}T00:00:00+00:00", '%Y-%m-%dT%H:%M:%S%z')
        else:
            # Parse the observation date as an iso date, adding +00:00 to make it UTC
            m.obs_date = datetime.strptime(header['DATE-OBS'] + "+00:00", '%Y-%m-%dT%H:%M:%S.%f%z')


        m.exptime           = safe_header(header, 'EXPTIME')

        (m.ra, m.dec, m.coord) = get_ra_dec(header)

        m.object            = safe_header(header, 'OBJECT')
        m.slit_name         = safe_header(header, 'SLIT_N')
        m.airmass           = safe_header(header, 'AIRMASS')
        m.beam_splitter_pos = safe_header(header, 'BSPLIT_N')
        m.grism             = safe_header(header, 'GRISM_N')
        m.grating_name      = safe_header(header, 'GRATNG_N')
        m.grating_tilt      = safe_header(header, 'GRTILT_P')

        m.apername = None
        m.filter1 = None
        m.filter2 = None
        m.filter3 = None
        m.program = safe_header(header,'PROGRAM')
        m.observer = safe_header(header,'OBSERVER')


        m.filename = str(file_path)

        lamp_status = get_shane_lamp_status(header)
        m.frame_type = self.determine_frame_type(m.exptime, lamp_status, m.object)
        return m

        
