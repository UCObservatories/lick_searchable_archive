import pytest

from astropy.coordinates import IllegalSecondWarning, Angle
from astropy.logger import AstropyUserWarning
from pathlib import Path

from lick_archive.metadata.apf import APFReader
from lick_archive.metadata.data_dictionary import FrameType, Telescope, Instrument, IngestFlags
from lick_archive.metadata.metadata_utils import get_hdul_from_text
import os 
from datetime import datetime, timezone, timedelta

def test_can_read():
    # Test with apf as parent
    assert APFReader.can_read(Path("/data/2019-05/23/APF/ucb-blo178.fits"), []) is True

    # Test without apf as parent
    assert APFReader.can_read(Path("/data/2019-05/23/shane/ucb-blo178.fits"), []) is False

def test_frame_type():

    reader = APFReader()

    # Test unknown, too early obs_date
    decker = "W (1.00:3.0)"
    obstype = "OBJECT"
    object = None
    obs_date = datetime.fromisoformat('2011-12-31T00:00:00.000Z')
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.unknown
    assert ingest_flags == IngestFlags.OLD


    # Test unknown, no OBJECT
    obs_date = datetime.fromisoformat('2012-01-01T00:00:00.000Z')
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.unknown
    assert ingest_flags == IngestFlags.NO_OBJECT_IN_HEADER

    # Test object with bias
    object = "soemthing BIas something"
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.bias
    assert ingest_flags == IngestFlags.CLEAR

    # Test object with dark
    object = "soemthing daRk something"
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.dark
    assert ingest_flags == IngestFlags.CLEAR

    # Test object with wideflat is a flat
    object = "soemthing wideflat something"
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.flat
    assert ingest_flags == IngestFlags.CLEAR

    # Test object with narrowflat is a flat
    object = "soemthing narrowflat something"
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.flat
    assert ingest_flags == IngestFlags.CLEAR

    # Test object with iodine is a flat
    object = "soemthing iodine something"
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.flat
    assert ingest_flags == IngestFlags.CLEAR

    # Test object with ThAr is an arc. ThAr may or may not be one word
    object = "soemthing ThAr something"
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.arc
    assert ingest_flags == IngestFlags.CLEAR

    object = "soemthing Th Ar something"
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.arc
    assert ingest_flags == IngestFlags.CLEAR

    # Test object with pinhole and no decker is pinhole 
    object = "soemthing pinhole something"
    decker = None
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.pinhole
    assert ingest_flags == IngestFlags.CLEAR

    decker = ''
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.pinhole
    assert ingest_flags == IngestFlags.CLEAR

    # Test object with pinhole and decker = Pinhole is pinhole
    decker = "Pinhole"
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.pinhole
    assert ingest_flags == IngestFlags.CLEAR

    # Test object with pinhole and non-PinHhole decker is unknown
    decker = "W"
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.unknown
    assert ingest_flags == IngestFlags.UNKNOWN_FORMAT

    # Test object with irrevelevant text, but OBSTYPE == DARK is treated as a dark
    object = "test"
    obstype = "DARK"
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.dark
    assert ingest_flags == IngestFlags.CLEAR

    # Test Science
    obstype = "OBJECT"
    frame_type, ingest_flags = reader.determine_frame_type(obs_date, decker, obstype, object)
    assert frame_type == FrameType.science
    assert ingest_flags == IngestFlags.CLEAR

def test_apf_valid_files():
    test_data_dir = Path(__file__).parent / 'test_data'

    # Test file with:
    # TOBJECT and OBJECT
    # DATE-BEG

    file = '2019-05_23_apf_ucb-blo178-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = APFReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.APF
    assert row.instrument == Instrument.APF
    assert row.filename == str(path)
    assert row.obs_date == datetime(2019, 5, 24, 4, 4, 19, 130000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.CLEAR)
    assert row.exptime == 3.97
    assert row.ra == '11:45:17.0'
    assert row.dec == '8:15:29.2'
    assert row.object == 'HR4515'
    assert row.program == 'NEWCAM'
    assert row.observer == 'ucb-blo'
    assert row.frame_type == FrameType.science
    assert row.decker == 'W (1.00:3.0)'
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 is None
    assert row.filter2 is None
    assert row.sci_filter is None

    # Test file with:
    # DATE-STA
    # no TOBJECT
    # No decker
    file = '2011-05_18_APF_apff839-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.APF
    assert row.instrument == Instrument.APF
    assert row.filename == str(path)
    assert row.obs_date == datetime(2011, 5, 19, 17, 20, 36, 0, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.OLD)
    assert row.exptime == 0.0
    assert row.ra == '+00:29:43'
    assert row.dec == '+00:00:00'
    assert row.object == 'Baseline'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Radovan'
    assert row.frame_type == FrameType.unknown
    assert row.decker is 'Unknown'
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 is None
    assert row.filter2 is None
    assert row.sci_filter is None

def test_apf_missing_values():

    test_data_dir = Path(__file__).parent / 'test_data'

    # Test file with no DATE-BEG and no DATE-STA and no RA/DEC
    file = '2011-05_18_APF_apff839-no-date-no-coord-hdu0.txt'

    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = APFReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.APF
    assert row.instrument == Instrument.APF
    assert row.filename == str(path)
    # Date should be Noon PST on the directories date
    assert row.obs_date == datetime(2011, 5, 18, 20, 00, 00, 0, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.USE_DIR_DATE | IngestFlags.NO_COORD | IngestFlags.OLD)
    assert row.exptime == 0.0
    assert row.ra is None
    assert row.dec is None
    assert row.object == 'Baseline'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Radovan'
    assert row.frame_type == FrameType.unknown
    assert row.decker is 'Unknown'
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 is None
    assert row.filter2 is None
    assert row.sci_filter is None

def test_apf_bad_date():
    test_data_dir = Path(__file__).parent / 'test_data'

    # Test file with:
    # TOBJECT and OBJECT
    # DATE-BEG

    file = '2019-05_23_apf_ucb-blo178-bad-date-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = APFReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.APF
    assert row.instrument == Instrument.APF
    assert row.filename == str(path)
    # Date should be Noon PST on the directories date
    assert row.obs_date == datetime(2019, 5, 23, 20, 00, 00, 0, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.USE_DIR_DATE)
    assert row.exptime == 3.97
    assert row.ra == '11:45:17.0'
    assert row.dec == '8:15:29.2'
    assert row.object == 'HR4515'
    assert row.program == 'NEWCAM'
    assert row.observer == 'ucb-blo'
    assert row.frame_type == FrameType.science
    assert row.decker == 'W (1.00:3.0)'
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 is None
    assert row.filter2 is None
    assert row.sci_filter is None

