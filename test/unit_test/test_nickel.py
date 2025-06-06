import pytest

from astropy.coordinates import IllegalSecondWarning, Angle
from astropy.logger import AstropyUserWarning
from pathlib import Path

from lick_archive.metadata.nickel import NickelReader
from lick_archive.metadata.data_dictionary import FrameType, Telescope, Instrument, IngestFlags
from lick_archive.metadata.metadata_utils import get_hdul_from_text
import os 
from datetime import datetime, timezone, timedelta


def test_can_read():
    # Test with nickel as parent
    assert NickelReader.can_read(Path("/data/2006-06/nickel/lick320.fits"), []) is True

    # Test without nickel as parent
    assert NickelReader.can_read(Path("/data/2006-06/shane/lick320.fits"), []) is False

    
def test_nickel_frame_type():
    test_data_dir = Path(__file__).parent / 'test_data'

    # Test None obstype and object:  2006-12_06_nickel_lick320-no-object-hdu0.txt
    file = '2006-12_06_nickel_lick320-no-object-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_SPEC
    assert row.filename == str(path)
    assert row.obs_date == datetime(2006, 12, 6, 21, 0, 4, 20000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.NO_OBJECT_IN_HEADER | IngestFlags.NO_OBSTYPE)
    assert row.exptime == 1.0
    assert row.ra == '05:47:17.4'
    assert row.dec == '64:11:41.0'
    assert row.object == None
    assert row.program == 'NEWCAM'
    assert row.observer == 'Laura Langland-Shula'
    assert row.frame_type == FrameType.unknown
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'R'
    assert row.filter2 is None
    assert row.sci_filter is None


    # Test DARK obstype, exptime == 0:
    file = '2007-03_17_nickel_g953-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_DIR
    assert row.filename == str(path)
    assert row.obs_date == datetime(2007, 3, 18, 7, 37, 5, 660000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.CLEAR)
    assert row.exptime == 0.0
    assert row.ra == '09:57:34.6'
    assert row.dec == '-00:30:56.0'
    assert row.object == 'dark'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Wong + Marchis'
    assert row.frame_type == FrameType.bias
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'V'
    assert row.filter2 is None
    assert row.sci_filter is None

    # Test DARK obstype, exptime > 0: 2019-05/13/nickel/d185.fits
    file = '2019-05_13_nickel_d185-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_DIR
    assert row.filename == str(path)
    assert row.obs_date == datetime(2019, 5, 14, 18, 0, 19, 200000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.CLEAR)
    assert row.exptime == 15.0
    assert row.ra == 20.84348487854
    assert row.dec == 39.75394058228
    assert row.object == 'morning flats'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Naina Asaravala, Nick Yee'
    assert row.frame_type == FrameType.dark
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'V'
    assert row.filter2 is None
    assert row.sci_filter is None

    # Test None obstype, and "dark object":
    file = '2007-03_17_nickel_g953-no-obstype-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_DIR
    assert row.filename == str(path)
    assert row.obs_date == datetime(2007, 3, 18, 7, 37, 5, 660000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.NO_OBSTYPE)
    assert row.exptime == 0.0
    assert row.ra == '09:57:34.6'
    assert row.dec == '-00:30:56.0'
    assert row.object == 'dark'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Wong + Marchis'
    assert row.frame_type == FrameType.unknown
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'V'
    assert row.filter2 is None
    assert row.sci_filter is None

    # Test bias exptime == 0: 2019-05/06/nickel/d109.fits
    file = '2019-05_06_nickel_d109-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_DIR
    assert row.filename == str(path)
    assert row.obs_date == datetime(2019, 5, 7, 4, 14, 46, 960000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.CLEAR)
    assert row.exptime == 0.0
    assert row.ra == 167.069442749
    assert row.dec == 39.94518280029
    assert row.object == 'Bias'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Matthew Salinas, Jonathan Hood, Tatiana Gibson'
    assert row.frame_type == FrameType.bias
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'B'
    assert row.filter2 is None
    assert row.sci_filter is None

    # Test bias exptime > 1: 2007-04/24/nickel/spec200.ccd
    file = '2007-04_24_nickel_spec200-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_SPEC
    assert row.filename == str(path)
    assert row.obs_date == datetime(2007, 4, 25, 3, 8, 54, 0, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.CLEAR)
    assert row.exptime == 30.0
    assert row.ra == '09:14:45.8'
    assert row.dec == '-07:31:40.0'
    assert row.object == 'bias'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Stanley Browne'
    assert row.frame_type == FrameType.flat
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'U'
    assert row.filter2 is None
    assert row.sci_filter is None
    
    # Test flat: 2006-09/20/nickel/spec122.ccd
    file = '2006-09_20_nickel_spec122-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_SPEC
    assert row.filename == str(path)
    assert row.obs_date == datetime(2006, 9, 21, 9, 21, 18, 970000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.CLEAR)
    assert row.exptime == 25.0
    assert row.ra == '01:00:59.4'
    assert row.dec == '13:30:35.0'
    assert row.object == 'flat field lamp'
    assert row.program == 'NEWCAM'
    assert row.observer == 'S. Brown'
    assert row.frame_type == FrameType.flat
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'OPEN'
    assert row.filter2 is None
    assert row.sci_filter is None

    # Test lamp: 2006-12/06/nickel/spec100.ccd
    file = '2006-12_06_nickel_spec100-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_SPEC
    assert row.filename == str(path)
    assert row.obs_date == datetime(2006, 12, 7, 5, 9, 4, 770000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.CLEAR)
    assert row.exptime == 2.0
    assert row.ra == '00:15:01.8'
    assert row.dec == '20:15:02.0'
    assert row.object == 'neon lamp'
    assert row.program == 'NEWCAM'
    assert row.observer == 'S. Browne'
    assert row.frame_type == FrameType.arc
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'OPEN'
    assert row.filter2 is None
    assert row.sci_filter is None

    # Test hg: 2007-03/29/nickel/spec154.ccd
    file = '2007-03_29_nickel_spec154-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_SPEC
    assert row.filename == str(path)
    assert row.obs_date == datetime(2007, 3, 30, 10, 35, 36, 520000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.CLEAR)
    assert row.exptime == 2.0
    assert row.ra == '12:23:03.4'
    assert row.dec == '27:33:53.0'
    assert row.object == 'Hg'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Stanley Browne'
    assert row.frame_type == FrameType.arc
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'OPEN'
    assert row.filter2 is None
    assert row.sci_filter is None

    # Test focus: 2019-05/24/nickel/d116.fits
    file = '2019-05_24_nickel_d116-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_DIR
    assert row.filename == str(path)
    assert row.obs_date == datetime(2019, 5, 25, 7, 31, 59, 160000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.CLEAR)
    assert row.exptime == 80.13
    assert row.ra == 217.7646026611
    assert row.dec == 28.00996017456
    assert row.object == 'Focusing 14'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Naina Asaravala, Nick Yee'
    assert row.frame_type == FrameType.focus
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'V'
    assert row.filter2 is None
    assert row.sci_filter is None


    # Test science frames
    # None obstype, science object: 2006-12/06/nickel/lick320.fits
    file = '2006-12_06_nickel_lick320-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_SPEC
    assert row.filename == str(path)
    assert row.obs_date == datetime(2006, 12, 6, 21, 0, 4, 20000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.NO_OBSTYPE)
    assert row.exptime == 1
    assert row.ra == '05:47:17.4'
    assert row.dec == '64:11:41.0'
    assert row.object == 'GRB061126'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Laura Langland-Shula'
    assert row.frame_type == FrameType.science
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'R'
    assert row.filter2 is None
    assert row.sci_filter is None


def test_nickel_instrument():

    test_data_dir = Path(__file__).parent / 'test_data'

    # Test No version, no instrume: 2007-04_24_nickel_spec289-no-version-no-instr-hdu0.txt
    file = '2007-04_24_nickel_spec289-no-version-no-instr-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    with pytest.raises(ValueError, match="Unknown instrument"):
        row = reader.read_row(path, hdul)

    # Test bad version
    # 2007-04_24_nickel_spec289-bad-version-hdu0.txt
    file = '2007-04_24_nickel_spec289-bad-version-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    with pytest.raises(ValueError, match="Unknown instrument"):
        row = reader.read_row(path, hdul)

    # Test with no version/ bad instrume
    # 2007-04_24_nickel_spec289-bad-version-bad-instr-hdu0.txt
    file = '2007-04_24_nickel_spec289-bad-version-bad-instr-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    with pytest.raises(ValueError, match="Unrecognized instrument"):
        row = reader.read_row(path, hdul)


    # Test for good instrument values done in other tests

def test_nickel_date():
    test_data_dir = Path(__file__).parent / 'test_data'

    # Test DATE-BEG 2018-06_24_nickel_d38034-no-date-hdu0.txt
    file = '2018-06_24_nickel_d38034-no-date-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_DIR
    assert row.filename == str(path)
    assert row.obs_date == datetime(2018, 6, 25, 2, 37, 11, 130000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.CLEAR)
    assert row.exptime == 3
    assert row.ra == 190.7661437988
    assert row.dec == -5.044168949127
    assert row.object == 'dflat'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Charles Kilpatrick'
    assert row.frame_type == FrameType.flat
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'V'
    assert row.filter2 is None
    assert row.sci_filter is None


    # Test no date at all
    # 2012-01_25_nickel_d189-no-date-hdu0.txt
    file = '2012-01_25_nickel_d189-no-date-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_DIR
    assert row.filename == str(path)
    assert row.obs_date == datetime(2012, 1, 25, 20, 0, 0, 0, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.USE_DIR_DATE)
    assert row.exptime == 20
    assert row.ra == '08:51:48.1'
    assert row.dec == '11:49:36.0'
    assert row.object == 'm67'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Daniel Cohen, BY Choi'
    assert row.frame_type == FrameType.science
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'V'
    assert row.filter2 is None
    assert row.sci_filter is None

def test_nickel_coord():
    test_data_dir = Path(__file__).parent / 'test_data'

    # Test no coord: 2012-01_25_nickel_d189-no-coord-hdu0.txt
    file = '2012-01_25_nickel_d189-no-coord-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    reader = NickelReader()
    row = reader.read_row(path, hdul)

    assert row.telescope == Telescope.NICKEL
    assert row.instrument == Instrument.NICKEL_DIR
    assert row.filename == str(path)
    assert row.obs_date == datetime(2012, 1, 26, 7, 7, 32, 630000, tzinfo=timezone.utc)
    assert row.ingest_flags == "{:032b}".format(IngestFlags.NO_COORD)
    assert row.exptime == 20
    assert row.ra is None
    assert row.dec is None
    assert row.coord is None
    assert row.object == 'm67'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Daniel Cohen, BY Choi'
    assert row.frame_type == FrameType.science
    assert row.slit_name is None
    assert row.beam_splitter_pos is None
    assert row.grism is None
    assert row.grating_name is None
    assert row.grating_tilt is None

    assert row.apername is None
    assert row.filter1 == 'V'
    assert row.filter2 is None
    assert row.sci_filter is None


