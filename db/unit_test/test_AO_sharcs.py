from astropy.io import fits
from pathlib import Path

from ingest.shane_ao_sharcs import ShaneAO_ShARCS
from archive_schema import FrameType, IngestFlags
from unit_test.utils import get_hdul_from_text
import os 
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import BIT
from sqlalchemy import cast


def test_ao_sharcs():
    test_data_dir = Path(__file__).parent / 'test_data'

    file = '2014-05_20_AO_s0002-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneAO_ShARCS.can_read(path, hdul) is True

    reader = ShaneAO_ShARCS()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'ShaneAO/ShARCS'
    assert row.obs_date == datetime(2014, 5, 20, 22, 49, 25, 515000, tzinfo=timezone.utc)
    assert row.ingest_flags == '00000000000000000000000000000110'
    assert row.exptime == 0.9797
    assert row.ra == '08:37:44.74'
    assert row.dec == '37:22:13.2'
    assert row.object == 'domeflats'
    assert row.program == 'Keplertargets'
    assert row.observer == 'Wolfgang'
    assert row.airmass == 1.00097284
    assert row.frame_type == FrameType.flat
    assert row.slit_name == None
    assert row.beam_splitter_pos == None
    assert row.grism == None
    assert row.grating_name == None
    assert row.grating_tilt == None

    assert row.apername == 'Slit-100um-H'
    assert row.filter1 == 'CaF-Kgrism'
    assert row.filter2 == 'K'
    assert row.sci_filter is None


    file = '2014-05_20_AO_s0010-001-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneAO_ShARCS.can_read(path, hdul) is True

    reader = ShaneAO_ShARCS()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'ShaneAO/ShARCS'
    assert row.obs_date == datetime(2014, 5, 20, 0, 0, 0, 0, tzinfo=timezone.utc)
    assert row.ingest_flags == '00000000000000000000001000011011'
    assert row.exptime == None
    assert row.ra == None
    assert row.dec == None
    assert row.object == None
    assert row.program == None
    assert row.observer == None
    assert row.frame_type == FrameType.unknown
    assert row.slit_name == None
    assert row.beam_splitter_pos == None
    assert row.grism == None
    assert row.grating_name == None
    assert row.grating_tilt == None

    assert row.apername is None
    assert row.filter1 is None
    assert row.filter2 is None
    assert row.sci_filter is None

    file = '2014-05_20_AO_s0011-1-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneAO_ShARCS.can_read(path, hdul) is True

    reader = ShaneAO_ShARCS()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'ShaneAO/ShARCS'
    assert row.obs_date == datetime(2014, 5, 20, 23, 15, 39, 78000, tzinfo=timezone.utc)
    assert row.ingest_flags == '00000000000000000000001000010111'
    assert row.exptime == (0.09797 * 2)
    assert row.frame_type == FrameType.unknown

    file = '2014-05_20_AO_s0051-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneAO_ShARCS.can_read(path, hdul) is True

    reader = ShaneAO_ShARCS()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'ShaneAO/ShARCS'
    assert row.obs_date == datetime(2014, 5, 20, 0, 0, 0, 0, tzinfo=timezone.utc)
    assert row.ingest_flags == '00000000000000000000000000001010'
    assert row.exptime == 29.0958
    assert row.frame_type == FrameType.flat

    file = '2018-11_20_AO_s0066-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneAO_ShARCS.can_read(path, hdul) is True

    reader = ShaneAO_ShARCS()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'ShaneAO/ShARCS'
    assert row.obs_date == datetime(2018, 11, 21, 7, 56, 19, 755000, tzinfo=timezone.utc)
    assert row.ingest_flags == '00000000000000000000000000000000'
    assert row.ra == 41.843781
    assert row.dec == 43.401867
    assert row.frame_type == FrameType.science

    assert row.apername == 'Open'
    assert row.filter1 == 'BrG-2.16'
    assert row.filter2 == 'Open'
    assert row.sci_filter is None

    file = '2019-04_21_AO_s0180-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneAO_ShARCS.can_read(path, hdul) is True

    reader = ShaneAO_ShARCS()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'ShaneAO/ShARCS'
    assert row.ingest_flags == '00000000000000000000000000010000'
    assert row.object == ''
    assert row.frame_type == FrameType.science

    file = '2019-07_18_AO_s1173-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneAO_ShARCS.can_read(path, hdul) is True

    reader = ShaneAO_ShARCS()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'ShaneAO/ShARCS'
    assert row.obs_date == datetime(2019, 7, 18, 22, 52, 14, 273000, tzinfo=timezone.utc)
    assert row.ingest_flags == '00000000000000000000001000010001'
    assert row.object is None
    assert row.frame_type == FrameType.unknown
