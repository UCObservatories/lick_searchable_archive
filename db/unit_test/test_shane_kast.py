import pytest

from astropy.io import fits
from pathlib import Path

from ingest.shane_kast import ShaneKastReader
from archive_schema import FrameType, IngestFlags
from unit_test.utils import get_hdul_from_text
import os 
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import BIT
from sqlalchemy import cast


def test_not_shane_kast():
    test_data_dir = Path(__file__).parent / 'test_data'

    file = '2019-11_09_AO_s1303-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is False

    reader = ShaneKastReader()
    with pytest.raises(KeyError):
        row = reader.read_row(path, hdul)



def test_red_headers():
    test_data_dir = Path(__file__).parent / 'test_data'

    file = '2012-01_02_shane_r36-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is True

    reader = ShaneKastReader()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'Kast Red'
    assert row.obs_date == datetime(2012, 1, 3, 6, 12, 31, 110000, tzinfo=timezone.utc)
    assert row.ingest_flags == '00000000000000000000000000000001'
    assert row.exptime == 1.0
    assert row.ra == '01:42:48.5'
    assert row.dec == '29:22:13.0'
    assert row.object == 'IR flat'
    assert row.program == 'NEWCAM'
    assert row.observer == 'Silverman'
    assert row.frame_type == FrameType.flat
    assert row.slit_name == '2.0 arcsec'
    assert row.beam_splitter_pos == 'd55'
    assert row.grism == '600/4310'
    assert row.grating_name == '300/7500'
    assert row.grating_tilt == 5099

    assert row.apername is None
    assert row.filter1 is None
    assert row.filter2 is None
    assert row.sci_filter is None


    file = '2018-11_16_shane_r1011-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is True

    reader = ShaneKastReader()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'Kast Red'
    # This image uses WCS to store coordinates, so its in decimal degrees
    # instead of hms
    assert row.ra == 343.1081542969
    assert row.dec == 37.27479171753
    assert row.frame_type == FrameType.arc

    assert row.ingest_flags == "00000000000000000000000000000000"

    file = '2019-12_16_shane_r5079-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is True

    reader = ShaneKastReader()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'Kast Red'

    assert row.frame_type == FrameType.unknown
    assert row.ingest_flags == "00000000000000000000000000011001"
    assert row.object is None
    assert row.obs_date == datetime(2019, 12, 16, 0, 0, 0, 0, tzinfo=timezone.utc)

    file = '2019-05_02_shane_r684-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is True

    reader = ShaneKastReader()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'Kast Red'

    assert row.frame_type == FrameType.bias
    assert row.ingest_flags == "00000000000000000000000000000000"

    file = '2019-05_02_shane_r650-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is True

    reader = ShaneKastReader()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'Kast Red'

    assert row.frame_type == FrameType.science
    assert row.ingest_flags == "00000000000000000000000000000000"

    file = '2012-01_20_shane_r104-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is True

    reader = ShaneKastReader()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'Kast Red'

    assert row.frame_type == FrameType.unknown
    assert row.ingest_flags == "00000000000000000000000000010001"
    assert row.object == ""



def test_blue_headers():
    test_data_dir = Path(__file__).parent / 'test_data'

    file = '2012-01_02_shane_b808-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is True

    reader = ShaneKastReader()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'Kast Blue'
    assert row.ingest_flags == '00000000000000000000000000000001'
    assert row.frame_type == FrameType.science

    file = '2018-11_16_shane_b1004-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is True

    reader = ShaneKastReader()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'Kast Blue'
    assert row.frame_type == FrameType.arc

    assert row.ingest_flags == "00000000000000000000000000000000"

    file = '2019-05_04_shane_b2-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is True

    reader = ShaneKastReader()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'Kast Blue'

    assert row.frame_type == FrameType.unknown
    assert row.ingest_flags == "00000000000000000000000000011001"
    assert row.object is None
    assert row.obs_date == datetime(2019, 5, 4, 0, 0, 0, 0, tzinfo=timezone.utc)

    file = '2019-05_02_shane_b607-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is True

    reader = ShaneKastReader()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'Kast Blue'

    assert row.frame_type == FrameType.flat
    assert row.ingest_flags == "00000000000000000000000000000000"

    file = '2012-01_18_shane_b1011-hdu0.txt'
    hdul = get_hdul_from_text([test_data_dir / file])
    path = Path(file.replace("_", os.sep).replace(".txt", ".fits"))

    assert ShaneKastReader.can_read(path, hdul) is True

    reader = ShaneKastReader()
    row = reader.read_row(path, hdul)
    assert row.telescope == 'Shane'
    assert row.instrument == 'Kast Blue'

    assert row.frame_type == FrameType.bias
    assert row.ingest_flags == "00000000000000000000000000000001"

