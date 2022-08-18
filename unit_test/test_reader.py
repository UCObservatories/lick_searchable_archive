import pytest

from pathlib import Path

from lick_archive.metadata.reader import open_fits_file, read_row
from lick_archive.db.archive_schema import IngestFlags


def test_open_fits_file():
    test_data_dir = Path(__file__).parent / 'test_data'

    hdul, ingest_flags = open_fits_file(test_data_dir / '2012-01' / '18' / 'shane' / 'good_2012_01_18_r1002.fits')
    assert hdul is not None
    assert ingest_flags == IngestFlags.CLEAR
    hdul.close()

    hdul, ingest_flags = open_fits_file(test_data_dir / 'no_simple_2012_01_18_r1002.fits')
    assert hdul is not None
    assert hdul[0].header['VERSION'] == 'kastr'
    assert ingest_flags == IngestFlags.NO_FITS_SIMPLE_CARD | IngestFlags.FITS_VERIFY_ERROR
    hdul.close()

    hdul, ingest_flags = open_fits_file(test_data_dir / 'no_end_2012_01_18_b1083.fits')
    assert hdul is not None
    assert hdul[0].header['VERSION'] == 'kastb'
    assert ingest_flags == IngestFlags.NO_FITS_END_CARD | IngestFlags.FITS_VERIFY_ERROR
    hdul.close()

    hdul, ingest_flags = open_fits_file(test_data_dir / 'no_end_no_simple_2012_01_18_b1083.fits')
    assert hdul is not None
    assert hdul[0].header['VERSION'] == 'kastb'
    assert ingest_flags == IngestFlags.NO_FITS_END_CARD | IngestFlags.NO_FITS_SIMPLE_CARD  | IngestFlags.FITS_VERIFY_ERROR
    hdul.close()

    hdul, ingest_flags = open_fits_file(test_data_dir / 'verify_error_2012_01_18_r1002.fits')
    assert hdul is not None
    assert hdul[0].header['VERSION'] == 'kastr'
    assert ingest_flags == IngestFlags.FITS_VERIFY_ERROR
    hdul.close()

    hdul, ingest_flags = open_fits_file(test_data_dir / 'SC2_20190502185845.jpg')
    assert hdul is None
    assert ingest_flags & IngestFlags.UNKNOWN_FORMAT != 0

    hdul, ingest_flags = open_fits_file(test_data_dir / 'not_fits_text.txt')
    assert hdul is None
    assert ingest_flags & IngestFlags.UNKNOWN_FORMAT != 0

    with pytest.raises(FileNotFoundError):
        hdul, ingest_flags = open_fits_file(test_data_dir / 'i_do_not_exist.fits')

def test_read_row():
    test_data_dir = Path(__file__).parent / 'test_data'

    with pytest.raises(ValueError, match = 'Unknown FITS file:'):
        row = read_row(test_data_dir / 'not_from_lick_fits.fits' )

    with pytest.raises(ValueError, match = 'Unknown file format:'):
        row = read_row(test_data_dir / 'not_fits_text.txt' )
    
    with pytest.raises(FileNotFoundError):
        row = read_row(test_data_dir / 'i_do_not_exist.fits')

    row = read_row(test_data_dir / '2012-01' / '18' / 'shane' / 'good_2012_01_18_r1002.fits')
    assert row.instrument == 'Kast Red'
