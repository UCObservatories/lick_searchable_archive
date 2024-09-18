import pytest
import os
from collections import namedtuple
import datetime
import copy

from lick_archive.client.lick_archive_client import LickArchiveClient
from ext_test_common import PUBLIC_FILE,TEST_USER, PRIVATE_FILE, TEST_INSTR

expected_metadata = {"telescope":  "Shane",
                     "instrument": "Kast Red",
                     "obs_date":   "2019-05-24T12:00:01.490000Z", # datetime.datetime(year=2019,month=5,day=24,hour=4,minute=0,second=1,microsecond=490000,tzinfo=datetime.timezone.utc),
                     "exptime":   40.0,
                     "ra":         "21:51:11.01",
                     "dec":        "28:51:50.3",
                     "object":     "BD+28 4211",
                     "program":    "NEWCAM",
                     "coversheet": "RECUR_S101",
                     "file_size":  3355200}


def test_query_public(archive_backend):

    from lick_archive.config.archive_config import ArchiveConfigFile
    lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

    client = LickArchiveClient(archive_backend, 1, 30, 5)

    # Make sure we are not logged in at first
    assert client.get_login_status() is True
    assert client.logged_in_user is None

    # Query for the known public file
    # first a count query
    result_count = client.query(field="filename", value=PUBLIC_FILE, count=True)[0]
    assert result_count == 1

    # Then a result query
    results = client.query(field="filename", value=PUBLIC_FILE, results=["filename", "telescope", "instrument", "obs_date", "exptime", "ra", "dec", "object", "program", "coversheet", "file_size"])

    result_count = results[0]
    rows = results[1]
    prev_page = results[2]
    next_page = results[3]
    
    assert result_count == 1
    assert len(rows) == 1
    assert prev_page is None
    assert next_page is None

    result = rows[0]  # First (and only) row 

    expected_results = copy.copy(expected_metadata)
    expected_results["filename"] = PUBLIC_FILE

    for key in expected_results:
        assert key in result, f"{key} not found in query results"
        assert result[key] == expected_results[key], f"Exepcted results for {key}: '{expected_results[key]}' != actual results '{result[key]}'"

def test_query_private(archive_backend,test_user_password_env):

    from lick_archive.config.archive_config import ArchiveConfigFile
    lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

    client = LickArchiveClient(archive_backend, 1, 30, 5)

    # Login as test user
    assert client.login(TEST_USER,os.environ[test_user_password_env]) is True
    assert client.logged_in_user == TEST_USER


    # Query for the known public file
    results = client.query(field="filename", value=PUBLIC_FILE, results=["filename", "telescope", "instrument", "obs_date", "exptime", "ra", "dec", "object", "program", "coversheet", "file_size"])

    result_count = results[0]
    rows = results[1]
    prev_page = results[2]
    next_page = results[3]
    
    assert result_count == 1
    assert len(rows) == 1
    assert prev_page is None
    assert next_page is None

    result = rows[0]  # First (and only) row 

    expected_results = copy.copy(expected_metadata)
    expected_results["filename"] = PUBLIC_FILE

    for key in expected_results:
        assert key in result, f"{key} not found in query results"
        assert result[key] == expected_results[key], f"Exepcted results for {key}: '{expected_results[key]}' != actual results '{result[key]}'"

    # Find private file. We'll search in adjacent days in case of date roll over
    ingest_dates = [datetime.date.today() - datetime.timedelta(days=1), datetime.date.today()]

    found=False
    for ingest_date  in ingest_dates:
        expected_filename = ingest_date.strftime("%Y-%m/%d/")  + TEST_INSTR + "/" + PRIVATE_FILE
        results = client.query(field="filename", value=expected_filename,count=True)
        if results[0] == 1:
            found=True
            break

    assert found is True, "Could not find private file."

    results = client.query(field="filename", value=expected_filename, results=["filename", "telescope", "instrument", "obs_date", "exptime", "ra", "dec", "object", "program", "coversheet", "file_size"])

    result_count = results[0]
    rows = results[1]
    prev_page = results[2]
    next_page = results[3]
    
    assert result_count == 1
    assert len(rows) == 1
    assert prev_page is None
    assert next_page is None

    result = rows[0]  # First (and only) row 

    expected_results = copy.copy(expected_metadata)
    expected_results["filename"] = expected_filename

    for key in expected_results:
        assert key in result, f"{key} not found in query results"
        assert result[key] == expected_results[key], f"Exepcted results for {key}: '{expected_results[key]}' != actual results '{result[key]}'"

    # Now log out, and verify the file can't be seen publically
    assert client.logout() is True
    assert client.logged_in_user is None
    assert client.get_login_status() is True
    assert client.logged_in_user is None

    results = client.query(field="filename", value=expected_filename,count=True)
    assert results[0] == 0

    results = client.query(field="filename", value=expected_filename, results=["filename", "telescope", "instrument", "obs_date", "exptime", "ra", "dec", "object", "program", "coversheet", "file_size"])

    result_count = results[0]
    rows = results[1]

    assert result_count == 0
    assert len(rows) == 0