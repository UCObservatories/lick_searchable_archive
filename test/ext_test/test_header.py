import pytest

import os
import datetime

from astropy.io.fits import Header

from lick_archive.client.lick_archive_client import LickArchiveClient, QueryTerm
from ext_test_common import PUBLIC_FILE,TEST_USER, PRIVATE_FILE, replace_parsed_url_hostname

from requests import HTTPError

expected_metadata = {"PROGRAM":  "Shane",
                     "EXPTIME":   40.0,
                     "RA":         "21:51:11.01",
                     "DEC":        "28:51:50.3",
                     "OBJECT":     "BD+28 4211",
                     "PROGRAM":    "NEWCAM",
                     "OBSTYPE":    "OBJECT",
                     "DATE-OBS":   "2019-05-24T12:00:01.49",
                    }

expected_private_metadata = {"TRUITIME":   1.45479,
                             "RA":         "06:11:36.61",
                             "DEC":        "48:42:40.2",
                             "OBJECT":     "AurA",
                             "PROGRAM":    "2024B_S026i0",
                             "DATE-BEG":   "2025-01-28T03:46:54.079",
                             }

def test_header_public(archive_host, archive_config, ssl_ca_bundle):

    archive_frontend = replace_parsed_url_hostname(archive_config.host.frontend_url.parsed_url, archive_host)

    client = LickArchiveClient(archive_frontend, 1, 30, 5, ssl_verify=ssl_ca_bundle)

    # Login is done through the backend API, but we want to test the external api, so switch the URL
    client.archive_url = archive_frontend

    # Get the header for the publically available file
    header_str = client.header(PUBLIC_FILE)

    header = Header.fromstring(header_str,sep='\n')


    for key in expected_metadata:
        assert key in header, f"{key} not found in query results"
        assert header[key] == expected_metadata[key], f"Exepcted results for {key}: '{expected_metadata[key]}' != actual results '{header[key]}'"

def test_header_private(archive_host, archive_config, ssl_ca_bundle, test_user_password_env):

    archive_frontend = replace_parsed_url_hostname(archive_config.host.frontend_url.parsed_url, archive_host)


    client = LickArchiveClient(archive_frontend, 1, 30, 5, ssl_verify=ssl_ca_bundle, username=TEST_USER, password=os.environ[test_user_password_env])


    # Make sure public file's header can still be seen when logged in
    # Get the header for the publically available file
    header_str = client.header(PUBLIC_FILE)

    header = Header.fromstring(header_str,sep='\n')


    for key in expected_metadata:
        assert key in header, f"{key} not found in query results"
        assert header[key] == expected_metadata[key], f"Exepcted results for {key}: '{expected_metadata[key]}' != actual results '{header[key]}'"


    # Get the header for the privately available file
    header_str = client.header(PRIVATE_FILE)

    header = Header.fromstring(header_str,sep='\n')


    for key in expected_private_metadata:
        assert key in header, f"{key} not found in query results"
        assert header[key] == expected_private_metadata[key], f"Exepcted results for {key}: '{expected_private_metadata[key]}' != actual results '{header[key]}'"


    # Clear client credentials to verify the private file is not visible publically
    client.set_auth_credentials(None,None)

    # Get the header for the privately available file
    with pytest.raises(HTTPError):
        header_str = client.header(PRIVATE_FILE)
