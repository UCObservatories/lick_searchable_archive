import pytest

import os
import datetime
from pathlib import Path
import hashlib

from astropy.io.fits import Header

from lick_archive.client.lick_archive_client import LickArchiveClient
from ext_test_common import PUBLIC_FILE,TEST_USER, PRIVATE_FILE, TEST_INSTR, replace_parsed_url_hostname

from requests import HTTPError

expected_size = 3355200
expected_hash = 'c47fa3d9d85000a609092bf33583eb5260a4dc04937f9dfa51f9e4324e9c69d4'

def get_hash_of_file(file):
    with open(file, "rb") as f:
        b = f.read()
    hash = hashlib.sha256(b,usedforsecurity=False)
    return hash.hexdigest()

def test_download_public(archive_host, archive_config, ssl_ca_bundle, tmp_path):

    archive_backend = replace_parsed_url_hostname(archive_config.host.api_url.parsed_url, archive_host)
    archive_frontend = replace_parsed_url_hostname(archive_config.host.frontend_url.parsed_url, archive_host)


    client = LickArchiveClient(archive_backend, 1, 30, 5, ssl_verify=ssl_ca_bundle)

    # Make sure we are not logged in at first
    assert client.get_login_status() is True
    assert client.logged_in_user is None

    destination_path = tmp_path / Path(PUBLIC_FILE).name

    # Donwload only works via the frontend URL, because of the way we use X-SendFile.
    # But we prefer login using the backend, so we now switch the client to use the
    # frontend for the actual download but switch it back to the backend url 
    # when logging in/out. Eventually, the backend login/logout API should also be 
    # made available externally so that this entire test could just use the 
    # frontend_url.
    client.archive_url = archive_frontend

    # Download the publically available file
    success = client.download(PUBLIC_FILE, destination_path)

    assert success == True, f"Client failed to download {PUBLIC_FILE} to {destination_path}"
    assert destination_path.is_file(), f"Destination file {destination_path} does not exist"
    st_info = destination_path.stat()
    assert st_info.st_size == expected_size, f"Destination file size {st_info.st_size} does not match expected size {expected_size}"

    result_hash = get_hash_of_file(destination_path)
    assert result_hash == expected_hash, f"Destination file contents do not match expected sha256 hash."

def test_download_private(archive_host, archive_config, ssl_ca_bundle, test_user_password_env, tmp_path):

    archive_backend = replace_parsed_url_hostname(archive_config.host.api_url.parsed_url, archive_host)
    archive_frontend = replace_parsed_url_hostname(archive_config.host.frontend_url.parsed_url, archive_host)


    client = LickArchiveClient(archive_backend, 1, 30, 5, ssl_verify=ssl_ca_bundle)

    # Login as test user
    assert client.login(TEST_USER,os.environ[test_user_password_env]) is True
    assert client.logged_in_user == TEST_USER

    destination_path = tmp_path / Path(PUBLIC_FILE).name

    # Donwload only works via the frontend URL, because of the way we use X-SendFile.
    # But we prefer login using the backend, so we now switch the client to use the
    # frontend for the actual download but switch it back to the backend url 
    # when logging in/out. Eventually, the backend login/logout API should also be 
    # made available externally so that this entire test could just use the 
    # frontend url.
    client.archive_url = archive_frontend

    # Make sure the logged in user can download a publically available file
    success = client.download(PUBLIC_FILE, destination_path)

    assert success == True, f"Client failed to download {PUBLIC_FILE} to {destination_path}"
    assert destination_path.is_file(), f"Destination file {destination_path} does not exist"
    st_info = destination_path.stat()
    assert st_info.st_size == expected_size, f"Destination file size {st_info.st_size} does not match expected size {expected_size}"

    result_hash = get_hash_of_file(destination_path)
    assert result_hash == expected_hash, f"Destination file contents do not match expected sha256 hash."

    # Switch url back for now
    client.archive_url = archive_backend

    # Find private file. We'll search in adjacent days in case of date roll over    
    ingest_dates = [datetime.date.today() - datetime.timedelta(days=1), datetime.date.today(), datetime.date.today() + datetime.timedelta(days=1)]

    found=False
    for ingest_date  in ingest_dates:
        expected_filename = ingest_date.strftime("%Y-%m/%d/")  + TEST_INSTR + "/" + PRIVATE_FILE
        results = client.query(field="filename", value=expected_filename,count=True)
        if results[0] == 1:
            found=True
            break

    assert found is True, "Could not find private file."


    # Download and validate the private file
    destination_path = tmp_path / PRIVATE_FILE

    client.archive_url = archive_frontend

    success = client.download(expected_filename, destination_path)

    assert success == True, f"Client failed to download {expected_filename} to {destination_path}"
    assert destination_path.is_file(), f"Destination file {destination_path} does not exist"
    st_info = destination_path.stat()
    assert st_info.st_size == expected_size, f"Destination file size {st_info.st_size} does not match expected size {expected_size}"

    result_hash = get_hash_of_file(destination_path)
    assert result_hash == expected_hash, f"Destination file contents do not match expected sha256 hash."

    client.archive_url = archive_backend

    # Now log out, and verify the file can't be seen publically
    assert client.logout() is True
    assert client.logged_in_user is None
    assert client.get_login_status() is True
    assert client.logged_in_user is None

    client.archive_url = archive_frontend
    with pytest.raises(HTTPError):
        result = client.download(expected_filename, destination_path)
