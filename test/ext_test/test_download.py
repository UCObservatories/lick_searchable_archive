import pytest

import os
import datetime
from pathlib import Path
import hashlib

from astropy.io.fits import Header

from lick_archive.client.lick_archive_client import LickArchiveClient
from ext_test_common import PUBLIC_FILE,TEST_USER, PRIVATE_FILE, replace_parsed_url_hostname

from requests import HTTPError

expected_public_size = 3355200
expected_public_hash = 'c47fa3d9d85000a609092bf33583eb5260a4dc04937f9dfa51f9e4324e9c69d4'

expected_private_size = 16804800
expected_private_hash = '0027681b6dce289cb404cc706d2ef688d72367baf2451e024246d88dc022d9f3'

def get_hash_of_file(file):
    with open(file, "rb") as f:
        b = f.read()
    hash = hashlib.sha256(b,usedforsecurity=False)
    return hash.hexdigest()

def test_download_public(archive_host, archive_config, ssl_ca_bundle, tmp_path):

    archive_frontend = replace_parsed_url_hostname(archive_config.host.frontend_url.parsed_url, archive_host)


    client = LickArchiveClient(archive_frontend, 1, 30, 5, ssl_verify=ssl_ca_bundle)

    destination_path = tmp_path / Path(PUBLIC_FILE).name

    # Download the publically available file
    success = client.download(PUBLIC_FILE, destination_path)

    assert success == True, f"Client failed to download {PUBLIC_FILE} to {destination_path}"
    assert destination_path.is_file(), f"Destination file {destination_path} does not exist"
    st_info = destination_path.stat()
    assert st_info.st_size == expected_public_size, f"Destination file size {st_info.st_size} does not match expected size {expected_size}"

    result_hash = get_hash_of_file(destination_path)
    assert result_hash == expected_public_hash, f"Destination file contents do not match expected sha256 hash."

def test_download_private(archive_host, archive_config, ssl_ca_bundle, test_user_password_env, tmp_path):

    archive_frontend = replace_parsed_url_hostname(archive_config.host.frontend_url.parsed_url, archive_host)


    client = LickArchiveClient(archive_frontend, 1, 30, 5, ssl_verify=ssl_ca_bundle, username=TEST_USER, password=os.environ[test_user_password_env])

    destination_path = tmp_path / Path(PUBLIC_FILE).name

    # Make sure the logged in user can download a publically available file
    success = client.download(PUBLIC_FILE, destination_path)

    assert success == True, f"Client failed to download {PUBLIC_FILE} to {destination_path}"
    assert destination_path.is_file(), f"Destination file {destination_path} does not exist"
    st_info = destination_path.stat()
    assert st_info.st_size == expected_public_size, f"Destination file size {st_info.st_size} does not match expected size {expected_public_size}"

    result_hash = get_hash_of_file(destination_path)
    assert result_hash == expected_public_hash, f"Destination file contents do not match expected sha256 hash."


    # Download and validate the private file
    destination_path = tmp_path / Path(PRIVATE_FILE).name

    client.archive_url = archive_frontend

    success = client.download(PRIVATE_FILE, destination_path)

    assert success == True, f"Client failed to download {PRIVATE_FILE} to {destination_path}"
    assert destination_path.is_file(), f"Destination file {destination_path} does not exist"
    st_info = destination_path.stat()
    assert st_info.st_size == expected_private_size, f"Destination file size {st_info.st_size} does not match expected size {expected_private_size}"

    result_hash = get_hash_of_file(destination_path)
    assert result_hash == expected_private_hash, f"Destination file contents do not match expected sha256 hash."


    # clear credentials, and verify the file can't be seen publically
    client.set_auth_credentials(None, None)

    with pytest.raises(HTTPError):
        result = client.download(PRIVATE_FILE, destination_path)
