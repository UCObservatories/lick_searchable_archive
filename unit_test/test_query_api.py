# Test the archive API

import pytest
from collections import namedtuple
import os
from datetime import datetime, date

# Setup test Django settings
os.environ["DJANGO_SETTINGS_MODULE"] = "unit_test.django_test_settings"

import django
from django.http import QueryDict

from rest_framework.test import APIRequestFactory
from rest_framework.request import Request
from rest_framework.serializers import ValidationError
from rest_framework.exceptions import APIException

from lick_archive.db.archive_schema import Base, Main, FrameType
from unit_test.utils import MockDatabase
from lick_searchable_archive.query.query_api import QueryAPIView, QueryAPIPagination,QueryAPIFilterBackend
from lick_searchable_archive.query.sqlalchemy_django_utils import SQLAlchemyQuerySet, SQLAlchemyORMSerializer

# Test rows shared between most tests
test_rows = [ Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2019, month=6, day=1, hour=0, minute=0, second=0),
                   frame_type=FrameType.arc,     object=None, filename="/data/testfile1.fits",  ingest_flags='00000000000000000000000000000000'),
              Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2018, month=12, day=1, hour=0, minute=0, second=0),
                   frame_type=FrameType.science, object="object 1", filename="/data/testfile2.fits",  ingest_flags='00000000000000000000000000000000'),                       
              Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2019, month=6, day=1, hour=0, minute=0, second=0),
                   frame_type=FrameType.science, object="object 2", filename="/data/testfile3.fits",  ingest_flags='00000000000000000000000000000000'),                       
              Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2020, month=6, day=1, hour=0, minute=0, second=0),
                   frame_type=FrameType.science, object="object 2", filename="/data/testfile4.fits",  ingest_flags='00000000000000000000000000000000'),                       
]

class MockView(QueryAPIView):
    """A test view for testing FilesAPIView"""
    allowed_sort_attributes = ["id", "filename", "object", "obs_date"]
    allowed_result_attributes = ["filename", "obs_date", "object", "frame_type", "header"]
    indexed_attributes = ['filename', 'date', 'date_range', 'object']
    serializer_class = SQLAlchemyORMSerializer
    def __init__(self, engine, request):
        self.engine=engine
        self.request=request
        self.format_kwarg = "json"

    def get_queryset(self):
        return SQLAlchemyQuerySet(self.engine, Main)



def test_no_filters(tmp_path):
    """Test a query with no filters, which should fail"""

    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()
    request = Request(request_factory.get("files/", data=QueryDict("results=filename")))

    with MockDatabase(Base) as mock_db:
        view = MockView(mock_db.engine, request)
        
        with pytest.raises(APIException, match="At least one required field must be included in the query."):
            view.list(request)


def test_filename_filter(tmp_path):
    """Test filtering on filename"""
    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()

    request = Request(request_factory.get("files/", data=QueryDict("filename=testfile1.fits&results=filename")))

    with MockDatabase(Base, test_rows) as mock_db:
        view = MockView(mock_db.engine, request)
        response = view.list(request)

        assert len(response.data["results"]) == 1

        assert len(response.data["results"][0].keys()) == 2
        assert "id" in response.data["results"][0]
        # Note the view filters out the full path stored in the db
        assert response.data["results"][0]["filename"]  == "testfile1.fits"

def test_object_filter(tmp_path):
    """Test an exact object filter"""
    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()

    request = Request(request_factory.get("files/", data=QueryDict("object=object 2&results=filename,object&sort=filename")))

    with MockDatabase(Base, test_rows) as mock_db:
        view = MockView(mock_db.engine, request)
        response = view.list(request)

        assert len(response.data["results"]) == 2

        for i in range(2,len(test_rows)):
            assert len(response.data["results"][i-2].keys()) == 3
            assert "id" in response.data["results"][i-2]
            assert response.data["results"][i-2]["filename"]  == os.path.basename(test_rows[i].filename)
            assert response.data["results"][i-2]["object"]  == test_rows[i].object

def test_prefix_filter(tmp_path):
    """Test filtering with a string prefix"""
    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()

    request = Request(request_factory.get("files/", data=QueryDict("object=object&prefix=t&results=filename,object&sort=filename")))

    with MockDatabase(Base, test_rows) as mock_db:
        view = MockView(mock_db.engine, request)
        response = view.list(request)

        assert len(response.data["results"]) == 3

        for i in range(1,len(test_rows)):
            assert len(response.data["results"][i-1].keys()) == 3
            assert "id" in response.data["results"][i-1]
            assert response.data["results"][i-1]["filename"]  == os.path.basename(test_rows[i].filename)
            assert response.data["results"][i-1]["object"]  == test_rows[i].object


def test_date_filter(tmp_path):
    """Test a date filter"""
    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()

    request = Request(request_factory.get("files/", data=QueryDict("date=2018-12-1&results=filename,obs_date")))

    with MockDatabase(Base, test_rows) as mock_db:
        view = MockView(mock_db.engine, request)
        response = view.list(request)

        assert len(response.data["results"]) == 1

        assert len(response.data["results"][0].keys()) == 3
        assert "id" in response.data["results"][0]
        assert response.data["results"][0]["filename"]  == "testfile2.fits"
        assert response.data["results"][0]["obs_date"]  == datetime(year=2018, month = 12, day = 1)

        assert "id" in response.data["results"][0]
        assert response.data["results"][0]["filename"]  == "testfile2.fits"
        assert response.data["results"][0]["obs_date"]  == datetime(year=2018, month = 12, day = 1)



def test_date_range_filter(tmp_path):
    """Test a date range filter"""
    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()

    request = Request(request_factory.get("files/", data=QueryDict("date_range=2018-12-31,2020-01-01&results=filename,obs_date&sort=filename")))

    with MockDatabase(Base, test_rows) as mock_db:
        view = MockView(mock_db.engine, request)
        response = view.list(request)

        assert len(response.data["results"]) == 2

        assert len(response.data["results"][0].keys()) == 3
        assert "id" in response.data["results"][0]
        assert response.data["results"][0]["filename"]  == "testfile1.fits"
        assert response.data["results"][0]["obs_date"]  == datetime(year=2019, month = 6, day = 1)

        assert len(response.data["results"][1].keys()) == 3
        assert "id" in response.data["results"][1]
        assert response.data["results"][1]["filename"]  == "testfile3.fits"
        assert response.data["results"][1]["obs_date"]  == datetime(year=2019, month = 6, day = 1)

def test_reverse_date_range_filter(tmp_path):
    """Test that a reversed date range is handled correctly"""
    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()

    request = Request(request_factory.get("files/", data=QueryDict("date_range=2020-01-01,2018-12-31&results=filename,obs_date&sort=filename")))

    with MockDatabase(Base, test_rows) as mock_db:
        view = MockView(mock_db.engine, request)
        response = view.list(request)

        assert len(response.data["results"]) == 2

        assert len(response.data["results"][0].keys()) == 3
        assert "id" in response.data["results"][0]
        assert response.data["results"][0]["filename"]  == "testfile1.fits"
        assert response.data["results"][0]["obs_date"]  == datetime(year=2019, month = 6, day = 1)

        assert len(response.data["results"][1].keys()) == 3
        assert "id" in response.data["results"][1]
        assert response.data["results"][1]["filename"]  == "testfile3.fits"
        assert response.data["results"][1]["obs_date"]  == datetime(year=2019, month = 6, day = 1)

def test_no_sort_attributes(tmp_path):
    """ Test a query with no specified sort attributes. The results should be sorted by id"""

    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()
    request = Request(request_factory.get("files/", data=QueryDict("filename=testfile&prefix=t&results=filename,obs_date")))

    with MockDatabase(Base, test_rows) as mock_db:
        view = MockView(mock_db.engine, request)
        response = view.list(request)

        assert len(response.data["results"]) == len(test_rows)

        # In this test we don't care about the specific order, only that the ids are in order
        for result in response.data["results"]:

            assert len(result.keys()) == 3
            assert "id" in result
        
        assert response.data["results"][0]["id"] < response.data["results"][1]["id"] < response.data["results"][2]["id"] < response.data["results"][3]["id"]

def test_no_result_attributes(tmp_path):
    """ Test a query with no specified result attributes. This should return all allowed result attributes (and id)
    This also tests the header field post-processing
    """

    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()
    request = Request(request_factory.get("files/", data=QueryDict("filename=testfile&prefix=t&sort=filename")))

    with MockDatabase(Base, test_rows) as mock_db:
        view = MockView(mock_db.engine, request)
        response = view.list(request)

        assert len(response.data["results"]) == len(test_rows)

        for i in range(len(test_rows)):
            assert "id" in response.data["results"][i]
            assert response.data["results"][i]["filename"]   == os.path.basename(test_rows[i].filename)
            assert response.data["results"][i]["obs_date"]   == test_rows[i].obs_date
            if "object" not in response.data["results"][i]:
                # One row has a NULL object that won't show up in the results
                assert response.data["results"][i]["filename"] == "testfile1.fits"
            else:
                assert response.data["results"][i]["object"]     == test_rows[i].object
            # The frame type is converted from a python enum to a string
            assert response.data["results"][i]["frame_type"] == test_rows[i].frame_type.name

            # Post-processing by the view should turn the header into a URL
            assert response.data["results"][i]["header"]     == "http://testserver/files/{}/header".format(os.path.basename(test_rows[i].filename))


def test_count(tmp_path):
    """Test a count query """
    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()
    request = Request(request_factory.get("files/", data=QueryDict("date=2019-06-01&count=t")))

    with MockDatabase(Base, test_rows) as mock_db:
        view = MockView(mock_db.engine, request)
        response = view.list(request)
        assert response.data['count'] == 2

def test_invalid_query(tmp_path):
    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()

    request = Request(request_factory.get("files/", data=QueryDict("filename=file.fits&results=invalid_field")))
    with MockDatabase(Base, test_rows) as mock_db:
        view = MockView(mock_db.engine, request)

        with pytest.raises(ValidationError):
            view.list(request)