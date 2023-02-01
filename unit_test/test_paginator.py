# Test the paginator used by the lick archive api

from collections import namedtuple
import os
from datetime import datetime
import urllib.parse

# Setup test Django settings
os.environ["DJANGO_SETTINGS_MODULE"] = "unit_test.django_test_settings"

import django
from django.http import QueryDict

from rest_framework.test import APIRequestFactory
from rest_framework.request import Request

from lick_archive.db.archive_schema import Base, Main, FrameType
from unit_test.utils import MockDatabase
from lick_searchable_archive.query.query_api import QueryAPIPagination,QueryAPIFilterBackend
from lick_searchable_archive.query.sqlalchemy_django_utils import SQLAlchemyQuerySet

# Test rows shared between most tests
test_rows = [ Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2019, month=6, day=1, hour=0, minute=0, second=0),
                   frame_type=FrameType.arc,     object="NA", filename="testfile1.fits",  ingest_flags='00000000000000000000000000000000'),
              Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2018, month=12, day=1, hour=0, minute=0, second=0),
                   frame_type=FrameType.science, object="object 1", filename="testfile2.fits",  ingest_flags='00000000000000000000000000000000'),                       
              Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2019, month=6, day=1, hour=0, minute=0, second=0),
                   frame_type=FrameType.science, object="object 2", filename="testfile3.fits",  ingest_flags='00000000000000000000000000000000'),                       
              Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2020, month=6, day=1, hour=0, minute=0, second=0),
                   frame_type=FrameType.science, object="object 3", filename="testfile4.fits",  ingest_flags='00000000000000000000000000000000'),                       
]

def test_no_results(tmp_path):
    # Test a query that returns no results form the database

    MockView = namedtuple("MockView", ["allowed_result_attributes", "allowed_sort_attributes", "indexed_attributes", "filter_backends"])
    mock_view = MockView(allowed_result_attributes =["filename", "obs_date", "object", "frame_type", "header"],
                         allowed_sort_attributes=   ["id", "filename", "object", "obs_date"],
                         indexed_attributes= ['filename', 'date', 'date_range', 'object'],
                         filter_backends=[QueryAPIFilterBackend])

    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()
    request = Request(request_factory.get("files/", data=QueryDict("results=filename,obs_date&sort=object")))

    with MockDatabase(Base) as mock_db:
        queryset = SQLAlchemyQuerySet(mock_db.engine, Main)
        
        paginator = QueryAPIPagination()

        page = paginator.paginate_queryset(queryset, request, mock_view)
        response = paginator.get_paginated_response(page)
        assert response.data['results'] == []

def test_one_page_of_results(tmp_path):
    # Test pulling exactly one page from the database
    MockView = namedtuple("MockView", ["allowed_result_attributes", "allowed_sort_attributes", "indexed_attributes", "filter_backends"])
    mock_view = MockView(allowed_result_attributes =["filename", "obs_date", "object", "frame_type", "header"],
                         allowed_sort_attributes=   ["id", "filename", "object", "obs_date"],
                         indexed_attributes= ['filename', 'date', 'date_range', 'object'],
                         filter_backends=[QueryAPIFilterBackend])

    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()
    request = Request(request_factory.get("files/", data=QueryDict(f"results=filename,obs_date&sort=filename&page_size={len(test_rows)}")))


    with MockDatabase(Base, test_rows) as mock_db:
        queryset = SQLAlchemyQuerySet(mock_db.engine, Main)
        
        paginator = QueryAPIPagination()
        page = paginator.paginate_queryset(queryset, request, mock_view)
        response = paginator.get_paginated_response(page)

        assert len(response.data["results"]) == len(test_rows)

        # Validate the results vs test_rows.
        for i in range(len(test_rows)):
            assert len(response.data["results"][i].keys()) == 3
            assert "id" in response.data["results"][i]
            assert response.data["results"][i]["filename"] == test_rows[i].filename
            assert response.data["results"][i]["obs_date"] == test_rows[i].obs_date

        # Validate this is the only page
        assert response.data["next"] is None 
        assert response.data["previous"] is None 

def test_multi_page_result(tmp_path):
    "Test multiple pages of results from a query."

    additional_rows = [ Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2019, month=6, day=1, hour=0, minute=0, second=0),
                             frame_type=FrameType.arc,     object="None", filename="testfile5.fits",  ingest_flags='00000000000000000000000000000000'),
                        Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2018, month=12, day=1, hour=0, minute=0, second=0),
                             frame_type=FrameType.science, object="object 5", filename="testfile6.fits",  ingest_flags='00000000000000000000000000000000'),                       
                        Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2019, month=6, day=1, hour=0, minute=0, second=0),
                             frame_type=FrameType.science, object="object 6", filename="testfile7.fits",  ingest_flags='00000000000000000000000000000000'),                       
                        Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2020, month=6, day=1, hour=0, minute=0, second=0),
                             frame_type=FrameType.science, object="object 5", filename="testfile8.fits",  ingest_flags='00000000000000000000000000000000'),                       
                        Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2020, month=6, day=1, hour=0, minute=0, second=0),
                             frame_type=FrameType.science, object="object 3", filename="testfile9.fits",  ingest_flags='00000000000000000000000000000000'),                       
                        Main(telescope="Shane", instrument="Kast Blue", obs_date = datetime(year=2020, month=6, day=1, hour=0, minute=0, second=0),
                             frame_type=FrameType.science, object="object 4", filename="testfile10.fits",  ingest_flags='00000000000000000000000000000000'),                       
    ]

    MockView = namedtuple("MockView", ["allowed_result_attributes", "allowed_sort_attributes", "indexed_attributes", "filter_backends"])
    mock_view = MockView(allowed_result_attributes =["filename", "obs_date", "object", "frame_type", "header"],
                         allowed_sort_attributes=   ["id", "filename", "object", "obs_date"],
                         indexed_attributes= ['filename', 'date', 'date_range', 'object'],
                         filter_backends=[QueryAPIFilterBackend])

    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    request_factory = APIRequestFactory()
    page_size = 3

    base_query_string = f"results=filename,object&sort=object&page_size={page_size}"
    multipage_test_rows = test_rows + additional_rows
    with MockDatabase(Base, multipage_test_rows) as mock_db:
        queryset = SQLAlchemyQuerySet(mock_db.engine, Main)
        
        paginator = QueryAPIPagination()

        # Figure out the expected # of pages. I do it this way so I can add to the test_rows
        # without having to remember to update this
        expected_pages = len(multipage_test_rows) // page_size
        if len(multipage_test_rows) % page_size != 0:
            expected_pages += 1

        # For validating that all results were returned
        returned_filenames = []

        # For validating tha results were returned in the right order
        returned_objects = []

        # Go through all the pages of results
        query_string = base_query_string
        for page_num in range(expected_pages):
            request = Request(request_factory.get("files/", data=QueryDict(query_string)))

            page = paginator.paginate_queryset(queryset, request, mock_view)
            response = paginator.get_paginated_response(page)            

            for i in range(len(response.data["results"])):
                assert len(response.data["results"][i].keys()) == 3
                assert "id" in response.data["results"][i]
                returned_filenames.append(response.data["results"][i]["filename"])
                returned_objects.append(response.data["results"][i]["object"])

            if page_num == 0:
                assert response.data["previous"] is None
            if page_num != expected_pages-1:
                assert response.data["next"] is not None
                parsed_url = urllib.parse.urlparse(response.data["next"])
                query_string = parsed_url.query
            else:
                assert response.data["next"] is None

        # Assert all rows returned
        for row in multipage_test_rows:
            assert row.filename in returned_filenames

        # Assert rows returned in correct order
        for i in range(1, len(multipage_test_rows)):
            if returned_objects[i] is None or returned_objects[i-1] is None:
                # SQL doesn't specify how NULL/None should be sorted, so ignore those rows
                continue
            assert returned_objects[i] >= returned_objects[i-1]

        # Now test in reverse order
        returned_filenames = []
        returned_objects = []
        for page_num in range(expected_pages):
            request = Request(request_factory.get("files/", data=QueryDict(query_string)))

            page = paginator.paginate_queryset(queryset, request, mock_view)
            response = paginator.get_paginated_response(page)            

            for i in range(len(response.data["results"])):
                assert len(response.data["results"][i].keys()) == 3
                assert "id" in response.data["results"][i]
                returned_filenames.append(response.data["results"][i]["filename"])
                
            for i in reversed(range(len(response.data["results"]))):
                # Insert objects in reverse order to preserve order
                returned_objects.insert(0, response.data["results"][i]["object"])

            if page_num == 0:
                assert response.data["next"] is None
            # Use the previous link this time to iterate through the pages
            if page_num != expected_pages-1:
                assert response.data["previous"] is not None
                parsed_url = urllib.parse.urlparse(response.data["previous"])
                query_string = parsed_url.query
            else:
                assert response.data["previous"] is None

        # Assert all rows returned
        for row in multipage_test_rows:
            assert row.filename in returned_filenames

        # Assert rows returned in correct order
        for i in range(1, len(multipage_test_rows)):
            if returned_objects[i] is None or returned_objects[i-1] is None:
                # SQL doesn't specify how NULL/None should be sorted, so ignore those rows
                continue
            assert returned_objects[i] >= returned_objects[i-1]

