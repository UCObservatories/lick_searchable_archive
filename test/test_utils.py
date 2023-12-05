import contextlib
import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import django

# Setup test Django settings
os.environ["DJANGO_SETTINGS_MODULE"] = "django_test_settings"

# Force archive config to load the test version rather than the default config
from lick_archive.archive_config import ArchiveConfigFile
ArchiveConfigFile.from_file(Path(__file__).parent / "archive_test_config.ini")

from lick_archive.db.archive_schema import Main
from lick_archive.data_dictionary import api_capabilities
from lick_searchable_archive.query.query_api import QueryAPIView
from lick_searchable_archive.query.sqlalchemy_django_utils import SQLAlchemyQuerySet, SQLAlchemyORMSerializer


class MockDatabase(contextlib.AbstractContextManager):

    def __init__(self, base_class, rows=None):

        self.base_class = base_class

        # Create an in memory engine
        self.engine = create_engine('sqlite://')

        # Create the schema
        self.base_class.metadata.create_all(self.engine)

        if rows is not None:
            # Session for inserting rows
            self.Session = sessionmaker(bind=self.engine)
            session = self.Session()

            session.bulk_save_objects(rows)
            session.commit()
            session.close()


    def __exit__(self, exc_type, exc_value, traceback):
        self.base_class.metadata.drop_all(self.engine)
        return False

class MockView(QueryAPIView):
    """A test view for testing the query api"""
    allowed_sort_attributes = ["id", "filename", "object", "obs_date"]
    allowed_result_attributes = ["filename", "obs_date", "object", "frame_type", "header"]
    required_attributes = list(api_capabilities['required']['db_name'])
    serializer_class = SQLAlchemyORMSerializer

    def __init__(self, engine, request=None):
        self.engine=engine
        self.request=request
        self.format_kwarg = "json"

    def get_queryset(self):
        return SQLAlchemyQuerySet(self.engine, Main)


# Setup django environment
def setup_django_environment(test_path):
    os.environ["UNIT_TEST_DIR"] = str(test_path)

    django.setup()



# Helper to create a request for testing
def create_test_request(path, data):
    from rest_framework.test import APIRequestFactory
    from rest_framework.request import Request

    request_factory = APIRequestFactory()
    request = Request(request_factory.get(path, data=data))

    return request

# Helper to validate a test request to build the needed "validated_query" request attribute
def create_validated_request(path, data, view):
    request = create_test_request(path, data)

    from lick_searchable_archive.query.query_api import QuerySerializer

    serializer = QuerySerializer(data=request.query_params, view=view)
    try:
        serializer.is_valid(raise_exception=True)
    except Exception as e:
        raise

    # Store the validated results in the request to be passed to paginators and filters
    request.validated_query = serializer.validated_data
    return request
