import pytest
from collections import namedtuple
import os
from datetime import date
from urllib.parse import quote
# Setup test Django settings
os.environ["DJANGO_SETTINGS_MODULE"] = "django_test_settings"

import django
from django.http import QueryDict
from rest_framework.serializers import ValidationError

from lick_searchable_archive.query.query_api import QuerySerializer

def test_query_serializer(tmp_path):

    MockView = namedtuple("MockView", ["allowed_result_attributes", "allowed_sort_attributes"])
    mock_view = MockView(allowed_result_attributes =["filename", "obs_date", "object", "frame_type", "header"],
                         allowed_sort_attributes=   ["id", "filename", "object", "obs_date"])

    # Setup django environment
    os.environ["UNIT_TEST_DIR"] = str(tmp_path)
    django.setup()

    # filename query
    query_params = QueryDict("filename=afile.fits&results=filename,obs_date,frame_type,object")

    serializer = QuerySerializer(data=query_params, view=mock_view)
    assert serializer.is_valid(raise_exception=True) is True
    assert serializer.validated_data['filename'] == "afile.fits"
    assert serializer.validated_data['results'] == ['filename','obs_date','frame_type','object']
    assert serializer.validated_data['sort'] == ["id"]
    assert serializer.validated_data['count'] is False
    assert serializer.validated_data['prefix'] is False
    assert "date" not in serializer.validated_data
    assert "object" not in serializer.validated_data
    assert "ra_dec" not in serializer.validated_data
    assert "filters" not in serializer.validated_data

    # Date query
    query_params = QueryDict("date=1970-01-01")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    assert serializer.is_valid(raise_exception=True) is True
    assert serializer.validated_data['date'] == [date(year=1970, month=1, day=1)]
    assert serializer.validated_data['results'] == []
    assert serializer.validated_data['sort'] == ["id"]
    assert serializer.validated_data['count'] is False
    assert serializer.validated_data['prefix'] is False
    assert "filename" not in serializer.validated_data
    assert "object" not in serializer.validated_data
    assert "ra_dec" not in serializer.validated_data
    assert "filters" not in serializer.validated_data

    # Date range with count, filter, and sort
    query_params = QueryDict("date=1970-01-01,2023-01-01&filters=instrument,SHARCS,KAST_RED&count=t&sort=filename")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    assert serializer.is_valid(raise_exception=True) is True
    assert serializer.validated_data['date'] == [date(year=1970, month=1, day=1),date(year=2023, month=1, day=1)]
    assert serializer.validated_data['results'] == []
    assert serializer.validated_data['sort'] == ["filename"]
    assert serializer.validated_data['count'] is True
    assert serializer.validated_data['prefix'] is False
    assert "ShaneAO/ShARCS" in serializer.validated_data['filters']
    assert "Kast Red" in serializer.validated_data['filters']
    assert len(serializer.validated_data['filters']) == 2
    assert "filename" not in serializer.validated_data
    assert "object" not in serializer.validated_data
    assert "ra_dec" not in serializer.validated_data

    # Object query with prefix and sort
    query_params = QueryDict("object=HD3&prefix=t&sort=object,-obs_date")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    assert serializer.is_valid(raise_exception=True) is True
    assert serializer.validated_data['object'] == "HD3"
    assert serializer.validated_data['results'] == []
    assert serializer.validated_data['sort'] == ["object","-obs_date"]
    assert serializer.validated_data['count'] is False
    assert serializer.validated_data['prefix'] is True
    assert "filename" not in serializer.validated_data
    assert "date" not in serializer.validated_data
    assert "ra_dec" not in serializer.validated_data
    assert "filters" not in serializer.validated_data

    # ra_dec query with no radius, + on sort. Note + must be quoted
    query_params = QueryDict("ra_dec=349.99,-5.1656&sort=" + quote("+obs_date"))
    serializer = QuerySerializer(data=query_params, view=mock_view)
    assert serializer.is_valid(raise_exception=True) is True
    assert serializer.validated_data['ra_dec'] == [349.99, -5.1656]
    assert serializer.validated_data['results'] == []
    assert serializer.validated_data['sort'] == ["+obs_date"]
    assert serializer.validated_data['count'] is False
    assert serializer.validated_data['prefix'] is False
    assert "filename" not in serializer.validated_data
    assert "date" not in serializer.validated_data
    assert "object" not in serializer.validated_data
    assert "filters" not in serializer.validated_data

    # ra_dec query with radius
    query_params = QueryDict("ra_dec=349.99,-5.1656,0.1&sort=obs_date")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    assert serializer.is_valid(raise_exception=True) is True
    assert serializer.validated_data['ra_dec'] == [349.99, -5.1656,0.1]
    assert serializer.validated_data['results'] == []
    assert serializer.validated_data['sort'] == ["obs_date"]
    assert serializer.validated_data['count'] is False
    assert serializer.validated_data['prefix'] is False
    assert "filename" not in serializer.validated_data
    assert "date" not in serializer.validated_data
    assert "object" not in serializer.validated_data
    assert "filters" not in serializer.validated_data

    #Everything empty    
    query_params = QueryDict("")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    assert serializer.is_valid(raise_exception=True) is True
    assert "filename" not in serializer.validated_data
    assert "date" not in serializer.validated_data
    assert "object" not in serializer.validated_data
    assert "ra_dec" not in serializer.validated_data
    assert "filters" not in serializer.validated_data
    assert serializer.validated_data['results'] == []
    assert serializer.validated_data['sort'] == ["id"]
    assert serializer.validated_data['count'] is False
    assert serializer.validated_data['prefix'] is False

    # Invalid dates
    query_params = QueryDict("date=1970-13-56")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    with pytest.raises(ValidationError, match="Date has wrong format"):
        serializer.is_valid(raise_exception=True)

    query_params = QueryDict("date=01/01/1970,01/01/2023")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    with pytest.raises(ValidationError, match="Date has wrong format"):
        serializer.is_valid(raise_exception=True)

    # Invalid ra_dec
    query_params = QueryDict("ra_dec=100,-91")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    with pytest.raises(ValidationError, match="DEC must be between"):
        serializer.is_valid(raise_exception=True)

    query_params = QueryDict("ra_dec=100,91")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    with pytest.raises(ValidationError, match="DEC must be between"):
        serializer.is_valid(raise_exception=True)


    # Invalid results (fails regex)
    query_params = QueryDict("results=99,38")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    with pytest.raises(ValidationError, match="This value does not match the required pattern."):
        serializer.is_valid(raise_exception=True)

    # Invalid result (not in allowed list)
    query_params = QueryDict("results=filename,coord")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    with pytest.raises(ValidationError, match="coord is not a valid result field."):
        serializer.is_valid(raise_exception=True)

    # Invalid sort (fails regex)
    query_params = QueryDict("sort=99")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    with pytest.raises(ValidationError, match="This value does not match the required pattern."):
        serializer.is_valid(raise_exception=True)

    # Another invalid sort. We have to quote the + or it get ignored by the QueryDict
    query_params = QueryDict("sort=" + quote("+-id"))
    serializer = QuerySerializer(data=query_params, view=mock_view)
    with pytest.raises(ValidationError, match="This value does not match the required pattern."):
        serializer.is_valid(raise_exception=True)

    # Invalid sort (not in allowed list)
    query_params = QueryDict("sort=object,-frame_type,header")
    serializer = QuerySerializer(data=query_params, view=mock_view)
    with pytest.raises(ValidationError) as exc_info:
        serializer.is_valid(raise_exception=True)

    assert exc_info.value.detail["sort"][0]["sort"]=="frame_type is not a valid field for sorting"
    assert exc_info.value.detail["sort"][1]["sort"]=="header is not a valid field for sorting"