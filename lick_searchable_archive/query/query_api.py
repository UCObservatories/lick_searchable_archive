"""The classes that implement the query API used by the lick archive."""
import logging

logger = logging.getLogger(__name__)

import datetime
from pathlib import Path
import os

from django.db.models import F
from rest_framework.pagination import PageNumberPagination
from rest_framework.serializers import ValidationError
from rest_framework.exceptions import APIException
from rest_framework.response import Response
from rest_framework import status, serializers
from rest_framework.generics import ListAPIView
from django.conf import settings

from astropy.coordinates import SkyCoord, Angle
from lick_archive.db import archive_schema
from lick_archive.db.pgsphere import SCircle


class ListWithSeperator(serializers.ListField):
    """
    A custom list field that supports items seperated by a seperator character. This is used to
    support URL query strings like "results=filename,object,obs_date"

    Args:
    sep_char (str):  The seperator character.
    """
    def __init__(self, sep_char, **kwargs):
        super().__init__(**kwargs)

        if len(sep_char) != 1:
            raise ValueError("sep_char must be a single character")
        self.sep_char = sep_char

    def to_internal_value(self, data):
        """Override to_internal_value to convert a string to a list split by our seperatar character."""

        if isinstance(data, list):
            split_data = []
            for item in data:
                split_data +=  item.split(self.sep_char)
        else:
            split_data = data
        
        return super().to_internal_value(split_data)

class QuerySerializer(serializers.Serializer):
    """A Serializer class used to validate the query string.
    """
    filename = serializers.CharField(max_length=256, required=False)
    date = ListWithSeperator(sep_char=",", child=serializers.DateField(), 
                             min_length=1, max_length=2,
                             allow_empty=False, required=False)
    datetime=ListWithSeperator(sep_char=",", 
                               child=serializers.DateTimeField(format='iso-8601', input_formats=['iso-8601'], default_timezone=datetime.timezone.utc), 
                               min_length=1, max_length=2,
                               allow_empty=False, required=False)
    object = serializers.CharField(max_length=256, required=False)
    ra_dec = ListWithSeperator(sep_char=",", child=serializers.FloatField(), min_length=2, max_length=3, allow_empty=False, required=False)
    count = serializers.BooleanField(default=False, required=False)
    prefix = serializers.BooleanField(default=False)
    contains = serializers.BooleanField(default=False)
    match_case = serializers.BooleanField(default=True)
    results = ListWithSeperator(sep_char=",", child=serializers.RegexField(regex='^[A-Za-z][A-Za-z0-9_]*$', max_length=30, allow_blank=False), default=[], max_length=128)
    sort = ListWithSeperator(sep_char=",", child=serializers.RegexField(regex='^(-|\+)?[A-Za-z][A-Za-z0-9_]*$', max_length=30, allow_blank=False), default=["id"], max_length=128, required=False, allow_empty=False)
    filters = ListWithSeperator(sep_char=",",child=serializers.CharField(max_length=60, allow_blank=False),min_length=1, max_length=128, required=False, allow_empty=False)

    def __init__(self, data, view):
        """
        Initialize the serializer.

        Args:
        data (django.http.QueryDict): The QueryDict representing the query string as parsed by Django.
        view (QueryAPIView):          The view object receiving the query. 
                                      It should specify allowed_result_attributes, and allowed_sort_attributes
                                      as attributes.
        """
        self.allowed_result_attributes = view.allowed_result_attributes
        self.allowed_sort_attributes = view.allowed_sort_attributes

        super().__init__(data=data)

    def validate_ra_dec(self, value):
        """Validate ra value in ra_dec"""
        dec=value[1]
        if dec <= -90.0 or dec >= 90.0:
            raise serializers.ValidationError([{'ra_dec': 'DEC must be between -90 and 90 degrees'}])
        return value

    def validate_filters(self,value):
        """Validate the filters passed into the query."""
        # Eventually this might allow filtering on arbitrary fields using simple expressions,
        # but for now we only allow one filter, on instrument
        if value[0] != "instrument":
            raise serializers.ValidationError([{'filters': 'Only "instrument" is allowed as a filter.'}])
        requested_instruments = []
        valid_instruments = [x.name for x in archive_schema.Instrument]
        for instrument in value[1:]:
            # We'll allow case insensitive instrument names in the query
            if instrument.upper() in valid_instruments:
                # The DB holds the string value of the enum
                requested_instruments.append(archive_schema.Instrument[instrument.upper()].value)
            else:
                raise serializers.ValidationError([{'filters': 'Instrument filter must be one of: ' + ",".join([f"\"{x}\"" for x in valid_instruments])}])
        if len(requested_instruments)==0:
            raise serializers.ValidationError([{'filters': 'Instrument filter must be one of: ' + ",".join([f"\"{x}\"" for x in valid_instruments])}])
        return requested_instruments

    def validate_sort(self, value):
        """Validate the sort fields of a query"""
        errors = []

        # Validate each field
        for sort_field in value:
            # Pull off the "-" indicating a reversed sort
            if sort_field.startswith("-"):
                field_name = sort_field.strip("-")
            elif sort_field.startswith("+"):
                field_name = sort_field.strip("+")
            else:
                field_name = sort_field

            if field_name not in self.allowed_sort_attributes:
                errors.append({'sort': f"{field_name} is not a valid field for sorting"})
            
        if len(errors) > 0:
            raise serializers.ValidationError(errors)
        return value

    def validate_results(self, value):
        """Validate the result fields of a query"""
        errors = []
        for result_field in value:
            if  result_field not in self.allowed_result_attributes:
                errors.append({'results': f"{result_field} is not a valid result field."})
            
        if len(errors) > 0:
            raise serializers.ValidationError(errors)
        return value
    
    def validate(self, data):
        """Validates relationships between fields in the query"""

        # Make sure prefix and contains are not both true
        if "prefix" in data and "contains" in data:
            if data["prefix"] and data["contains"]:
                raise serializers.ValidationError([{'prefix': "'prefix' and 'contains' cannot both be true."}])
        return data



class QueryAPIPagination(PageNumberPagination):
    """Paginate the results of the archive Query API. Uses the Django Rest Framework PageNumberPagination
    class to do most of the work.
    """

    # Define the strings used in the URL to paginate.
    page_size_query_param = "page_size"
    page_size=50
    max_page_size=1000

    def __init__(self):
        self.is_count=False

    def paginate_queryset(self, queryset, request, view):
        """Returns the appropriate page of results from a queryset given a request.
        count queries are not paginated.
        
        Args:
            queryset (django.db.models.query.QuerySet): 
            The QuerySet to get results from.
            
            request (rest_framework.request.Request):   
            The request specifying the query.
            
            view    (QueryAPIView)):                    
            The view running the query. It should have an allowed_result_attributes attribute.

            Return (Mapping): 
            The page the resultsA filtered and sorted QuerySet returning the requested page.
        """

        # Make sure the request has been validated
        if not hasattr(request, "validated_query"):
            logger.error(f"Unvalidated request passed to paginate_queryset.")
            raise APIException("Unvalidated request passed to paginate_queryset.")


        if request.validated_query['count'] is True:
            # Don't paginate, it's a count query
            # The queryset was already filtered by the view, so just run the count
            self.is_count=True
            return [{"count": queryset.count()}]
        else:
            # Set the result attributes
            logger.info(f"QueryParams {request.validated_query} results: {request.validated_query['results']}")

            if len(request.validated_query['results']) == 0:                
                # Use all allowed result attributes if none are set
                requested_attributes = view.allowed_result_attributes
            else:
                requested_attributes =  request.validated_query['results']

                # Make sure all sort attributes are included in the results
                for sort_attribute in request.validated_query['sort']:
                    if sort_attribute.startswith("+") or sort_attribute.startswith("-"):
                        sort_attribute=sort_attribute[1:]
                    if sort_attribute not in requested_attributes:
                        requested_attributes.append(sort_attribute)

            # Make sure "id" is always in the result attributes
            if "id" not in requested_attributes:
                requested_attributes = ["id"] + requested_attributes

            result_attributes = []
            result_expressions={}

            # Make a shallow copy of the result attributes, replacing
            # the special "header" attribute withe an expression
            # that references the filename
            for api_result_name in requested_attributes:
                if api_result_name == "header":
                    result_expressions[api_result_name] = F('filename')
                else:
                    result_attributes.append(api_result_name)

            # Apply the result attributes to the queryset
            queryset = queryset.values(*result_attributes, **result_expressions)

        # Use the superclass to handle the logic of paginating
        return super().paginate_queryset(queryset, request, view)

    def get_paginated_response(self, data):
        """Return a paginated response from data returned from the query.
        
        Args:
        data (Mapping): A page of data results from a query.

        Return (rest_framework.response.Response):
        The response with the data formatted approriately with next page/previous page links.
        """

        if self.is_count is True:
            # Counts don't need fancy paginated results
            return Response(data[0])
        else:
            return super().get_paginated_response(data)

class QueryAPIFilterBackend:
    """A filter backend to filter a query set based on a request."""

    def get_ordering(self, request, queryset, view):
        """Return the fields that should be used to order the query based on the
        request.
        
        Args:
        request (rest_framework.request.Request): 
        The request specifying the query.

        queryset (django.models.query.QuerySet): 
        The queryset being used for the query. 

        view (QueryAPIView):
        The view running the query. It should have the "indexed_attributes", 
        "allowed_sort_attributes" and "allowed_result_attributes" attributes that
        are used to validate the query.

        Return (list): The list of field names to sort by.

        Raises:
        rest_framework.serializers.ValidationError: Thrown if the query is not valid.
        """

        # Make sure the request has been validated
        if not hasattr(request, "validated_query"):
            logger.error(f"Unvalidated request passed to get_ordering.")
            raise APIException("Unvalidated request passed to get_ordering.")

        return request.validated_query['sort']

    def filter_queryset(self, request, queryset, view):
        """Filter a query set based on a request.
        
        Args:
        request (rest_framework.request.Request): 
        The request specifying the query.

        queryset (django.models.query.QuerySet): 
        The queryset to filter. 

        view (QueryAPIView):
        The view running the query. It should have the "indexed_attributes", 
        "allowed_sort_attributes" and "allowed_result_attributes" attributes that
        are used to validate the query.

        Return (django.models.query.QuerySet): A QuerySet filtered according to the request.

        Raises:
        rest_framework.serializers.ValidationError: Thrown if the query is not valid.
        """

        # Make sure the request has been validated
        if not hasattr(request, "validated_query"):
            logger.error(f"Unvalidated request passed to filter_queryset.")
            raise APIException("Unvalidated request passed to filter_queryset.")

        # The prefix parameter indicates whether the query on the required field is for a prefix
        # i.e. whether a % is appended at the end of the search value
        if request.validated_query['prefix']:
            string_match = "startswith"
        elif request.validated_query['contains']:
            string_match = "contains"
        else:
            string_match = "exact"

        required_field = None
        required_search_value = None

        # Make sure at least one of the required fields is being queried.
        # These are the fields that are indexed in the database.
        for field in view.indexed_attributes:
            if field in request.validated_query:
                if required_field is None:
                    required_field = field
                    required_search_value = request.validated_query[field]
                else:
                    # We don't allow duplicates of these fields, at least for now.
                    raise ValidationError({"query": f"Only one field of: ({', '.join(view.indexed_attributes)}) may be queried on."})


        if required_field is None:
            raise ValidationError({"query": f"At least one required field must be included in the query. The required fields are: ({', '.join(view.indexed_attributes)})"})

        logger.info(f"Building {required_field} query on '{required_search_value}'")
        filters = self._build_where(required_field, required_search_value, string_match, request.validated_query['match_case'],
                                    request.validated_query.get('filters', None))

        queryset = queryset.filter(**filters)

        # Add sort attributes if needed
        if request.validated_query['count'] is False and len(request.validated_query['sort']) > 0:
            return queryset.order_by(request.validated_query['sort'])
        else:
            return queryset
            
    def _build_where(self, field, value, string_match, match_case, user_filters=None):
        """Build the Django keyword arguments to filter a queryset.
        
        Args:
            field (str): The field name to filter on
            
            value (str, or datetime.date): The value to filter by.
            
            string_match (str): 
                How to compare strings. Either "exact", "startswith", or "contains".

            match_case (bool):
                Whether or not to to perform case sensitive string filtering.

            user_filters(list):
                Optional filters to apply to the where clause.

        Returns (dict): The keyword arguments needed to filter the QuerySet.
        """
        filters = {}
        if field == 'filename':
            # The database has the full filename, but clients only see the relative pathname
            # A weird implication is that if the client wants to use an absolute path, because
            # os.path.join will ignore the first path if the second path is an absolute path.
            full_filename = os.path.join(settings.LICK_ARCHIVE_ROOT_DIR, value)
            logger.debug(f"rootdir {settings.LICK_ARCHIVE_ROOT_DIR}, value {value} Full filename {full_filename}")
            self._build_string_filter(filters, field, full_filename, string_match, match_case)
        elif field == 'object':
            self._build_string_filter(filters, field, value, string_match, match_case)
        elif field == 'date':
            start_date_time = datetime.datetime.combine(value[0], datetime.time(hour=0, minute=0, second=0))
            if len(value) > 1:
                end_date_time = datetime.datetime.combine(value[1]+datetime.timedelta(days=1), datetime.time(hour=0, minute=0, second=0))
            else:
                end_date_time = datetime.datetime.combine(value[0]+datetime.timedelta(days=1), datetime.time(hour=0, minute=0, second=0))
    
            self._build_range_filter(filters, "obs_date", start_date_time, end_date_time)
        elif field == 'datetime':
            start_date_time = value[0]
            if len(value) > 1:
                end_date_time = value[1]
                # If a date range was given, we add a millisecond so the results are inclusive
                self._build_range_filter(filters, "obs_date", start_date_time, end_date_time+ datetime.timedelta(milliseconds=1))
            else:
                self._build_exact_filter(filters, "obs_date", value[0])
        elif field == 'ra_dec':
            ra_dec = SkyCoord(ra=value[0], dec=value[1], unit=("deg", "deg"))
            if len(value) == 3:
                radius = Angle(value[2], unit="deg")
            else:
                # Use a default if no radius is given
                radius = Angle(settings.LICK_ARCHIVE_DEFAULT_SEARCH_RADIUS)
            self._build_contained_in_filter(filters, "coord", ra_dec, radius)

        if user_filters is not None:
            # Currently only instrument filters are supported
            self._build_in_filter(filters, "instrument", user_filters)



        return filters

    def _build_range_filter(self, filters, orm_field_name, value1, value2):
        """Build a range filter for a field.
        
        Args:
        filters (dict):       A filter dictionary to add the filter to.
        orm_filed_name (str): The orm field to name to filter on, which may not be the same name used
                              in the query string.
        value1 (object):      The first value in the range to filter by. The range will be re-arranged
                              if value1 is not less than value2.
        value2 (object):      The second value in the range to filter by. The range will be re-arranged
                              if value1 is not less than value2.
        """
        if value1 < value2:
            start_value = value1
            end_value = value2
        else:
            start_value = value2
            end_value = value1

        logger.debug(f"Using range {start_value}, {end_value}")        
        filters[orm_field_name + "__range"] = (start_value, end_value)

    def _build_string_filter(self, filters, orm_field_name, value, string_match, match_case):
        """Build a string filter for a field.
        
        Args:
        filters (dict):       A filter dictionary to add the filter to.
        orm_filed_name (str): The orm field to name to filter on, which may not be the same name used
                              in the query string.
        value (str):          The value to filter by.
        string_match (str):     Either "exact", "startswith", "in", or "contains"
        match_case (bool):    If true, a case sensitive search is performed. If false, it is case insensitive.
        """
        logger.debug(f"String filter value {value}")
        if match_case:
            sensitivity = ""
        else:
            sensitivity = "i"

        filters[f"{orm_field_name}__{sensitivity}{string_match}" ] = value

    def _build_in_filter(self, filters, orm_field_name, values):
        """Build a filter for a field that will exactly match one of a fixed set of values.
        
        Args:
        filters (dict):       A filter dictionary to add the filter to.
        orm_filed_name (str): The orm field to name to filter on, which may not be the same name used
                              in the query string.
        values (list or Any): The value or values to filter by.
        """
        logger.debug(f"in filter value {values}")
        if not isinstance(values, list):
            values = [values]
        filters[f"{orm_field_name}__in" ] = values

    def _build_contained_in_filter(self, filters, orm_field_name, ra_dec, radius):
        """Build a filter for the PostgreSQL "<@" geometric operation that searches for
           coordinates within a circle.

        Args:
        filters (dict):       A filter dictionary to add the filter to.
        orm_filed_name (str): The orm field to name to filter on, which may not be the same name used
                              in the query string.
        ra_dec (SkyCoord):    The center of a circle on the sky.
        radius (Angle):       The angular radius of a circle.
                    
        """
        logger.debug(f"in contained in filter {ra_dec} {radius}")
        # To be a proper Django operation we'd have to make a custom
        # lookup, but since we're faking it with SQLAlchemy we don't have to
        filters[f"{orm_field_name}__contained_in" ] = SCircle(ra_dec, radius)

    def _build_exact_filter(self, filters, orm_field_name, value):
        """Build a filter for a field that will exactly match a value
        
        Args:
        filters (dict):       A filter dictionary to add the filter to.
        orm_filed_name (str): The orm field to name to filter on, which may not be the same name used
                              in the query string.
        value (str):          The value to filter by.
        """
        logger.debug(f"exact filter value {value}")
        filters[orm_field_name + "__exact"] = value


class QueryAPIView(ListAPIView):
    """
    A custom DRF view for implementing the archive query api. It should be subclassed
    to specify the QuerySet type, Serializer type. The subclass should also
    populate the allowed_sort_attributes, allowed_result_attributes, and indexed_attributes
    values.
    """
    # Use cursor based pagination
    pagination_class = QueryAPIPagination
    filter_backends = [QueryAPIFilterBackend]
    allowed_sort_attributes =[]
    allowed_result_attributes =[]
    indexed_attributes = []

    def list(self, request, *args, **kwargs):
        """Performs a query based on a request. The query is handled by the
        ListAPIView, QueryAPIPagination, and QueryAPIFilterBackend classes. 
        This class performs post processing of database results.

        The post processing involves:
        Replacing the full path in the filename to the relative path. (Hiding the
        mount point of the archive file system from clients).

        Replacing the filename returned as "header" with a URL that can be used to
        access a plaintext version of the header.
        
        Args:
        request (rest_framework.requests.Request): The request specifying the query.        
        args (list):     Additional arguments to the view.
        **kwargs (dict): Additional keyword arguments to the view.
        
        Return (rest_framework.response.Response): The processed response from the query.
        """

        # Validate the query using a serializer
        serializer = QuerySerializer(data=request.query_params, view=self)
        logger.debug(f"QueryParams {request.query_params}")
        try:
            serializer.is_valid(raise_exception=True)
        except Exception as e:
            logger.error(f"QueryParams {request.query_params}", exc_info=True)
            raise

        # Store the validated results in the request to be passed to paginators and filters
        request.validated_query = serializer.validated_data

        # Use the superclass method to utilize the DRF's infrastructure
        response = super().list(request, *args, **kwargs)
        if response.status_code == status.HTTP_200_OK:
            # A count query doesn't have a results entry
            if 'results' in response.data:
                # Filter header URLS to have the propper format and
                # both header and filename entries to be relative paths
                for record in response.data['results']:
                    if "header" in record:
                        filepath = Path(record['header'])
                        relative_path = filepath.relative_to(settings.LICK_ARCHIVE_ROOT_DIR)
                        header_url = settings.LICK_ARCHIVE_HEADER_URL_FORMAT.format(relative_path)
                        record["header"] = header_url
                    if "filename" in record:
                        record["filename"] = str(Path(record['filename']).relative_to(settings.LICK_ARCHIVE_ROOT_DIR))
        return response