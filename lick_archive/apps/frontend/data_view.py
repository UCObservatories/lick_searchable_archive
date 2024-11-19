"""Frontend web interface for querying lick archive."""

import logging
import math
from datetime import datetime, time, timezone, timedelta
import itertools

logger = logging.getLogger(__name__)

from astropy.coordinates import Angle
import astropy.units

from django.shortcuts import render
from django.utils.html import format_html
from django.http import HttpResponseNotAllowed
from django.utils.dateparse import parse_datetime
from lick_archive.client.lick_archive_client import LickArchiveClient
from lick_archive.metadata.data_dictionary import Instrument
from lick_archive.utils.django_utils import log_request_debug
from lick_archive.config.archive_config import ArchiveConfigFile

lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config


from .forms import QueryForm, DEFAULT_RESULTS, DEFAULT_SORT, UI_ALLOWED_RESULT

# The lick observatory uses Pacific Standard Time regardless of dst
_LICK_TIMEZONE = timezone(timedelta(hours=-8))






def get_user_facing_result_fields(fields):
    """ Convert API result fields to user facing names, and sort them in a 
    consistent manner.

    Args:
        fields (list of str): Validated list of sort fields using API names
    Return:
        list of str: Sorted list of fields with user facing names.
        list of str: List of api field names sorted to match the user facing names.        
        list of str: List of unit descriptions sorted to match the user facing names. None is used
                     for fields without unit descriptions.

    """
    # Common field ordering
    field_ordering = ["filename", "telescope", "instrument", "frame_type", "obs_date", "exptime", "ra", "dec", "airmass", "object", "program", "observer", "coversheet", "public_date", "file_size", "mtime", "header"]

    # Sort common fields by that ordering. We use UI_ALLOWED_RESULT to get the api field/user facing field pair
    sorted_common_fields = [field_tuple for field_tuple in sorted(UI_ALLOWED_RESULT[0][1], key=lambda x: field_ordering.index(x[0])) if field_tuple[0] in fields]

    # Use the sorting in UI_ALLOWED_RESULTS for the instrument field sorting, which
    # we get by flattening UI_ALLOWED_RESULT
    instrument_fields = [field_tuple for field_tuple in (itertools.chain(*[x[1] for x in UI_ALLOWED_RESULT[1:]])) if field_tuple[0] in fields]
    
    return zip(*(sorted_common_fields + instrument_fields))

def get_user_facing_field_units(api_fields, coord_format):
    """Return user facing units for a list of fields.
    
    Args:
        api_fields (list of str): A list of field names, as 
                                  known to the backend API.

        coord_format (str):       The format to use for sky
                                  coordinates. Either "sexigesimal"
                                  or "decimal"
                                  
    Return (list of str or None): A list of strings describing the
                                  units for each field in api_fields.
                                  If there is no unit description, the
                                  list will contain a None for that 
                                  field.
    
    """
    field_units = { "obs_date": "UTC-8",
                    "exptime": "seconds"}

    if coord_format  == "sexigesimal":
        field_units['ra'] = "hms"
        field_units['dec'] = "dms"
    else:
        field_units['ra'] = "degrees"
        field_units['dec'] = "degrees"

    return [field_units.get(api_field, None) for api_field in api_fields]
        
        

def process_results(api_fields, result_list, coord_format):
    processed_results = []
    for result in result_list:
        row =[result["id"]]
        for api_field in api_fields:
            try:
                if api_field not in result:
                    row.append("")
                elif api_field == "header":
                    # Convert header to a link
                    row.append(format_html('<a href="{}">header</a>',result[api_field]))
                elif api_field == "filename":
                    # Convert filename to a download link
                    if "download_link" in result:
                        row.append(format_html('<a href="{}">{}</a>',result["download_link"], result[api_field]))
                    else:
                        # No download link in result, so don't convert filename to a link
                        row.append(result[api_field])
                elif api_field == "obs_date":
                    # Format date only out to seconds
                    value=parse_datetime(result[api_field])
                    if value is None:
                        raise ValueError(f"Invalid date time value.")
                    # Convert to lick timezone, and remove the tz info so it won't be
                    # in the output string. (The column label gives the timezone)
                    value = value.astimezone(_LICK_TIMEZONE).replace(tzinfo=None)
                    row.append(value.isoformat(sep=' ', timespec='milliseconds'))
                elif api_field == "ra":
                    if ":" in result[api_field] or "h" in result[api_field] or "m" in result[api_field] or "s" in result[api_field]:
                        try:
                            # Use astropy to parse and validate angles. It needs to be specifically told to expect hms
                            if coord_format == "decimal":
                                ra_angle = Angle(result[api_field], unit=astropy.units.hourangle).to_string(decimal=True, unit=astropy.units.deg)
                            else:
                                ra_angle = Angle(result[api_field], unit=astropy.units.hourangle).to_string(decimal=False, unit=astropy.units.hourangle,sep=":")

                        except Exception as e:
                            logger.error(f"Failed to parse angle from backend {e.__class__.__name__}:{e}")
                            ra_angle = "invalid"
                    elif len(result[api_field].strip()) > 0:
                        try:
                            # Use astropy to parse and validate angles
                            if coord_format=="decimal":
                                ra_angle = Angle(result[api_field], unit=astropy.units.deg).to_string(decimal=True)
                            else:
                                ra_angle = Angle(result[api_field], unit=astropy.units.deg).to_string(decimal=False, unit=astropy.units.hourangle, sep=":")
                            
                        except Exception as e:
                            logger.error(f"Failed to parse angle from backend {e.__class__.__name__}:{e}")
                            ra_angle = "invalid"
                    else:
                        ra_angle = ""
                    row.append(ra_angle)

                elif api_field == "dec":
                    if len(result[api_field].strip()) > 0:
                        try:
                            # Use astropy to parse and validate angles
                            # It will automatically handle dms vs decimal degrees
                            if coord_format=="decimal":
                                dec_angle = Angle(result[api_field], unit=astropy.units.deg).to_string(decimal=True)
                            else:
                                dec_angle = Angle(result[api_field], unit=astropy.units.deg).to_string(decimal=False, sep=":")
                        except Exception as e:
                            logger.error(f"Failed to parse angle from backend {e.__class__.__name__}:{e}")
                            dec_angle = "invalid"
                    else:
                        dec_angle = ""
                    row.append(dec_angle)

                elif isinstance(result[api_field], float):
                    # Only show 3 digits for floating point values
                    row.append(str(round(result[api_field], 3)))
                else:
                    row.append(result[api_field])
            except Exception as e:
                logger.error(f"Failed to parse api value for field '{api_field}'. Problem row: '{result}'", exc_info=True)                
                raise

        processed_results.append(row)
    return processed_results


def index(request):
    context = {'result_count': 0,
               'result_fields': [],
               'result_units': [],
               'result_list': None,
               'username': '',
               'archive_url': lick_archive_config.host.frontend_url + "/index.html",
               'login_url': lick_archive_config.host.frontend_url + "/users/login/",
               'logout_url': lick_archive_config.host.frontend_url + "/users/logout/",
               'total_pages': 0,
               'current_page': 1,
               'start_result': 1,
               'end_result': 1,}

    log_request_debug(request)

    if request.user.is_authenticated:
        context['username'] = request.user.username

    if request.method == 'POST':
        logger.debug(f"Unvalidated Form contents: {request.POST}")
        form = QueryForm(request.POST)
        if form.is_valid():
            logger.info("Query Form is valid.") 
            logger.debug(f"Form contents: {form.cleaned_data}")
            result_fields = DEFAULT_RESULTS if form.cleaned_data["result_fields"] == [] else form.cleaned_data["result_fields"]
            sort = DEFAULT_SORT if form.cleaned_data["sort_fields"] == [] else form.cleaned_data["sort_fields"]
            
            sort_dir = form.cleaned_data.get("sort_dir", "+")
            sort =  sort_dir + sort

            # Run the appropriate query type
            query_type = form.cleaned_data["which_query"]

            query_operator = form.cleaned_data[query_type]["operator"]
            query_value = form.cleaned_data[query_type]["value"]
            count_query = form.cleaned_data["count"] == "yes"
            page = form.cleaned_data["page"]
            page_size = form.cleaned_data["page_size"]
            prefix = False
            contains = False
            match_case = None
            # We can't use "object" for the form field because it conflicts with the python "object"
            # type. So we use object_name and rename it here.  The other form field's are named after
            # the query field sent in the API to the archive
            if query_type == "object_name":
                query_field = "object"
                match_case = form.cleaned_data[query_type]["modifier"]

            elif query_type == "date":
                # Convert dates to noon to noon datetimes in the lick observatory timezone
                query_field="obs_date"

                query_value = [datetime.combine(d, time(hour=12, minute=0, second=0, tzinfo=_LICK_TIMEZONE)) for d in query_value if d is not None]

                if len(query_value) == 1:
                    # An exact date query needs to be a datetime range ending at the next day at noon
                    query_value.append(query_value[0] + timedelta(days=1))
                elif len(query_value) == 2:
                    # The last date in a range should go to the next day at noon as the end date/time
                    query_value[1] = query_value[1] + timedelta(days=1)
                else:
                    # The form validation should prevent this, but if it didn't....
                    form.add_error("date","Invalid date or date range.")

            else:
                query_field = query_type

            # Set prefix for the string fields
            if query_field == "filename" or query_field == "object":
                prefix = query_operator == "prefix"
                contains = query_operator == "contains"

            # Look for a filter by instrument
            instruments = form.cleaned_data.get("instruments", None)
            filters = {}
            if instruments is not None:
                all_instruments = [x.name for x in Instrument]
                # If all the instruments are specified no filter is needed
                if len(instruments) != all_instruments:
                    filters["instrument"] = instruments

            if len(form.errors) == 0:
                try:
                    logger.info("Running query")
                    logger.info(f"Query Field: {query_field}")
                    logger.info(f"Query Value: {query_value}")
                    logger.info(f"Count: {count_query}")
                    logger.info(f"Results: {result_fields}")
                    logger.info(f"Sort: {sort}")
                    logger.info(f"Match Case: {match_case}")
                    logger.info(f"Prefix: {prefix}")
                    logger.info(f"Contains: {contains}")
                    logger.info(f"Username: '{request.user.username}'")

                    # Add download link to the fields passed to the api to enable downloads
                    extra_fields = []
                    if "filename" in result_fields:
                        extra_fields = ['download_link']
                        
                    archive_client = LickArchiveClient(f"{lick_archive_config.host.api_url}", 1, 30, 5, request=request)

                    total_count, result, prev, next = archive_client.query(field=query_field,
                                                                           value = query_value,
                                                                           filters=filters,
                                                                           prefix = prefix,
                                                                           contains = contains,
                                                                           match_case=match_case,
                                                                           count= count_query,
                                                                           results = result_fields + extra_fields,
                                                                           sort = sort,
                                                                           page_size=page_size,
                                                                           page = page)
                except Exception as e:
                    logger.error(f"Exception querying archive {e}", exc_info=True)
                    form.add_error(None,"Failed to run query against archive.")

            # Process the results of the query
            if len(form.errors) == 0:
                context['result_count'] = total_count
                if not count_query:

                    # Post process results
                    try:
                        # Sort the result fields in the order they should appear to the user, and get the user
                        # facing names
                        api_fields, user_fields = get_user_facing_result_fields(result_fields)
                        field_units = get_user_facing_field_units(api_fields, form.cleaned_data['coord_format'])

                        context['result_list'] = process_results(api_fields, result, form.cleaned_data['coord_format'])
                        context['result_fields'] = list(zip(user_fields, field_units))

                        # Set page information
                        total_pages = math.ceil(total_count/page_size)

                        # If the requested page is too big, the REST API will have returned an error,
                        # leave the page and other context values at their defaults
                        if page <= total_pages:
                            form.fields["page"].widget.total_pages = total_pages
                            context['total_pages'] = total_pages
                            context['current_page'] = page
                            context['start_result'] = ((page - 1) * page_size) + 1
                            context['end_result'] = context['start_result'] + len(context['result_list']) -1

                    except Exception as e:
                        logger.error(f"Exception processing results from archive {e}", exc_info=True)
                        form.add_error(None, "Failed to process results from archive.")

        for key in form.errors:
            logger.error(f"Form error {key} : '{form.errors[key]}")
    elif request.method == "GET":
        form = QueryForm()
    else:
        return HttpResponseNotAllowed(['GET','POST'])
    context['form'] = form
    return render(request, 'frontend/index.html', context)

