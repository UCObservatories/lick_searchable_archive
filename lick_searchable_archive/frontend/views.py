"""Frontend web interface for querying lick archive."""

import os
import logging
import math
from datetime import datetime, time, timezone, timedelta
import zoneinfo
import functools

logger = logging.getLogger(__name__)


from django.conf import settings
from django.shortcuts import render
from django import forms
from django.core.exceptions import NON_FIELD_ERRORS, ValidationError
from django.forms.renderers import TemplatesSetting
from django.utils.html import format_html, escape
from django.utils.safestring import mark_safe
from django.utils.dateparse import parse_datetime
from lick_archive.lick_archive_client import LickArchiveClient
from lick_archive.db import archive_schema

# The lick observatory uses Pacific Standard Time regardless of dst
_LICK_TIMEZONE = timezone(timedelta(hours=-8))


class OperatorWidget(forms.MultiWidget):

    def __init__(self, operators, subwidgets, names, class_prefix, attrs=None):

        widgets={"operator": forms.Select(choices=operators, attrs={"class": class_prefix+"operator"})}

        if len(names) != len(subwidgets):
            raise ValueError("Length of names must match length of subwidgets")        

        for i, subwidget in enumerate(subwidgets):
            subwidget.attrs["class"] = class_prefix + "value_" + str(i)
            widgets[names[i]] = subwidget

        super().__init__(widgets,attrs)

    def decompress(self, value):
        return_value = []
        if isinstance(value,dict):
            logger.debug("Decompressing dict")
            return_value = [value["operator"], value["value"]]
        else:
            return_value = ['','']
        logger.debug(f"Returning: {return_value}")
        return return_value

class PageNavigationWidget(forms.Widget):

    def __init__(self, attrs, form_id, max_controls, num_surrounding_controls=2):
        super().__init__(attrs)
        self.template_name = "widgets/page_navigation.html"
        self.max_controls = max_controls
        self.num_surrounding_controls=num_surrounding_controls
        self.total_pages=1
        self.form_id = form_id

    def determine_pages(self, current_page):
        need_start_ellipses = False
        need_end_ellipses = False

        if not isinstance(current_page, int):
            current_page = int(current_page)

        # The page list always starts with the previous page and the first page. A negative previous page is allowed, and
        # renders to a disabled HTML control.
        page_list=[(current_page - 1, "<"), (1, "1")]

        if self.total_pages <= self.max_controls -2 :
            # There are enough controls to show all the pages
            page_list += list(zip(range(2,self.total_pages),[str(n) for n in range(2,self.total_pages)]))
        else:
            # All of the page numbers won't fit within the desired number of controls, some ellipses will be needed.

            # At low pages numbers the ellipses will be before the last page. 
            # For example with max_controls = 12, total_pages=100, current_page = 2, surrounding = 2 
            #     <<  1 <2> 3  4  5  6  7  8  ...100 >>
            # This will continue to be the case until there aren't enough controls to hold the surrounding pages. This is the change over
            # point. In this example the change over is between pages 6 and 7
            #     <<  1  2  3  4  5 <6> 7  8  ...100 >>
            #     <<  1 ... 4  5  6 <7> 8  9  ...100 >>
            
            # To find the change over point, find the last page that could be displayed without elipses after the 1 (max_controls -4)
            # and subtract the number of surrounding pages. The 4 includes the previous page and next page controls, the last page control,
            # and the ellipses before the last page.
            change_over = self.max_controls-(4+self.num_surrounding_controls)

            if current_page <= change_over:
                # The current page is before the change over, only one ellipses is needed, at the end
                need_end_ellipses = True
                
                # The range of numbers up to but not included the ellipses before the last page
                start_range=2
                end_range=self.max_controls-4
            else:
                # After the change over, there will be one ellipses at the start, and potentially one at the end
                need_start_ellipses = True

                if current_page+self.num_surrounding_controls < self.total_pages-2:
                    # The current page is far enough away from the last page to require ellipses at the end
                    need_end_ellipses = True

                    # End the range with the last of the surrounding pages around the current_page
                    end_range=current_page+self.num_surrounding_controls

                    # Start the range with enough pages to fill up the maximum number of controls 
                    # (-6 to exclude for the two ellipses, previous and next page, and first and last page)
                    start_range = (end_range-(self.max_controls-6))+1
                else:
                    # The current page is close enough to the final page that no ellipses are needed at the end
                    # End the range just before the final page
                    end_range=self.total_pages-1
                    # Start the range with enough pages to fill up the maximum allowed controls (-5 for one ellipses, the previous and next page,
                    # and the first and last page)
                    start_range=(end_range-(self.max_controls-5))+1

            # Build the pages between the first and last page, adding ellipses if needed
            if need_start_ellipses:
                page_list.append(("...","..."))

            page_list += list(zip(range(start_range,end_range+1),[str(n) for n in range(start_range,end_range+1)]))

            if need_end_ellipses:
                page_list.append(("...","..."))

        # The page list ends with the last page and the previous page
        if self.total_pages > 1:
            page_list.append((self.total_pages, str(self.total_pages)))

        page_list.append((current_page+1, ">"))

        return page_list

    def format_value(self, value):
        return int(value)

    def get_context(self, name, value, attrs):
        default_context = super().get_context(name, value, attrs)
        # Set the total pages from our choices value
        default_context['widget']['total_pages'] = self.total_pages
        default_context['widget']['page_list'] = self.determine_pages(value)
        default_context['widget']['form_id'] = self.form_id

        logger.debug(f"Returning: {default_context['widget']}")
        return default_context

class QueryWithOperator(forms.MultiValueField):

    #@staticmethod
    #def validate(value):
    ##    if not isinstance(value, dict):
    ##        raise ValidationError("Value not dict.", code="required" )
    #    if len(value) < 1:
    #        raise ValidationError("One search field must be entered.", code="required" )
    #    elif len(value) > 1:
    #        raise ValidationError("Only one search field can be entered.", code="required:")

    #    value_key = value.keys()[0]
    #    if  value_key not in ["date", "filename", "object"]:
    #        raise ValidationError("Unrecognized search field %(value)s", parans = {"value": value_key}, code="required:")

    def __init__(self, operators, fields, names=[''], class_prefix='', **kwargs):
        error_messages = {"incomplete": "Enter an operator and value"}

        all_fields = (forms.ChoiceField(choices=operators),
                      *fields)


        super().__init__(fields=all_fields, require_all_fields=True, 
                         widget=OperatorWidget(operators=operators, class_prefix=class_prefix, subwidgets=[field.widget for field in fields],names=names),
                         error_messages=error_messages, **kwargs)

    def compress(self, data_list):
        if len(data_list) == 2:
            value = {"operator": data_list[0], "value": data_list[1]}
        elif len(data_list) > 2:
            value = {"operator": data_list[0], "value": tuple(data_list[1:])}
        else:
            # Emtpy list
            return {"operator": None, "value": None}

        return value

DEFAULT_SORT = ["obs_date"]
DEFAULT_RESULTS = ["filename", "instrument", "frame_type", "object", "exptime", "obs_date"]  

def get_field_groups(fields, exclude):
    
    groups = dict()
    for field_name in fields:
        if field_name not in exclude:
            field_info = archive_schema.field_info[field_name]
            field_value = (field_name, field_info.user_name)
            groups.setdefault(field_info.user_group, []).append(field_value)

    # This list establishes the order groups appear on the front end, 
    # We put the default "common" group first, followed by the other groups in alphabetical order
    group_names = [archive_schema.DEFAULT_GROUP] + [group_name for group_name in sorted(groups.keys()) if group_name != archive_schema.DEFAULT_GROUP]

    return [[group_name, groups[group_name]] for group_name in group_names]


# Create your models here.
class QueryForm(forms.Form):
    #start_date = forms.DateField(label="Observation Date End", required=False)
    which_query = forms.ChoiceField(choices=[("filename",""), ("object_name",""), ("date","")], initial="object_name", required=True, widget=forms.RadioSelect)
    filename =   QueryWithOperator(label="By Path and Filename", operators=[("exact", "="), ("prefix", "starts with")],
                                   class_prefix="search_terms_",
                                   fields=[forms.CharField(max_length=1024, strip=True, empty_value="")],
                                   initial={"operator": "exact", "value": ""},  help_text='e.g. "2014-04/08/AO/m140409_0040.fits"',required=False)
    object_name = QueryWithOperator(label="By Object", operators= [("exact", "="), ("prefix", "starts with"), ("contains", "contains")],
                                    class_prefix="search_terms_",
                                    fields=[forms.CharField(max_length=80, empty_value="", strip=True)],
                                    initial={"operator": "exact", "value": ""},  help_text='e.g. "K6021275"', required=False) 
    object_case = forms.BooleanField(label="Case Insensitive Object Search", initial=True, required=False)
    date =   QueryWithOperator(label="By Observation Date", operators= [("exact", "="), ("range", "between")],
                               class_prefix="search_terms_",
                               fields=[forms.DateField(),forms.DateField()], names=["start", "end"],
                               initial={"operator": "exact", "value": None},  help_text='e.g. "2006-08-17". All dates are noon to noon PST (UTC-8). Date ranges are inclusive.', required=False)
    count = forms.ChoiceField(label="", choices=[("yes","Return only a count of matching files."), ("no","Return information about matching files.")], initial="no", required=True, widget=forms.RadioSelect)
    result_fields = forms.MultipleChoiceField(initial = DEFAULT_RESULTS,
                                              choices = get_field_groups(archive_schema.allowed_result_attributes, exclude=["id"]),        
                                              required=False)
    sort_fields = forms.MultipleChoiceField(initial = DEFAULT_SORT, choices = get_field_groups(archive_schema.allowed_sort_attributes, exclude=["id"]), 
                                            required=False)
    page=forms.IntegerField(min_value=1, widget=PageNavigationWidget(attrs={"class": "page_nav"},form_id="archive_query", max_controls=12))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['result_fields'].widget.attrs.update(size="10")
        self.fields['sort_fields'].widget.attrs.update(size="10")


    def clean(self):
        cleaned_data = super().clean()
        if cleaned_data is None:
            cleaned_data = self.cleaned_data
    
        # Verify the appropriate query type has a value populated        
        query_type = cleaned_data.get("which_query", '')
        if query_type == '':
            self.add_error('search terms', "No search term selected.")
        else:
            query_field = cleaned_data.get(query_type, None)
            if query_field is None or not isinstance(query_field, dict):
                query_value = None
                query_operator = None
            else:
                query_value = cleaned_data[query_type].get("value", None)
                query_operator = cleaned_data[query_type].get("operator", "exact")

            if query_type == "filename":
                if query_value is None or len(query_value) == 0:
                    self.add_error("filename",f"Cannot query by empty path.")

            elif query_type == "object_name":
                if query_value is None or len(query_value) == 0:
                    self.add_error("object_name", f"Cannot query by empty object.")
                if cleaned_data.get("object_case") is None:
                    self.add_error("object_case", f"Must specify whether an object query is case insensitive.")

            elif query_type == "date":
                if query_value is None:
                    self.add_error("date", f"Cannot query by empty date.")
                elif query_operator == 'exact':
                    if not isinstance(query_value,tuple) or query_value[0] is None:
                        self.add_error("date", f"Cannot query by empty date.")
                else:
                    if query_value[0] is None:
                        self.add_error("date", "Start date cannot be empty when querying a date range.")
                    if query_value[1] is None:
                        self.add_error("date", "End date cannot be empty when querying a date range.")
            else:
                self.add_error("which_query", f"Unknown query type {query_type}.")


        return cleaned_data

archive_client = LickArchiveClient(f"{settings.LICK_ARCHIVE_API_URL}", 1, 30, 5)


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
    field_ordering = ["filename", "telescope", "instrument", "frame_type", "obs_date", "exptime", "ra", "dec", "airmass", "object", "program", "observer", "header"]

    common_fields = []
    instrument_fields = {}
    units = { "obs_date": "UTC-8",
              "exptime": "seconds"}

    # Separate common fields from instrument specific fields
    for field in fields:
        field_info = archive_schema.field_info[field]
        if field_info.user_group == archive_schema.DEFAULT_GROUP:
            common_fields.append((field, field_info))
        else:
            instrument_fields.setdefault(field_info.user_group, []).append((field, field_info))

    # Sort common fields by the order in field_ordering
    common_fields.sort(key=lambda x: field_ordering.index(x[0]))

    api_fields = [f[0] for f in common_fields]
    field_units = [units.get(f[0]) for f in common_fields]
    user_fields = [f[1].user_name for f in common_fields]

    # Sort instrument fields by instrument name, then by user facing field name
    for instrument in sorted(instrument_fields.keys()):        
        instrument_fields[instrument].sort(key = lambda x: x[1].user_name)
        api_fields +=  [f[0] for f in instrument_fields[instrument]]
        field_units += [units.get(f[0]) for f in instrument_fields]
        user_fields += [f[1].user_name for f in instrument_fields[instrument]]

    return user_fields, api_fields, field_units

def process_results(api_fields, result_list):
    processed_results = []
    for result in result_list:
        row =[]
        for api_field in api_fields:
            try:
                if api_field not in result:
                    row.append("")
                elif api_field == "header":
                    # Convert header to a link
                    row.append(format_html('<a href="{}">header</a>',result[api_field]))
                elif api_field == "obs_date":
                    # Format date only out to seconds
                    value=parse_datetime(result[api_field])
                    if value is None:
                        raise ValueError(f"Invalid date time value.")
                    # Convert to lick timezone, and remove the tz info so it won't be
                    # in the output string. (The column label gives the timezone)
                    value = value.astimezone(_LICK_TIMEZONE).replace(tzinfo=None)
                    row.append(value.isoformat(sep=' ', timespec='milliseconds'))
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

def test_determine_pages():
    w = PageNavigationWidget({}, "form", 12, 2)
    for total_pages in range(1,16):
        w.total_pages = total_pages
        for page in range(1, total_pages+1):
            l = w.determine_pages(page)
            print(f"{page:-2} of {total_pages:-2} length({len(l):-2}) {l}")


def index(request):
    context = {'result_count': 0,
               'result_fields': [],
               'result_units': [],
               'result_list': None,
               'archive_url': settings.LICK_ARCHIVE_FRONTEND_URL,
               'total_pages': 0,
               'current_page': 1,
               'start_result': 1,
               'end_result': 1,}

    page_size = 50

    if logger.isEnabledFor(logging.DEBUG):
        for key in request.META.keys():
            if key.startswith("HTTP_"):
                logger.debug(f"Header key '{key[5:]}' value: {request.META[key]}")

    if request.method == 'POST':
        logger.debug(f"Unvalidated Form contents: {request.POST}")
        form = QueryForm(request.POST)
        if form.is_valid():
            logger.info("Query Form is valid.") 
            logger.debug(f"Form contents: {form.cleaned_data}")
            result_fields = DEFAULT_RESULTS if form.cleaned_data["result_fields"] == [] else form.cleaned_data["result_fields"]
            sort = DEFAULT_SORT if form.cleaned_data["sort_fields"] == [] else form.cleaned_data["sort_fields"]

            # Run the appropriate query type
            query_type = form.cleaned_data["which_query"]

            query_operator = form.cleaned_data[query_type]["operator"]
            query_value = form.cleaned_data[query_type]["value"]
            count_query = form.cleaned_data["count"] == "yes"
            page = form.cleaned_data["page"]
            prefix = False
            contains = False
            match_case = True
            # We can't use "object" for the form field because it conflicts with the python "object"
            # type. So we use object_name and rename it here.  The other form field's are named after
            # the query field sent in the API to the archive
            if query_type == "object_name":
                query_field = "object"
                match_case = not form.cleaned_data["object_case"]

            elif query_type == "date":
                # Convert dates to noon to noon datetimes in the lick observatory timezone
                query_field="datetime"

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

            if len(form.errors) == 0:
                try:
                    logger.info("Running query")
                    logger.info(f"Query Field: {query_field}")
                    logger.info(f"Query Value: {query_value}")
                    logger.info(f"Count: {count_query}")
                    logger.info(f"Results: {result_fields}")
                    logger.info(f"Sort: {sort}")
                    logger.info(f"Prefix: {prefix}")

                    total_count, result, prev, next = archive_client.query(field=query_field,
                                                                        value = query_value,
                                                                        prefix = prefix,
                                                                        contains = contains,
                                                                        match_case=match_case,
                                                                        count= count_query,
                                                                        results = result_fields,
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
                        user_fields, api_fields, field_units = get_user_facing_result_fields(result_fields)

                        context['result_list'] = process_results(api_fields, result)
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
    else:
        form = QueryForm()

    context['form'] = form
    return render(request, 'frontend/index.html', context)

