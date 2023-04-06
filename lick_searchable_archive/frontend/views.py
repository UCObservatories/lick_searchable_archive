from datetime import date, datetime
import os
import urllib.parse
import logging

logger = logging.getLogger(__name__)


from django.conf import settings
from django.shortcuts import render
from django import forms
from django.core.exceptions import NON_FIELD_ERRORS, ValidationError
from django.forms.renderers import TemplatesSetting
from django.utils.html import format_html
from django.utils.dateparse import parse_datetime
from lick_archive.lick_archive_client import LickArchiveClient
from lick_archive.db import archive_schema



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

DEFAULT_SORT = None
DEFAULT_RESULTS = ["filename", "frame_type", "object", "exptime", "obs_date"]  

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
    object_name = QueryWithOperator(label="By Object", operators= [("exact", "="), ("prefix", "starts with")],
                                    class_prefix="search_terms_",
                                    fields=[forms.CharField(max_length=80, empty_value="", strip=True)],
                                    initial={"operator": "exact", "value": ""},  help_text='e.g. "K6021275"', required=False) 
    date =   QueryWithOperator(label="By Observation Date", operators= [("exact", "="), ("range", "between")],
                               class_prefix="search_terms_",
                               fields=[forms.DateField(),forms.DateField()], names=["start", "end"],
                               initial={"operator": "exact", "value": None},  help_text='e.g. "2006-08-17". Date ranges are inclusive.', required=False)
         
    count = forms.ChoiceField(label="", choices=[("yes","Return only a count of matching files."), ("no","Return information about matching files.")], initial="no", required=True, widget=forms.RadioSelect)
    result_fields = forms.MultipleChoiceField(initial = DEFAULT_RESULTS,
                                              choices = get_field_groups(archive_schema.allowed_result_attributes, exclude=["id"]),        
                                              required=False)
    sort_fields = forms.MultipleChoiceField(choices = get_field_groups(archive_schema.allowed_sort_attributes, exclude=["id"]), 
                                            required=False)
    

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['result_fields'].widget.attrs.update(size="10")
        self.fields['sort_fields'].widget.attrs.update(size="10")

    def _validate_page_url(self, page_url):
        # We dont' do much validation because that's the REST API's and lick_archive_client's job, but we do verify the
        # URL can be parsed.
        try:
            parsed_url = urllib.parse.urlparse(page_url,allow_fragments=False)
            query_string = parsed_url.query
            query_dict = urllib.parse.parse_qs(query_string, strict_parsing=True)
            return query_dict
        except ValueError as e:
            logger.error(f"Invalid page URL: {e}", exc_info=True)
            self.add_error(None, "Failed to navigate to page")

        return None

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

        if "next" in self.data:
            cleaned_data["page"] = "Next"
            cleaned_data["page_params"] = self._validate_page_url(self.data["next"])            
        elif "previous" in self.data:
            cleaned_data["page"] = "Previous"
            cleaned_data["page_params"] = self._validate_page_url(self.data["previous"])
        else:
            cleaned_data["page"] = "initial"
            cleaned_data["page_params"] = None

        return cleaned_data

archive_client = LickArchiveClient(f"{settings.LICK_ARCHIVE_API_URL}", 1, 30, 5)


def get_user_facing_result_fields(fields):
    """ Convert API result fields to user facing names, and sort them in a 
    consistent manner.

    Args:
        fields (list of str): Validated list of sort fields using API names
    Return:
        list of str: Sorted list of fields with user facing names.
    """
    # Common field ordering
    field_ordering = ["filename", "telescope", "instrument", "frame_type", "obs_date", "exptime", "ra", "dec", "airmass", "object", "program", "observer", "header"]

    common_fields = []
    instrument_fields = {}

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
    user_fields = [f[1].user_name for f in common_fields]
    # Sort instrument fields by instrument name, then by user facing field name
    for instrument in sorted(instrument_fields.keys()):        
        instrument_fields[instrument].sort(key = lambda x: x[1].user_name)
        api_fields +=  [f[0] for f in instrument_fields[instrument]]
        user_fields += [f[1].user_name for f in instrument_fields[instrument]]

    return user_fields, api_fields

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
                    row.append(value.isoformat(timespec='seconds'))
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
    context = {'result_count': None,
               'result_fields': [],
               'result_list': None,
               'archive_url': settings.LICK_ARCHIVE_FRONTEND_URL,
               'previous_link': "",
               'next_link': ""}

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
            
            query_page = form.cleaned_data["page"]
            if query_page == "initial":
                # For the "initial" query (or query submitted with submit button), use the form fields for the query values
                logger.info("Running query")

                # Run the appropriate query type
                query_type = form.cleaned_data["which_query"]
                query_operator = form.cleaned_data[query_type]["operator"]
                query_value = form.cleaned_data[query_type]["value"]
                count_query = form.cleaned_data["count"] == "yes"
                prefix = None


                # We can't use "object" for the form field because it conflicts with the python "object"
                # type. So we use object_name and rename it here.  The other form field's are named after
                # the query field sent in the API to the archive
                if query_type == "object_name":
                    query_field = "object"
                else:
                    query_field = query_type

                if query_type=='date':
                    if query_operator == "exact":
                        # The query form always has a list of values even if only one is needed
                        query_value = query_value[0]
                    else:
                        # The API distinguishes between a single date and date range
                        # The list of values in the form is correct for a range
                        query_field = "date_range"                        

                # Set prefix for the string fields
                if query_field == "filename" or query_field == "object":
                    prefix = query_operator == "prefix"

                try:
                    logger.info(f"Query Field: {query_field}")
                    logger.info(f"Query Value: {query_value}")
                    logger.info(f"Count: {count_query}")
                    logger.info(f"Results: {result_fields}")
                    logger.info(f"Sort: {sort}")
                    logger.info(f"Prefix: {prefix}")

                    result,prev_page, next_page = archive_client.query(field=query_field,
                                                                        value = query_value,
                                                                        prefix = prefix,
                                                                        count= count_query,
                                                                        results = result_fields,
                                                                        sort = sort)

                except Exception as e:
                    logger.error(f"Exception querying archive {e}", exc_info=True)
                    form.add_error(None,"Failed to run query against archive.")
            else:
                # When navigating to a page, use the URL from the link provided by the form button, which came from
                # REST API on the initial query
                page_params = form.cleaned_data['page_params']
                count_query = False
                logger.info(f"Going to {query_page} page link: {page_params}")
                try:
                    result,prev_page, next_page = archive_client.query_page(page_params)
                except Exception as e:
                    logger.error(f"Exception querying page url {page_params} {e}", exc_info=True)
                    form.add_error(None, f"Failed to retrieve {query_page} page.")

            # Process the 
            if len(form.errors) == 0:
                if count_query:
                    context['result_count'] = result
                else:

                    # Set the previous/next page links
                    if prev_page is None:
                        context['previous_link'] = format_html('<button name="previous" disabled form="archive_query">Previous Page</button>')
                    else:
                        context['previous_link'] = format_html('<button name="previous" value="{}" form="archive_query">Previous Page</button>', prev_page)

                    if next_page is None:
                        context['next_link'] = format_html('<button name="next" disabled form="archive_query">Next Page</button>')
                    else:
                        context['next_link'] = format_html('<button name="next" value="{}" form="archive_query">Next Page</button>', next_page)

                    # Sort the result fields in the order they should appear to the user, and get the user
                    # facing names
                    user_fields, api_fields = get_user_facing_result_fields(result_fields)

                    # Post process results
                    try:
                        context['result_list'] = process_results(api_fields, result)
                        context['result_fields'] = user_fields
                    except Exception as e:
                        form.add_error(None, "Failed to process results from archive.")

        for key in form.errors:
            logger.error(f"Form error {key} : '{form.errors[key]}")
    else:
        form = QueryForm()

    context['form'] = form
    return render(request, 'frontend/index.html', context)

