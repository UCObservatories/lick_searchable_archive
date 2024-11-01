import logging

logger = logging.getLogger(__name__)

from django import forms
from django.utils.html import format_html

from lick_archive.metadata.data_dictionary import Category, api_capabilities, supported_instruments
from lick_archive.config.archive_config import ArchiveConfigFile

lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

from .fields import QueryWithOperator, AngleField
from .widgets import PageNavigationWidget

def get_field_groups(fields):
    
    # Exclude "id" from the UI, as it's intended for backend use only
    desired_fields = fields['db_name'] != 'id'
    
    groups = fields[desired_fields].group_by('category').groups

    group_dict = {g[0]['category'].value: tuple(tuple(row) for row in g['db_name','human_name']) for g in groups}

    # This list establishes the order groups appear on the front end, 
    # We put the default "common" group first, followed by the other groups in alphabetical order
    group_names = [Category.COMMON.value] + sorted([c.value for c in Category if c != Category.COMMON])

    return [(group_name, group_dict[group_name]) for group_name in group_names]

UI_ALLOWED_SORT = get_field_groups(api_capabilities['sort'])
UI_NOT_ALLOWED_RESULT = ["download_link"]
UI_ALLOWED_RESULT =  get_field_groups(api_capabilities['result'][[False if db_name in UI_NOT_ALLOWED_RESULT else True for db_name in api_capabilities['result']['db_name']]])
DEFAULT_SORT = "obs_date"
DEFAULT_RESULTS = ["filename", "instrument", "frame_type", "object", "exptime", "obs_date"]





class QueryForm(forms.Form):
    which_query = forms.ChoiceField(choices=[("filename",""), ("object_name",""), ("date",""), ("coord","")], initial="object_name", required=True, widget=forms.RadioSelect)
    filename =   QueryWithOperator(label="Path and Filename", operators=[("exact", "="), ("prefix", "starts with")],
                                   class_prefix="search_terms_",
                                   fields=[forms.CharField(max_length=1024, strip=True, empty_value="", required=False)],
                                   initial={"operator": "exact", "value": ""},  help_text=format_html('{}<p>{}','Example:', '"2014-04/08/AO/m140409_0040.fits"'),required=False)
    object_name = QueryWithOperator(label="Object Name", operators= [("exact", "="), ("prefix", "starts with"), ("contains", "contains")],
                                    modifier="Match Case",
                                    class_prefix="search_terms_",
                                    fields=[forms.CharField(max_length=80, empty_value="", strip=True, required=False)],
                                    initial={"operator": "exact", "value": ""},  help_text='Example: "K6021275"', required=False) 
    date =   QueryWithOperator(label="Observation Date", operators= [("exact", "="), ("range", "between")],
                               class_prefix="search_terms_",
                               fields=[forms.DateField(required=False),forms.DateField(required=False)],
                               initial={"operator": "exact", "value": None}, 
                               help_text=format_html('{}<p>{}<p>{}','Example: "2006-08-17".', 'All dates are noon to noon PST (UTC-8).', 'Date ranges are inclusive.'),
                               required=False)
    coord  = QueryWithOperator(label="Location", operators= [AngleField(label="Radius", required=False)], 
                               class_prefix="search_terms_",
                               fields=[AngleField(label="RA", required=False),
                                       AngleField(label="DEC",required=False)],                               
                               initial={"operator": None, "value": None},  
                               help_text=format_html('{}<p>{}<ul><li>{}</li><li>{}</li></ul>',
                                                     'Accepts hms/dms or decimal degrees. Radius assumed to be arcseconds if not specified.',
                                                     'Examples:',
                                                     'All of these are the same value'
                                                     '"radius 36s ra 6:12:19.5 dec -40:30:12.3"',
                                                     '"radius 36s ra 6 12 19.5 dec -40 30 12.3"',
                                                     '"radius 36s ra 6h12m19.5s dec -40d30m12.3s"',
                                                     '"radius .6m ra 6.20541666h dec -40.5034d"', 
                                                     '"radius 36 ra 93.0812 dec -40.5034"'), 
                               required=False)
    count = forms.ChoiceField(label="", choices=[("yes","Return only a count of matching files."), ("no","Return information about matching files.")], 
                              initial="no", required=True, widget=forms.RadioSelect(attrs = {"class": "search_fields_radio"}))
    sort_fields = forms.ChoiceField(label="Sorting", initial = DEFAULT_SORT, choices = UI_ALLOWED_SORT, 
                                    required=False)
    sort_dir = forms.ChoiceField(choices=[("+", "Low to high (Ascending)"), ("-", "High to low (Descending)")], initial="+", required=False)
    result_fields = forms.MultipleChoiceField(initial = DEFAULT_RESULTS,
                                              choices = UI_ALLOWED_RESULT,        
                                              required=False,
                                              widget=forms.SelectMultiple(attrs={"class": "search_fields_input_big"}))
    instruments = forms.MultipleChoiceField(initial = [x.name for x in supported_instruments], choices=[(x.name, x.value) for x in supported_instruments], widget=forms.CheckboxSelectMultiple(attrs={"class": "search_instr_check"}), required=False)
    page=forms.IntegerField(min_value=1,  initial=1,widget=PageNavigationWidget(attrs={"class": "page_nav"},form_id="archive_query", max_controls=12))
    page_size=forms.IntegerField(min_value=1, max_value=1000, initial=50, required=True, widget=forms.NumberInput(attrs={"class": "search_fields_input_small"}))
    coord_format=forms.ChoiceField(initial="sexigesimal", required=True, choices=[("sexigesimal", "sexigesimal"), ("decimal", "decimal degrees")])    

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['result_fields'].widget.attrs.update(size="10")

    def clean(self):
        cleaned_data = super().clean()
        if cleaned_data is None:
            cleaned_data = self.cleaned_data
    
        # Verify the appropriate query type has a value populated        
        query_type = cleaned_data.get("which_query", '')
        if query_type == '':
            self.add_error('which_query', "No search term selected.")
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
            elif query_type == "coord":
                logger.debug(f"coord query_value: {query_value} operator: {query_operator}")
                if query_value is None or not isinstance(query_value, tuple):
                    self.add_error("coord", "Cannot query by empty location.")
                else:
                    if query_operator is None or len(query_operator) == 0:
                        # We can use a default value for the radius
                        query_operator = lick_archive_config.query.default_search_radius

                    if query_value[1] is None or len(query_value[1]) == 0:
                        self.add_error("coord", f"RA and DEC are required for location query.")
                    cleaned_data["coord"]["value"] = (query_value[0], query_value[1], query_operator)

            else:
                self.add_error("which_query", f"Unknown query type {query_type}.")


        return cleaned_data

