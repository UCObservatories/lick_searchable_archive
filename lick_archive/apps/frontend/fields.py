"""Custom Django form fields and widgets"""

import re
import logging

logger = logging.getLogger(__name__)

from django import forms

from django.core.exceptions import ValidationError

from astropy.coordinates import Angle
import astropy.units

from .widgets import OperatorWidget

class AngleField(forms.CharField):
    """Custom django form field to parse and validate an angular distance.   
    """
    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

    def clean(self, value):
        """Validate the text in a CharField is an angular distance.
        
        Return: (str): The string representation of the angular distance, with units.
        """
        # Let the parent class do its validation
        value = super().clean(value)
        if value is None or len(value.strip()) == 0:
            return None
        angle_text = parse_and_validate_angle(value.strip(), field=self.label)
        return angle_text


def parse_and_validate_angle(value, field="Angle"):
    """Validate angle values and return an Astropy Angle object from user input.

    Args:
        value (str):                                         The incomming value to parse.
        field (str):                                         Field name to use in validation error messages.

    Returns: 
            str:                              Text to represent the angle/ This will be different from value
                                              if whitspace was trimmed.

    Raises (:obj:`django.core.exceptions.ValidationError`): A Django validation error, with code either "invalid" or "required"
        
    """  

    # Make sure it's not empty
    if value is None or len(value) == 0:
        logger.error(f"{field} is empty.")
        raise ValidationError(f"Required {field} is empty.", code="required")

    # Look for invalid characters
    for c in value:
        if c not in " 0123456789:.+-HDMShdms":
            logger.error(f"{field} value {value} has invalid character {c}.")
            raise ValidationError(message=f"Enter a valid {field}.", code="invalid")

    # Trim spaces to be a single space
    trimmed_value = " ".join(value.split())

    return trimmed_value


class QueryWithOperator(forms.MultiValueField):

    def __init__(self, operators, fields, modifier=None, names=[''], class_prefix='', **kwargs):
        error_messages = {"incomplete": "Enter an operator and value(s)"}
        self.modifier=modifier

        all_fields = []
        subwidgets = []
        if len(operators) > 0:
            if isinstance(operators[0], tuple):
                # A list of operators
                operator_field = forms.ChoiceField(choices=operators, required=False)
            else:
                # Specified operator field
                operator_field = operators[0]


            all_fields.append(operator_field)
            subwidgets.append(("operator", operator_field.label, operator_field.widget))

        if modifier is not None:
            modifier_field = forms.BooleanField(initial=False, required=False, label=modifier)
            all_fields.append(modifier_field)
            subwidgets.append(("modifier", modifier, modifier_field.widget))

        all_fields += fields
        subwidgets += [(f"value{i+1}", field.label, field.widget) for i, field in enumerate(fields)]

        logger.debug(f"all_fields: {all_fields}")

        super().__init__(fields=all_fields, require_all_fields=False, 
                         widget=OperatorWidget(subwidgets=subwidgets, class_prefix=class_prefix),
                         error_messages=error_messages, **kwargs)

    def compress(self, data_list):

        value = {"operator": None, "modifier": None, "value": None}

        data_length = len(data_list)

        if data_length >= 2:
            value["operator"] = data_list[0]    

            if self.modifier is not None:
                values_start = 2
                value["modifier"] = data_list[1]
            else:
                values_start = 1

            if len(data_list) == values_start + 1:
                value["value"] =  data_list[values_start]
            elif len(data_list) > values_start + 1:
                value["value"] = tuple(data_list[1:])
            

        return value
