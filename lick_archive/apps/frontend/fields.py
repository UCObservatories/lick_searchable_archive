"""Custom Django form fields and widgets"""

import logging

logger = logging.getLogger(__name__)

from django import forms

from django.core.exceptions import ValidationError

from astropy.coordinates import Angle
import astropy.units

from .widgets import OperatorWidget

class AngleField(forms.CharField):
    """Custom django form field to parse and validate an angular distance.
    
    Args:
        default_unit (str or :obj:`astropy.units.BaseUnit`): The default unit to use if none is specified.
    """
    def __init__(self, default_unit, *args, **kwargs):
        self.default_unit = default_unit

        super().__init__(*args, **kwargs)

    def clean(self, value):
        """Validate the text in a CharField is an angular distance.
        
        Return: (str): The string representation of the angular distance, with units.
        """
        # Let the parent class do its validation
        value = super().clean(value)
        if value is None or len(value.strip()) == 0:
            return None
        angle, angle_text = parse_and_validate_angle(value, default_unit=self.default_unit, field=self.label)
        return angle_text

    
def parse_and_validate_angle(value, default_unit, field="Angle"):
    """Validate angle values and return an Astropy Angle object from user input.

    Args:
        value (str):                                         The incomming value to parse.
        default_unit (str or :obj:`astropy.units.UnitBase`): The default units to use if none are given.
        field (str):                                         Field name to use in validation error messages.

    Returns (:obj:`astropy.coordinates.Angle`): The parsed Angle object.
            (str):                              The text used to crate the angle object. This will be different from value
                                                if default_unit was added.

    Raises (:obj:`django.core.exceptions.ValidationError`): A Django validation error, with code either "invalid" or "required"
        
    """

    # Messages for an invalid angle, and an angle with invalid units
    messages=["Enter a valid {}.", "Enter valid units for {}."]
    

    # Make sure it's not empty
    if value is None or len(value) == 0:
        logger.error(f"{field} is empty.")
        raise ValidationError(f"Required {field} is empty.", code="required")

    # Look for invalid characters
    for c in value:
        if c not in " 0123456789:.+-hdms'\"":
            logger.error(f"{field} value {value} has invalid character {c}.")
            raise ValidationError(message=messages[0].format(field), code="invalid")

    # Now let astropy validate it
    try:
        return Angle(value), value
    except astropy.units.UnitsError as e:
        # If astropy didn't like the units, try the default
        if default_unit is not None:
            try:
                updated_angle = value + str(default_unit)
                return Angle(updated_angle), updated_angle
            except Exception as e:
                logger.error(f"{field} value {value} could not be parsed with default units.", exc_info=True)
                raise ValidationError(message=messages[0].format(field), code="invalid")
        else:
            logger.error(f"{field} value {value} has invalid units: {e}")
            raise ValidationError(message=messages[1].format(field), code="invalid")
    except Exception as e:
        logger.error(f"{field} value {value} could not be parsed by astropy.", exc_info=True)
        raise ValidationError(message=messages[1].format(field), code="invalid")


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
