# Build the metadata config javascript file the frontend needs to be able to talk with the backend.

import argparse
from pathlib import Path
from lick_archive.metadata.data_dictionary import data_dictionary, Category, LargeInt, LargeStr, api_capabilities, supported_instruments, Instrument
import sys
from datetime import date, datetime
import enum


def get_parser():
    """
    Parse build_metadata_config command line arguments with argparse.
    """
    parser = argparse.ArgumentParser(description="Build the metadata_config.js file used when building the frontend Javascript to allow the frontend to understand the archive's metadata.")
    parser.add_argument("output", type=Path, help='Where to output the metadata_config.js file.')

    return parser

def main(args):
    with open(args.output, "w") as outfile:
        for cat in Category:
            write_subheader(outfile, cat.value)
            cat_fields = data_dictionary['category']==cat
            if len(data_dictionary[cat_fields]) > 0:
                write_fields(outfile, data_dictionary[cat_fields])


def write_subheader(outfile, name):
    outfile.write("\n" + name + "\n")
    outfile.write("-"*len(name) + "\n\n")

def write_fields(outfile, fields):
    user_facing_fields = fields["human_name"] != ''
    table_copy = fields[user_facing_fields].copy()
    table_copy['type'] = [type_to_string(x) for x in table_copy['type']]
    table_copy['human_name'].info.name = "Name"
    table_copy['type'].info.name = "Type"
    table_copy['description'].info.name = "Description"
    table_copy["Name","Type","Description"].write(outfile,format="ascii.rst")
    """
    for field in fields:
        if field["db_name"] not in api_capabilities["result"]["db_name"]:
            # Don't include internal fields
            continue
        outfile.write(field["human_name"] + "\n")
        outfile.write("^"*len(field["human_name"]) + "\n")
        outfile.write(":Description: ")
        outfile.write(field["description"] + "\n")
        outfile.write(":Type:        ")
        outfile.write(type_to_string(field["type"]) + "\n")
        outfile.write(":API Name:    ")
        outfile.write(f"``{field['db_name']}``\n\n")
    """
def type_to_string(type_object):
    if type_object == int or type_object == LargeInt:
        return "Integer"
    elif type_object == float:
        return "Floating Point"
    elif type_object == date:
        return "Date (ISO 8601)"
    elif type_object == datetime:
        return "Date and Time (ISO 8601)"
    elif type_object == LargeStr:
        return "ASCII Text File"
    else:
        result = "String"
        if issubclass(type_object, enum.Enum):
            # For enums use the list representation of their values without the square braces

            if type_object == Instrument:
                # Only report supported instruments
                allowed_values = ", ".join([f"``{x.value}``" for x in supported_instruments])
            else:
                allowed_values = ", ".join([f"``{x.value}``" for x in type_object])
            result += " - One of " + allowed_values
        return result
 

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    sys.exit(main(args))
