# Build the metadata config javascript file the frontend needs to be able to talk with the backend.

import argparse
from pathlib import Path
from lick_archive.metadata.data_dictionary import data_dictionary, field_units, api_capabilities, Category
import json
import sys

def get_parser():
    """
    Parse build_metadata_config command line arguments with argparse.
    """
    parser = argparse.ArgumentParser(description="Build the metadata_config.js file used when building the frontend Javascript to allow the frontend to understand the archive's metadata.")
    parser.add_argument("output", type=Path, help='Where to output the metadata_config.js file.')

    return parser

def build_field_dict(field):
    result = dict()
    result['human_name'] = field['human_name']
    result['units'] = field_units.get(field['db_name'],'')
    result['category'] = field['category'].value
    result['query_valid'] = field['db_name'] in api_capabilities['query']
    result['result_valid'] = field['db_name'] in api_capabilities['result']
    result['sort_valid'] = field['db_name'] in api_capabilities['sort']
    return result

def main(args):

    categories = [cat.value for cat in Category]
    all_fields = {field['db_name']: build_field_dict(field) for field in data_dictionary}
    # filter out fields that aren't visible, i.e. aren't queryable, resultable, or sortable
    archive_fields = {field[0]: field[1] for field in all_fields.items() if field[1]['query_valid'] or field[1]['result_valid'] or field[1]['sort_valid']}

    data_dictionary_wrapper = {"archiveCategories": categories,
                               "archiveFields":     archive_fields,
                               }


    # Group into categories
    results_js = json.dumps(data_dictionary_wrapper,indent=4)
    with open(args.output, "w") as f:
        f.write(results_js)


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    sys.exit(main(args))
