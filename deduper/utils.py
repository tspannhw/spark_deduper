from datetime import datetime

# Mllib
from pyspark.mllib.regression import LabeledPoint

from deduper.settings import settings

def get_headers():
    with open(settings['HEADER_LOCAL_DATA_PATH'], 'r') as f:
        header_line = f.readlines()[0]
    headers = header_line[:-1].split(settings['SEPARATOR'])
    return headers

def convert_dates(line_dict):
    return dict([(k, v) if k not in settings['DATE_FIELDS'] or v == None else (k, datetime.strptime(v, "%Y-%m-%d").date()) for k, v in line_dict.items()])

def add_predicate_key(data_dict, predicate_key_name, base_key, predicate_type, predicate_value):
    # Assertions
    assert predicate_type in ['FirstChars']
    assert base_key in data_dict.keys()

    if predicate_type == 'FirstChars':
        data_dict[predicate_key_name] = data_dict[base_key][:predicate_value].lower()

    return data_dict

def generate_pairs(dict_list):
    # TEST : IMPLEMENT ME
    return [LabeledPoint(True, [1, 2, 3]), LabeledPoint(False, [4, 5, 6])]