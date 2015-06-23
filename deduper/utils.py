from datetime import datetime
import itertools
import difflib

from deduper.settings import settings

def get_headers():
    with open(settings['HEADER_LOCAL_DATA_PATH'], 'r') as f:
        header_line = f.readlines()[0]
    headers = header_line[:-1].split(settings['SEPARATOR'])
    return headers

def convert_dates(line_dict):
    return dict([(k, v) if k not in settings['DATE_FIELDS'] or v == None else (k, datetime.strptime(v, "%Y-%m-%d").date()) for k, v in line_dict.items()])

def add_predicate_key(data_dict, base_key, predicate_type, predicate_value, predicate_key_name='PredicateKey'):
    if predicate_type == 'FirstChars':
        data_dict[predicate_key_name] = data_dict[base_key][:predicate_value].lower()

    return data_dict

def compute_string_distance(s1, s2):
    return difflib.SequenceMatcher(a=s1, b=s2).ratio()

def records_are_matches(d1, d2):
    # Find if it's a true match or not
    assert d1[settings['DEDUPER_GROUND_TRUTH_FIELD']] is not None and d2[settings['DEDUPER_GROUND_TRUTH_FIELD']] is not None
    return d1[settings['DEDUPER_GROUND_TRUTH_FIELD']] == d2[settings['DEDUPER_GROUND_TRUTH_FIELD']]

def dict_pair_2_distance_list(d1, d2):

    # Find the distances
    distances = []
    for field in settings['DEDUPER_FIELDS']:
        # If any of the 2 values is None, the distance is None (we'll convert into a SparseVector later.)
        if d1[field['name']] is None or d2[field['name']] is None:
            distances.append(None)
        else:
            if field['type'] == 'String':
                distances.append(compute_string_distance(str(d1[field['name']]), str(d2[field['name']])))
            elif field['type'] == 'Exact':
                distances.append(1 if d1[field['name']] == d2[field['name']] else 0)

    return distances

def generate_pairs(mapped_tuple):
    # Unpack dict_list
    predicate, dict_list = mapped_tuple

    # Initiate the list of pairs to return
    pairs = [(d1, d2) for d1, d2 in itertools.combinations(dict_list, 2)]

    return pairs

def records_in_same_block(d1, d2):
    return d1['PredicateKey'] == d2['PredicateKey']