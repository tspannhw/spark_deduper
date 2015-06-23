import os


# Build the settings dictionnary
settings = {

    # ***** Parsing **********
    'LOCAL_DATA_PATH' : '../test_data/fake_passengers40000.txt',
    'HEADER_LOCAL_DATA_PATH' : '../test_data/fake_passengers40000.header',
    'SEPARATOR' : '\t',
    'DATE_FIELDS' : [
        'ElevateSince',
        'PNRCreateDate',
    ],
    'RANDOM_SEED' : 42,

    # ****** Blocking *******
    'PREDICATE_FUNCTIONS' : [
        {
            'predicate_type' : 'FirstChars',
            'base_key' : 'NameFirst',
            'predicate_value' : 3,
        },
    ],

    # ***** ML **********
    'TEST_RELATIVE_SIZE' : 0.3,
    'MIN_TRUE_MATCHES_FOR_RECALL_CALCULATION' : 20,

    # ********* Deduping **********
    'DEDUPER_FIELDS' : [
        {'name' : 'NameFirst', 'type' : 'String'},
        {'name' : 'NameLast', 'type' : 'String'},
        {'name' : 'EMailAddress', 'type' : 'Exact'},
        {'name' : 'PhoneNumber', 'type' : 'String'},
        {'name' : 'EmergencyPhoneNumber', 'type' : 'String'},
        {'name' : 'EmergencyContactName', 'type' : 'String'},
        {'name' : 'NameInAddr', 'type' : 'String'},
        {'name' : 'Address', 'type' : 'String'},
        {'name' : 'ApptNumber', 'type' : 'String'},
        {'name' : 'City', 'type' : 'String'},
        {'name' : 'ZipCode', 'type' : 'String'},
    ],
    'DEDUPER_GROUND_TRUTH_FIELD' : 'FrequentTravelerNbr',
}

# Sanity check
assert os.path.isfile(settings['LOCAL_DATA_PATH'])
assert os.path.isfile(settings['HEADER_LOCAL_DATA_PATH'])
assert all([p['predicate_type'] in ['FirstChars'] for p in settings['PREDICATE_FUNCTIONS']])
assert all([f_name != 'PredicateKey' for f_name in [f['name'] for f in settings['DEDUPER_FIELDS']] + [settings['DEDUPER_GROUND_TRUTH_FIELD']]])