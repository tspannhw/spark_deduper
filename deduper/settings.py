import os

# Globals
LOCAL_DATA_PATH = '../test_data/fake_passengers40000.txt'
HEADER_LOCAL_DATA_PATH = '../test_data/fake_passengers40000.header'
SEPARATOR = '\t'

# Sanity check
assert os.path.isfile(LOCAL_DATA_PATH)
assert os.path.isfile(HEADER_LOCAL_DATA_PATH)

# Wrap up in the settings dictionary
settings = {
    'LOCAL_DATA_PATH' : LOCAL_DATA_PATH,
    'HEADER_LOCAL_DATA_PATH' : HEADER_LOCAL_DATA_PATH,
    'SEPARATOR' : SEPARATOR,
    'DATE_FIELDS' : [
        'ElevateSince',
        'PNRCreateDate',
    ],
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