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
    ]
}