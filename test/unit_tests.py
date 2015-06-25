import sys
import unittest
import datetime
import yaml

import deduper.utils as utils

# Get the settings + sanity check
TEST_SETTINGS_PATH = 'test/test_settings.yaml'
with open(TEST_SETTINGS_PATH, 'r') as f:
    settings = yaml.load(f)
utils.settings_sanity_check(settings)

class DeduperTest(unittest.TestCase):

    def test_add_predicate_key(self):
        d1 = {'ElevateSince': None,
             'City': None,
             'ApptNumber': None,
             'NameLast': u'qreNBCkwnF',
             'NameFirst': u'aZQlYFdhDS',
             'PNRCreateDate': datetime.date(2013, 2, 20),
             'EmergencyPhoneNumber': None,
             'ElevateMember': u'0',
             'TravelerOrigTerminal': u'jSbwXBFOCu',
             'FrequentTravelerNbr': None,
             'ZipCode': u'33934',
             'EMailAddress': None,
             'PNRLocatorID': u'AABXDA',
             'PhoneNumber': None,
             'Address': u'GCRtanMzuD',
             'EmergencyContactName': None,
             'NameInAddr': u'aZQlYFdhDS qreNBCkwnF',
        }
        d1_pred = utils.add_predicate_key(d1, 
            base_key = 'NameFirst',
            predicate_type = 'FirstChars',
            predicate_value = 3,
        )

        assert d1_pred['PredicateKey'] == 'azq'

    def test_records_are_matches(self):
        d1 = {'ElevateSince': datetime.date(2014, 1, 27), 'City': None, 'ApptNumber': None, 'NameFirst': u'EJActegNHR', 'PNRCreateDate': datetime.date(2013, 8, 4), 'EmergencyPhoneNumber': None, 'PredicateKey': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'ryGEuhvbNP', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'BGTJPD', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw', 'Address': u'aJxpyTrwqS', 'NameLast': u'obHGaFEtNQ', 'NameInAddr': None, 'PhoneNumber': u'5403578131'}
        d2 = {'ElevateSince': datetime.date(2014, 1, 27), 'City': u'UlEgqaSwNp', 'ApptNumber': None, 'NameFirst': u'EJActegNHR', 'PNRCreateDate': datetime.date(2014, 11, 18), 'EmergencyPhoneNumber': None, 'PredicateKey': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'ryGEuhvbNP', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'GHGNYF', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw', 'Address': None, 'NameLast': u'obHGaFEtNQ', 'NameInAddr': None, 'PhoneNumber': None}

        d3 = {'ElevateSince': datetime.date(2014, 1, 27), 'City': u'UlEgqaSwNp City', 'ApptNumber': 'Suite 1', 'NameFirst': u'EJActegNHR Jr.', 'PNRCreateDate': datetime.date(2014, 10, 11), 'EmergencyPhoneNumber': None, 'PredicateKey': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'San Francisco', 'FrequentTravelerNbr': u'13949200996', 'ZipCode': None, 'PNRLocatorID': u'AAAAAO', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw@gmail.com', 'Address': None, 'NameLast': u'obHGaFEtNQ Mr.', 'NameInAddr': None, 'PhoneNumber': None}

        # Those 2 have the same FT number
        assert utils.records_are_matches(d1, d2, settings['DEDUPER_GROUND_TRUTH_FIELD'])
        # Those 2 don't
        assert not utils.records_are_matches(d1, d3, settings['DEDUPER_GROUND_TRUTH_FIELD'])

    def test_dict_pair_2_distance_list(self):
        d1 = {'ElevateSince': datetime.date(2014, 1, 27), 'City': None, 'ApptNumber': None, 'NameFirst': u'EJActegNHR', 'PNRCreateDate': datetime.date(2013, 8, 4), 'EmergencyPhoneNumber': None, 'PredicateKey': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'ryGEuhvbNP', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'BGTJPD', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw', 'Address': u'aJxpyTrwqS', 'NameLast': u'obHGaFEtNQ', 'NameInAddr': None, 'PhoneNumber': u'5403578131'}
        d2 = {'ElevateSince': datetime.date(2014, 1, 27), 'City': u'UlEgqaSwNp', 'ApptNumber': None, 'NameFirst': u'EJActegNHR', 'PNRCreateDate': datetime.date(2014, 11, 18), 'EmergencyPhoneNumber': None, 'PredicateKey': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'ryGEuhvbNP', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'GHGNYF', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw', 'Address': None, 'NameLast': u'obHGaFEtNQ', 'NameInAddr': None, 'PhoneNumber': None}

        distances = utils.dict_pair_2_distance_list(d1, d2, settings['DEDUPER_FIELDS'])
        
        # Should have the same length than setting['DEDUPER_FILEDS']
        assert len(distances) == len(settings['DEDUPER_FIELDS'])

        # Name first is the same so it should have a distance of 1
        assert distances[[n['name'] for n in settings['DEDUPER_FIELDS']].index('NameFirst')] == 1

        # City is None for one of the so the distance should be None
        assert distances[[n['name'] for n in settings['DEDUPER_FIELDS']].index('City')] == None

    def test_generate_pairs(self):
        dict_list = [
            {'ElevateSince': datetime.date(2014, 1, 27), 'City': None, 'ApptNumber': None, 'NameFirst': u'EJActegNHR', 'PNRCreateDate': datetime.date(2013, 8, 4), 'EmergencyPhoneNumber': None, 'PredicateKey': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'ryGEuhvbNP', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'BGTJPD', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw', 'Address': u'aJxpyTrwqS', 'NameLast': u'obHGaFEtNQ', 'NameInAddr': None, 'PhoneNumber': u'5403578131'}, 
            {'ElevateSince': datetime.date(2014, 1, 27), 'City': u'UlEgqaSwNp', 'ApptNumber': None, 'NameFirst': u'EJActegNHR', 'PNRCreateDate': datetime.date(2014, 11, 18), 'EmergencyPhoneNumber': None, 'PredicateKey': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'ryGEuhvbNP', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'GHGNYF', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw', 'Address': None, 'NameLast': u'obHGaFEtNQ', 'NameInAddr': None, 'PhoneNumber': None}, 
            {'ElevateSince': datetime.date(2014, 1, 27), 'City': u'UlEgqaSwNp City', 'ApptNumber': 'Suite 1', 'NameFirst': u'EJActegNHR Jr.', 'PNRCreateDate': datetime.date(2014, 10, 11), 'EmergencyPhoneNumber': None, 'PredicateKey': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'San Francisco', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'AAAAAO', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw@gmail.com', 'Address': None, 'NameLast': u'obHGaFEtNQ Mr.', 'NameInAddr': None, 'PhoneNumber': None}
        ]
        predicate = 'eja'
        list_of_pairs = utils.generate_pairs((predicate, dict_list))

        # The list should be of size 3 (AB, AC and BC)
        assert len(list_of_pairs)== 3
        # Each element should be a tuple of 2 dictionaries
        assert all([type(t) == tuple for t in list_of_pairs])
        assert all([type(t[0]) == dict and type(t[1]) == dict for t in list_of_pairs])

    def test_records_in_same_block(self):
        d1 = {'ElevateSince': datetime.date(2014, 1, 27), 'City': None, 'ApptNumber': None, 'NameFirst': u'EJActegNHR', 'PNRCreateDate': datetime.date(2013, 8, 4), 'EmergencyPhoneNumber': None, 'PredicateKey': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'ryGEuhvbNP', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'BGTJPD', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw', 'Address': u'aJxpyTrwqS', 'NameLast': u'obHGaFEtNQ', 'NameInAddr': None, 'PhoneNumber': u'5403578131'}
        d2 = {'ElevateSince': datetime.date(2014, 1, 27), 'City': u'UlEgqaSwNp', 'ApptNumber': None, 'NameFirst': u'EJActegNHR', 'PNRCreateDate': datetime.date(2014, 11, 18), 'EmergencyPhoneNumber': None, 'PredicateKey': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'ryGEuhvbNP', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'GHGNYF', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw', 'Address': None, 'NameLast': u'obHGaFEtNQ', 'NameInAddr': None, 'PhoneNumber': None}

        d3 = {'ElevateSince': datetime.date(2014, 1, 27), 'City': u'UlEgqaSwNp', 'ApptNumber': None, 'NameFirst': u'EJActegNHR', 'PNRCreateDate': datetime.date(2014, 11, 18), 'EmergencyPhoneNumber': None, 'PredicateKey': u'jjj', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'ryGEuhvbNP', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'GHGNYF', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw', 'Address': None, 'NameLast': u'obHGaFEtNQ', 'NameInAddr': None, 'PhoneNumber': None}


        assert utils.records_in_same_block(d1, d2)
        assert not utils.records_in_same_block(d1, d3)

if __name__ == '__main__':
    unittest.main(argv=[sys.argv[0]])