import sys
import unittest
import datetime

import deduper.utils as utils
from deduper.settings import settings

class DeduperTest(unittest.TestCase):
    def test_convert_dates(self):
        d1 = {'ElevateSince': None,
             'City': None,
             'ApptNumber': None,
             'NameLast': u'qreNBCkwnF',
             'NameFirst': u'aZQlYFdhDS',
             'PNRCreateDate': u'2013-02-20',
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
        parsed_d1 = utils.convert_dates(d1)
        assert parsed_d1['ElevateSince'] == None
        assert parsed_d1['PNRCreateDate'] == datetime.date(2013, 2, 20)

        
        d2 = {'ElevateSince': '2012-12-12',
             'City': u'RgapzIPkWf',
             'ApptNumber': None,
             'NameLast': u'rDWoQgBpyL',
             'NameFirst': u'DIwrQFsNpG',
             'PNRCreateDate': None,
             'EmergencyPhoneNumber': None,
             'ElevateMember': u'0',
             'TravelerOrigTerminal': None,
             'FrequentTravelerNbr': u'35840833582',
             'ZipCode': u'67621',
             'EMailAddress': None,
             'PNRLocatorID': u'AABXIB',
             'PhoneNumber': None,
             'Address': u'zOKGsLYmMk',
             'EmergencyContactName': None,
             'NameInAddr': u'DIwrQFsNpG rDWoQgBpyL',
        }
        parsed_d2 = utils.convert_dates(d2)
        assert parsed_d2['ElevateSince'] == datetime.date(2012, 12, 12)
        assert parsed_d2['PNRCreateDate'] == None

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
            predicate_key_name = 'NameFirst3FirstChars',
            base_key = 'NameFirst',
            predicate_type = 'FirstChars',
            predicate_value = 3,
        )

        assert d1_pred['NameFirst3FirstChars'] == 'azq'

    def test_generate_pairs(self):
        dict_list = [
            {'ElevateSince': datetime.date(2014, 1, 27), 'City': None, 'ApptNumber': None, 'NameFirst': u'EJActegNHR', 'PNRCreateDate': datetime.date(2013, 8, 4), 'EmergencyPhoneNumber': None, 'NameFirst3FirstChars': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'ryGEuhvbNP', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'BGTJPD', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw', 'Address': u'aJxpyTrwqS', 'NameLast': u'obHGaFEtNQ', 'NameInAddr': None, 'PhoneNumber': u'5403578131'}, 
            {'ElevateSince': datetime.date(2014, 1, 27), 'City': u'UlEgqaSwNp', 'ApptNumber': None, 'NameFirst': u'EJActegNHR', 'PNRCreateDate': datetime.date(2014, 11, 18), 'EmergencyPhoneNumber': None, 'NameFirst3FirstChars': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'ryGEuhvbNP', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'GHGNYF', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw', 'Address': None, 'NameLast': u'obHGaFEtNQ', 'NameInAddr': None, 'PhoneNumber': None}, 
            {'ElevateSince': datetime.date(2014, 1, 27), 'City': u'UlEgqaSwNp City', 'ApptNumber': 'Suite 1', 'NameFirst': u'EJActegNHR Jr.', 'PNRCreateDate': datetime.date(2014, 10, 11), 'EmergencyPhoneNumber': None, 'NameFirst3FirstChars': u'eja', 'ElevateMember': u'1', 'TravelerOrigTerminal': u'San Francisco', 'FrequentTravelerNbr': u'31949200969', 'ZipCode': None, 'PNRLocatorID': u'AAAAAO', 'EmergencyContactName': None, 'EMailAddress': u'LnCUrAhSmw@gmail.com', 'Address': None, 'NameLast': u'obHGaFEtNQ Mr.', 'NameInAddr': None, 'PhoneNumber': None}
        ]
        predicate = 'eja'
        list_of_pairs = utils.generate_pairs((predicate, dict_list))
        # They all have the same FT number so they sould all have a True first value
        assert all([p[0] == True for p in list_of_pairs])
        # Thes second value should be a list of the same length as in settings DEDUPER_FIELDS
        list_length = len(settings['DEDUPER_FIELDS'])
        assert all([len(p[1]) == list_length for p in list_of_pairs])

if __name__ == '__main__':
    unittest.main(argv=[sys.argv[0]])