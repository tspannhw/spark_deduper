import sys
import unittest
import datetime

import deduper.utils as utils

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

if __name__ == '__main__':
    unittest.main(argv=[sys.argv[0]])