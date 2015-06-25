# Must be run from a spark executable (test/spark-standalone/bin/spark-submit)
import unittest
import yaml

from deduper.main import main
import deduper.utils as utils

# Get the settings + sanity check
TEST_SETTINGS_PATH = 'test/test_settings.yaml'
with open(TEST_SETTINGS_PATH, 'r') as f:
    settings = yaml.load(f)
utils.settings_sanity_check(settings)

class IntegrationTest(unittest.TestCase):
    def test_main_script(self):
        # Run the main script
        main(settings)
        
if __name__ == '__main__':
    unittest.main()