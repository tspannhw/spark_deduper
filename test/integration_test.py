# Must be run from a spark executable (test/spark-standalone/bin/spark-submit)
import unittest

from deduper.main import main

class IntegrationTest(unittest.TestCase):
    def test_main_script(self):
        # Run the main script
        main()
        
if __name__ == '__main__':
    unittest.main()