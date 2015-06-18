### Installation

## Clone the repo.

## cd to the spark_deduper directory
$ cd spark_deduper

# Add the src directory to PYTHONPATH
$ export PYTHONPATH=$PYTHONPATH:$(pwd)

### Tests

# Unit tests

$ python -m test.unit_tests

# Integration test

Use the standalone spark executable from the test dir to run the main script:
$ test/spark-standalone/bin/spark-submit test/integration_test.py