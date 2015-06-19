### Installation

## Clone the repo.

## Install Spark 1.3 o 1..4

## cd to the spark_deduper directory
$ cd spark_deduper

# Add the src directory to PYTHONPATH
$ export PYTHONPATH=$PYTHONPATH:$(pwd)

### Tests

# Unit tests

$ python -m test.unit_tests

# Integration test

Run the integration test, which runs the main script
$ spark-submit test/integration_test.py