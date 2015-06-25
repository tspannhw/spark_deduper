# Spark-based entity resolution engine

## Installation

Clone the repo.

Install Spark 1.3 o 1..4

cd to the spark_deduper directory

```
$ cd spark_deduper
```

Add the src directory to PYTHONPATH

```
$ export PYTHONPATH=$PYTHONPATH:$(pwd)
```

## Tests

### Unit tests

```
$ python -m test.unit_tests
```

### Integration test

```
$ spark-submit test/integration_test.py
```

## Usage


### Settings

#### Data parsing

#### Deduper 

#### String distance functions

#### Predicate functions

### Running the deduper

```
$ spark-submit deduper/main.py -s conf/virgin_settings.yaml
```

## TODO

