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

Copy the `conf/settings_template.yaml` file to use as your settings file.

#### Input training data

The input data should be in the form of a csv file, with any kind of separator. The data can be split in many such files, as long as they live in the same directory.

The header line must be in a separate file that contains only one line where the field names are separated by the same separator than in the data file(s).

Specify the data file or directory in the settings file under `LOCAL_DATA_PATH`, the header file under `HEADER_LOCAL_DATA_PATH` and the separator under `SEPARATOR	`.

#### Ground truth field

To train the model, the data must already be deduped, at least for some entries (the more the better, of course).  The field specified in the `DEDUPER_GROUND_TRUTH_FIELD` setting should contain values that are identical for entries refering to the same entity.  Entries where this field is null will be filtered out.

#### String distance functions

The `DEDUPER_FIELDS` setting lists the fields that will be used to train and subsequently use the deduper.  For each field that you with to use, you must provide a `name` (the name must match the field names in the header file) and a `type`.

To this date `type` can only one of String and Exact.  If a field is of type String, the `difflib.SequenceMatcher` function will be used to measure the string distance between two values for this field. This function retruns a value between 0 and 1 depending on the similarity of two strings (see [here](http://epydoc.sourceforge.net/stdlib/difflib.SequenceMatcher-class.html)).  If a field is of type Exact, the value 0 or 1 will be used as the string distance, depending on whether the values are identical or not.

#### Predicate functions

The `PREDICATE_FUNCTIONS` setting should contain a list of the predicate functions to try in order to achieve the best results. At this point, only one predicate function was implemented and is defined with `predicate_type` : FirstChars.  This function will extract the first characters (how many characters is given by `predicate_value`) of a given field (given by `base_key`) and convert them to lowercase.

#### Other settings

To evaluate the performance of the deduper, a fraction of the unique values of the field defined in `DEDUPER_GROUND_TRUTH_FIELD` will be hold out during the training phase.  Subsequently, when the model is evaluated on the test data, the deduper will generate random pairs from those entries (to avoid generating all pairs as the number of them grows exponentially), most of which will not be duplicate entries.  To make sure the precision and recall values are precise enough, the `MIN_TRUE_MATCHES_FOR_EVALUATION` setting defines a minimum number of true matches that must appear in the evaluation data before proceding.

### Running the deduper

Run the main script and specify the location of your settings file with the -s arguemt:

```
$ spark-submit deduper/main.py -s conf/settings.yaml
```

## TODO

* Add a Spark configuration file
* In the evaluation step, compute all 4 values of the confusion matrix, not just precision and recall
* Add a way to use the deduper with unlabeled data, including an additional step to cluster entities together to avoid having impossible mathces (ex.: A matches B and B matches C but A doesn't match C)
* Allow the non-use of predicate function and train on all possible pairs instead
* Add predicate functions : hashing of a string concatenating many fields, distances between integers and dates, common n-grams, locality sensitive hashing, etc.
* Logging with log4j