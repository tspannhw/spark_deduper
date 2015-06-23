import os
import random
from operator import add
from numpy import sqrt

# Spark!
from pyspark import SparkContext
# Set the absolute path of the py-files to use (relative paths don't work..)
pyFiles = [os.path.join(os.path.abspath('.'), f_name) for f_name in [
    "deduper/settings.py",
    "deduper/utils.py",
]]
sc = SparkContext("local", "Virgin Deduper", pyFiles=pyFiles)

# Import from pyfiles
from settings import settings
import utils

# mllib
from pyspark.mllib.regression import LabeledPoint, SparseVector
from pyspark.mllib.classification import LogisticRegressionWithSGD

# Globlas
N_PAIRS_TO_TEST = 100000 # Start value for generating random pairs with enough true matches

def log_line(line):
    print line
    with open(settings['LOG_FILE_PATH'], 'a') as f:
        f.writelines(line + '\n')


def main():
    def _get_rdd(headers):
        return (
            # Read the data
            sc.textFile(settings['LOCAL_DATA_PATH'])
                # Remove the warning lines
                .filter(lambda x: not x.startswith('Warning'))
                # Map into a tuple
                .map(lambda x: x.split(settings['SEPARATOR']))
                # Replace 'NULL' values by None
                .map(lambda x: [v if v != 'NULL' else None for v in x])
                # Zip into a dictionary with headers
                .map(lambda x: dict(zip(headers, x)))
                # Convert dates
                .map(lambda x: utils.convert_dates(x))
        )

    def _split_on_ground_thruth_field(data):
        unique_field_values = data.map(lambda x: x[settings['DEDUPER_GROUND_TRUTH_FIELD']]).distinct()
        train_values, test_values = unique_field_values.randomSplit([1 - settings['TEST_RELATIVE_SIZE'], settings['TEST_RELATIVE_SIZE']], seed=settings['RANDOM_SEED'])
        train_data = (
            train_values.map(lambda x: (x, None)).leftOuterJoin(
                data.map(lambda x: (x[settings['DEDUPER_GROUND_TRUTH_FIELD']], x))
            )
            .map(lambda x: x[1][1])
        )
        test_data = (
            test_values.map(lambda x: (x, None)).leftOuterJoin(
                data.map(lambda x: (x[settings['DEDUPER_GROUND_TRUTH_FIELD']], x))
            )
            .map(lambda x: x[1][1])
        )

        return train_data, test_data

    def _get_precision(results):
        # results is an rdd of tuples of the form (true_value, predicted_value)
        
        # Precision: of all predicted matches, how many were real?

        # Count all predicted matches
        predicted_matches = results.filter(lambda x: x[1] == 1)
        denominator = predicted_matches.count()

        # Count how many of them are actually true
        numerator = predicted_matches.map(lambda x: x[0]).reduce(add)

        percentage = float(numerator)/denominator*100

        return numerator, denominator, percentage

    def _get_recall(results):
        # results is an rdd of tuples of the form (true_value, predicted_value)

        # Recall : Of all true matches, how many were retrieved?

        # Count all true matches
        true_matches = results.filter(lambda x: x[0] == 1)
        denominator = true_matches.count()

        # Count how many of them were retrieved
        numerator = true_matches.map(lambda x: x[1]).reduce(add)

        percentage = float(numerator)/denominator*100

        return numerator, denominator, percentage

    def _predict_extra_block_pair(labeled_point, same_block_bool, logistic_regression):
        if not same_block_bool:
            return 0
        else:
            return logistic_regression.predict(labeled_point.features)


    # ********* MAIN ***************

    # Settings
    if settings['RANDOM_SEED'] is None:
        settings['RANDOM_SEED'] = random.randint(1, 100) 
    # At some point, we will need the length of the distances vectors (features for ml) later while constructing the labeled points..
    n_deduper_fields = len(settings['DEDUPER_FIELDS'])


    # Read the header line
    headers = utils.get_headers()

    # Get the data in an RDD
    data = _get_rdd(headers)
    log_line("The whole dataset contains %d records" % data.count())

    # Split labeled data and unlabeled data
    labeled_data = data.filter(lambda x: x[settings['DEDUPER_GROUND_TRUTH_FIELD']] is not None)
    unlabeled_data = data.filter(lambda x: x[settings['DEDUPER_GROUND_TRUTH_FIELD']] is None)
    log_line("%d records are labeled and %d records are unlabeld"% (labeled_data.count(), unlabeled_data.count()))

    # Split labeled data into a training and test datasets (on unique values of the DEDUPER_GROUND_TRUTH_FIELD such that all true pairs are together in their dataset)
    train_data, test_data = _split_on_ground_thruth_field(labeled_data)
    log_line("Labeled data was split into %d records for training and %d records for testing" % (train_data.count(), test_data.count()))

    # Loop on all predicates that we want to try
    for predicate_function in settings['PREDICATE_FUNCTIONS']:

        log_line("\n***** Predicate function %s *************\n" % str(predicate_function))

        # Add the predicate key to training and test_data
        train_data = train_data.map(lambda x: utils.add_predicate_key(x, **predicate_function))
        test_data = test_data.map(lambda x: utils.add_predicate_key(x, **predicate_function))

        # Generate a new rdd with all intra-block pairs and the true value of whether or not they are matches (based on the ground thruth field)
        train_pairs = (
            # Transform into tuples of the form (<key>, <value>) where key is the predicate and value is a list that will be extended with all elements of a block
            train_data.map(lambda x: (x['PredicateKey'], [x]))
                # Extend the list to get all dictionaries of a same block together
                .reduceByKey(lambda l1, l2 : l1 + l2)
                # Generate all pairs of records from each block : (d1, d2)
                .flatMap(utils.generate_pairs)
                # Determine if the pair is a match and use this as a key -> (<match>, (d1, d2))
                .map(lambda x: (utils.records_are_matches(x[0], x[1]), (x[0], x[1])))
                # Convert dictionaries into a list of distance measures (one for each DEDUPER_FIELD) -. (<match>, [0.5, 1, ..])
                .map(lambda x: (x[0], utils.dict_pair_2_distance_list(x[1][0], x[1][1])))
                # Convert list of distances into SparseVectors -> (<match>, SparseVector)
                .map(lambda x: (x[0], SparseVector(n_deduper_fields, dict([(i, v) for i, v in enumerate(x[1]) if v is not None]))))
                # Convert tuples into LabeledPoints (LabeledPoint)
                .map(lambda x: LabeledPoint(x[0], x[1]))
        )
        n_true_matches = train_pairs.filter(lambda x: x.label == 1).count()
        n_true_no_match = train_pairs.filter(lambda x: x.label == 0).count()
        log_line("When taking all intra-block pairs, we get %d true matches and %d true no-match" % (n_true_matches, n_true_no_match))
        ratio = float(n_true_matches)/n_true_no_match
        # If the ratio is too unbalanced, balance it
        if ratio < 0.85 or ratio > 1.15:
            log_line("Intra-block pairs are too unbalanced, we will sample the biggest set to get approximately the same number of each type")
            label_with_too_many = 0 if n_true_no_match > n_true_matches else 1
            keep_all_label = 0 if label_with_too_many == 1 else 1
            train_pairs = (
                # Keep all of the smaller set
                train_pairs.filter(lambda x: x.label == keep_all_label)
                .union(
                    # Add a sample of the bigger set
                    train_pairs.filter(lambda x: x.label == label_with_too_many)
                    .sample(False, ratio, seed=settings['RANDOM_SEED'])
                )
            )
            n_true_matches = train_pairs.filter(lambda x: x.label == 1).count()
            n_true_no_match = train_pairs.filter(lambda x: x.label == 0).count()
            log_line("After sampling, intra-block pairs, we get %d true matches and %d true no-match" % (n_true_matches, n_true_no_match))
        else:
            log_line("These intra-block pairs are balanced enough so we will keep all of them")
        
        # Train a logistic regression
        log_line("Training a logistic regression...")
        logistic_regression = LogisticRegressionWithSGD.train(train_pairs)

        # ******* Training results **************

        log_line("\nResults when comparing training intra-block pairs only:")
        # Build a rdd or tuples of the form: (true_label, predicted_label) for train and test data
        train_results = train_pairs.map(lambda x: (x.label, logistic_regression.predict(x.features)))
        # Precision and recall on training data
        numerator, denominator, percentage = _get_precision(train_results)
        log_line("Intra-block precision on training data: %d/%d = %.2f%%" % (numerator, denominator, percentage))
        numerator, denominator, percentage = _get_recall(train_results)
        log_line("Intra-block recall on training data: %d/%d = %.2f%%" % (numerator, denominator, percentage))

        # ******* Test results **************

        # Generate random pairs instead of intra-block pairs until the number of true matches is big enough
        n_true_matches_in_test_pairs = 0
        curr_n_pairs_in_test = 0
        fraction = 0
        while n_true_matches_in_test_pairs < settings['MIN_TRUE_MATCHES_FOR_RECALL_CALCULATION'] and fraction < 0.5:
            log_line("\nGenerating a random set of pairs for testing the model...\n")
            # Taking 2 samples whose size is the square root of the number of pairs we want and then excluding same-record pairs will give us a random sample of pairs of approximately the right size
            curr_n_pairs_in_test += N_PAIRS_TO_TEST
            fraction = float(sqrt(curr_n_pairs_in_test))/test_data.count()
            random_test_pairs = (
                test_data.sample(False, fraction, seed=settings['RANDOM_SEED'])
                    .map(lambda x: (True, x))
                    .join(
                        test_data
                        .sample(False, fraction, seed=settings['RANDOM_SEED'])
                        .map(lambda x: (True, x))
                    )
                .filter(lambda x: x[1][0] != x[1][1])
                # Only keep the tuple of 2 dictionaries
                .map(lambda x: x[1])
                # Determine if the pair is a match and use this as a key -> (<match>, (d1, d2))
                .map(lambda x: (utils.records_are_matches(x[0], x[1]), x))
                # Convert dictionaries into a list of distance measures (one for each DEDUPER_FIELD) -. (<match>, [0.5, 1, ..], (d1, d2))
                .map(lambda x: (x[0], utils.dict_pair_2_distance_list(x[1][0], x[1][1]), x[1]))
                # Convert list of distances into SparseVectors -> (<match>, SparseVector, (d1, d2))
                .map(lambda x: (x[0], SparseVector(n_deduper_fields, dict([(i, v) for i, v in enumerate(x[1]) if v is not None])), x[2]))
                # Convert tuples into LabeledPoints ->  (LabeledPoint, (d1, d2))
                .map(lambda x: (LabeledPoint(x[0], x[1]), x[2]))
                # Determine if the pair is in the same block or not -> (LabeledPoint, <same_block>)
                .map(lambda x: (x[0], utils.records_in_same_block(x[1][0], x[1][1])))
            )
            
            n_true_matches_in_test_pairs = random_test_pairs.filter(lambda x: x[0].label == 1).count()

        # Matches in random pairs will be very rare, make sure there are at least some of them..
        log_line("Number of same block pairs in the test set: %d" % random_test_pairs.filter(lambda x: x[1]).count())
        log_line("Number of true matches in the test set: %d" % n_true_matches_in_test_pairs)
        if n_true_matches_in_test_pairs == 0:
            raise BaseException("Could not find enough true matches to test prediction and recall on labeled data.")

        # Get results (<true_label>, <predicted_label>) for random_test_pairs
        test_results = random_test_pairs.map(lambda x: (x[0].label, _predict_extra_block_pair(x[0], x[1], logistic_regression)))
        
        # Precision and recall on test data
        log_line("\nResults when comparing test pairs:")
        numerator, denominator, percentage = _get_precision(test_results)
        log_line("Precision on test data (intra and extra block pairs): %d/%d = %.2f%%" % (numerator, denominator, percentage))
        numerator, denominator, percentage = _get_recall(test_results)
        log_line("Recall on test data (intra and extra block pairs): %d/%d = %.2f%%" % (numerator, denominator, percentage))
        log_line("\n\n")


if __name__ == '__main__':
    main()