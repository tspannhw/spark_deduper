import os

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

    # ********* MAIN ***************

    # Read the header line
    headers = utils.get_headers()

    # Get the data in an RDD
    data = _get_rdd(headers)

    # Filter out those withtout a frequent traveler number
    ml_data = data.filter(lambda x: x['FrequentTravelerNbr'] is not None)

    ml_data = (
        # Add a key for a predicate that takes the first 3 characeters of lastName
        ml_data.map(lambda x: utils.add_predicate_key(x, 
                predicate_key_name = 'NameFirst3FirstChars',
                base_key = 'NameFirst',
                predicate_type = 'FirstChars',
                predicate_value = 3,
                )
            )
        # Transform into tuples of the form (<key>, <value>) where key is the predicate and value is a list that will be extended with all elements of a block
        .map(lambda x: (x['NameFirst3FirstChars'], [x]))
        # Extend the list to get all dictionaries of a same block together
        .reduceByKey(lambda l1, l2 : l1 + l2)
        # Generate all pairs of records from each block
        .flatMap(utils.generate_pairs)
    )

    print type(ml_data)
    print ml_data.take(5)

if __name__ == '__main__':
    main()

