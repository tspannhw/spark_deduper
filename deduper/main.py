import os

# Spark!
from pyspark import SparkContext
sc = SparkContext("local", "Virgin Deduper", pyFiles=[
    "deduper/settings.py",
    "deduper/utils.py"
])

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
                # Replace 'NULL' values by Nans
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
    for d in data.take(5):
        print d

if __name__ == '__main__':
    main()