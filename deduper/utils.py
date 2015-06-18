from datetime import datetime

from deduper.settings import settings

def get_headers():
    with open(settings['HEADER_LOCAL_DATA_PATH'], 'r') as f:
        header_line = f.readlines()[0]
    headers = header_line[:-1].split(settings['SEPARATOR'])
    return headers

def convert_dates(line_dict):
    return dict([(k, v) if k not in settings['DATE_FIELDS'] or v == None else (k, datetime.strptime(v, "%Y-%m-%d").date()) for k, v in line_dict.items()])