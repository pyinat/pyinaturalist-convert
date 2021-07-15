import json
from os.path import abspath, dirname, join

PROJECT_DIR = abspath(dirname(dirname(__file__)))
SAMPLE_DATA_DIR = join(PROJECT_DIR, 'test', 'sample_data')


def load_sample_data(filename):
    with open(join(SAMPLE_DATA_DIR, filename), encoding='utf-8') as f:
        if filename.endswith('json'):
            return json.load(f)
        else:
            return f.read()
