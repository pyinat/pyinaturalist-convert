from csv import DictReader
from datetime import datetime

import pytest
from pyinaturalist import Observation, Taxon, User

from pyinaturalist_convert.converters import read, to_csv, to_dataframe, to_dataset
from test.conftest import SAMPLE_DATA_DIR, load_sample_data


@pytest.mark.parametrize('file_type', ['.csv', '_export.csv', '.feather', '.parquet', '.hdf'])
def test_read_formats(file_type):
    filename = SAMPLE_DATA_DIR / f'observations{file_type}'
    observations = read(filename)
    obs = observations[0]
    assert isinstance(obs, Observation)
    assert isinstance(obs.id, int)
    assert isinstance(obs.created_at, datetime)
    assert isinstance(obs.taxon, Taxon)
    assert isinstance(obs.user, User)


def test_to_dataset():
    observations = Observation.from_json_list(load_sample_data('observations.json'))
    dataset = to_dataset(observations)
    assert all(isinstance(i, int) for i in dataset['id'])
    assert all(isinstance(i, int) for i in dataset['taxon.id'])
    assert all(isinstance(i, datetime) for i in dataset['created_at'])
    assert isinstance(dataset['location'][0][0], float)


def test_to_dataframe():
    observations = Observation.from_json_list(load_sample_data('observations.json'))
    df = to_dataframe(observations)

    assert df['id'][0] == 117511016
    assert df['taxon.id'][0] == 48662
    assert df['annotations'][0] == []
    assert df['comments'][0] == []
    assert df['identifications'][0] == [{'261377245': 48662}]
    assert df['photos'][0][0]['198465145'].startswith(
        'https://inaturalist-open-data.s3.amazonaws.com'
    )
    assert df['photo_url'][0].startswith('https://inaturalist-open-data.s3.amazonaws.com')
    assert df['sounds'][0][0]['263113'].startswith('https://static.inaturalist.org')
    assert df['sound_url'][0].startswith('https://static.inaturalist.org')


def test_to_csv(tmp_path):
    observations = Observation.from_json_list(load_sample_data('observations.json'))
    filename = tmp_path / 'observations.csv'
    to_csv(observations, filename)

    with open(filename) as f:
        reader = DictReader(f)
        row = next(reader)

    assert row['id'] == '117511016'
    assert row['taxon.id'] == '48662'
    assert row['created_at'] == '2022-05-17 17:09:56-05:00'
