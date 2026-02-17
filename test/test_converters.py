from csv import DictReader
from datetime import datetime

import pytest
from pyinaturalist import Observation, Taxon, User

from pyinaturalist_convert.converters import (
    read,
    to_csv,
    to_dataframe,
    to_dataset,
)
from pyinaturalist_convert.db import create_tables, save_observations
from pyinaturalist_convert.dwc import to_dwc
from pyinaturalist_convert.geojson import to_geojson
from pyinaturalist_convert.gpx import to_gpx
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


def test_read__dwc(tmp_path):
    dwc_path = tmp_path / 'observations.dwc'
    observation = load_sample_data('observation.json')['results'][0]
    to_dwc(observation, dwc_path)

    observations = read(dwc_path)
    assert len(observations) == 1
    assert isinstance(observations[0], Observation)
    assert observations[0].id == 45524803


def test_read__geojson(tmp_path):
    geojson_path = tmp_path / 'observations.geojson'
    observation = load_sample_data('observation.json')
    to_geojson(observation, geojson_path)

    observations = read(geojson_path)
    assert len(observations) == 1
    obs = observations[0]
    assert isinstance(obs, Observation)
    assert obs.id == 45524803
    assert obs.location == (32.843097, -117.281583)


def test_read__gpx(tmp_path):
    gpx_path = tmp_path / 'observations.gpx'
    obs = Observation.from_json_file(SAMPLE_DATA_DIR / 'observation.json')[0]
    to_gpx([obs], gpx_path)

    observations = read(gpx_path)
    assert len(observations) == 1
    result = observations[0]
    assert isinstance(result, Observation)
    assert result.location == (32.8430971478, -117.2815829044)


def test_read__sqlite(tmp_path):
    db_path = tmp_path / 'observations.db'
    obs = Observation.from_json_file(SAMPLE_DATA_DIR / 'observation.json')
    create_tables(db_path)
    save_observations(obs, db_path)

    observations = read(db_path)
    assert len(observations) == 1
    assert isinstance(observations[0], Observation)
    assert observations[0].id == 45524803


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
    assert df['annotations'][0][0] == {'Life Stage': 'Adult'}
    assert df['annotations'][0][1] == {'17': 18}
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
