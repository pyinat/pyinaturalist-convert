import geojson
import pytest
from geojson import Feature, FeatureCollection

from pyinaturalist_convert.geojson import _to_geojson_feature, to_geojson
from test.conftest import load_sample_data


def test_to_geojson():
    observations = load_sample_data('observation.json')
    obs_geojson = to_geojson(observations)
    _validate_feature(obs_geojson)


def test_to_geojson__to_file(tmp_path):
    observations = load_sample_data('observation.json')
    file_path = tmp_path / 'observations.geojson'
    to_geojson(observations, file_path)

    with open(file_path) as f:
        obs_geojson = geojson.load(f)
    _validate_feature(obs_geojson)


def _validate_feature(obs_geojson: FeatureCollection):
    assert isinstance(obs_geojson, FeatureCollection)

    feature = obs_geojson['features'][0]
    assert obs_geojson.is_valid
    assert feature['geometry']['coordinates'] == [-117.281583, 32.843097]
    assert feature['properties']['id'] == 45524803
    assert feature['properties']['taxon.id'] == 48978
    assert isinstance(feature, Feature)
    assert feature.is_valid


def test_to_geojson__custom_properties():
    observations = load_sample_data('observation.json')
    geojson = to_geojson(observations, properties=['taxon.name', 'taxon.rank'])
    feature = geojson['features'][0]

    assert feature['properties']['taxon.name'] == 'Dirona picta'
    assert feature['properties']['taxon.rank'] == 'species'
    assert 'id' not in feature['properties'] and 'taxon.id' not in feature['properties']
    assert isinstance(feature, Feature)
    assert feature.is_valid


def test_to_geojson_obs_without_geojson():
    observations = load_sample_data('observations_without_coords.json')
    with pytest.raises(Exception) as excinfo:
        _to_geojson_feature(observations)
    assert 'Observation without coordinates' in str(excinfo.value)
