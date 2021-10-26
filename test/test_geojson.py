import pytest
from geojson import Feature, FeatureCollection

from pyinaturalist_convert.geojson import to_geojson, _to_geojson_feature
from test.conftest import load_sample_data


def test_to_geojson():
    observations = load_sample_data('observation.json')
    geojson = to_geojson(observations)
    feature = geojson['features'][0]

    assert isinstance(geojson, FeatureCollection)
    assert geojson.is_valid
    assert feature['geometry']['coordinates'] == [4.360086, 50.646894]
    assert feature['properties']['id'] == 16227955
    assert feature['properties']['taxon.id'] == 493595
    assert isinstance(feature, Feature)
    assert feature.is_valid


def test_to_geojson__custom_properties():
    observations = load_sample_data('observation.json')
    geojson = to_geojson(observations, properties=['taxon.name', 'taxon.rank'])
    feature = geojson['features'][0]

    assert feature['properties']['taxon.name'] == 'Lixus bardanae'
    assert feature['properties']['taxon.rank'] == 'species'
    assert 'id' not in feature['properties'] and 'taxon.id' not in feature['properties']
    assert isinstance(feature, Feature)
    assert feature.is_valid


def test_to_geojson_obs_without_geojson():
    observations = load_sample_data('observations_without_coords.json')
    with pytest.raises(Exception) as excinfo:
        _to_geojson_feature(observations)
    assert 'Observation without coordinates' in str(excinfo.value)
