from pyinaturalist_convert.geojson import to_geojson
from test.conftest import load_sample_data


def test_get_geojson_observations():
    observations = load_sample_data('observation.json')
    geojson = to_geojson(observations)
    feature = geojson['features'][0]

    assert feature['geometry']['coordinates'] == [4.360086, 50.646894]
    assert feature['properties']['id'] == 16227955
    assert feature['properties']['taxon.id'] == 493595


def test_get_geojson_observations__custom_properties():
    observations = load_sample_data('observation.json')
    geojson = to_geojson(observations, properties=['taxon.name', 'taxon.rank'])
    feature = geojson['features'][0]

    assert feature['properties']['taxon.name'] == 'Lixus bardanae'
    assert feature['properties']['taxon.rank'] == 'species'
    assert 'id' not in feature['properties'] and 'taxon.id' not in feature['properties']
