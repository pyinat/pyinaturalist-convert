from pyinaturalist.constants import API_V1_BASE_URL
from test.conftest import load_sample_data


from pyinaturalist_convert.geojson import get_geojson_observations


def test_get_geojson_observations(requests_mock):
    requests_mock.get(
        f'{API_V1_BASE_URL}/observations',
        json=load_sample_data('get_observation.json'),
        status_code=200,
    )

    geojson = get_geojson_observations(id=16227955)
    feature = geojson['features'][0]
    assert feature['geometry']['coordinates'] == [4.360086, 50.646894]
    assert feature['properties']['id'] == 16227955
    assert feature['properties']['taxon_id'] == 493595


def test_get_geojson_observations__custom_properties(requests_mock):
    requests_mock.get(
        f'{API_V1_BASE_URL}/observations',
        json=load_sample_data('get_observation.json'),
        status_code=200,
    )

    properties = ['taxon_name', 'taxon_rank']
    geojson = get_geojson_observations(id=16227955, properties=properties)
    feature = geojson['features'][0]
    assert feature['properties']['taxon_name'] == 'Lixus bardanae'
    assert feature['properties']['taxon_rank'] == 'species'
    assert 'id' not in feature['properties'] and 'taxon_id' not in feature['properties']
