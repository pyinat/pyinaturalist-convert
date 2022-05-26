from datetime import datetime

from dateutil.tz import tzoffset

from pyinaturalist_convert import dwc_to_observations, to_dwc
from test.conftest import SAMPLE_DATA_DIR, load_sample_data


def test_observation_to_dwc():
    """Get a test observation, and convert it to DwC"""
    observation = load_sample_data('observation.json')['results'][0]

    # Get as a dict, and just test for a few basic terms
    dwc_record = to_dwc(observation)[0]
    assert dwc_record['dwc:catalogNumber'] == 45524803
    assert dwc_record['dwc:decimalLatitude'] == 32.8430971478
    assert dwc_record['dwc:decimalLongitude'] == -117.2815829044
    assert dwc_record['dwc:eventDate'] == '2020-05-09 06:01:00-07:00'
    assert dwc_record['dwc:scientificName'] == 'Dirona picta'


def test_taxon_to_dwc():
    taxon = {
        'id': 12345,
        'rank': 'species',
        'name': 'Philemon buceroides',
        'ancestors': [
            {'rank': 'kingdom', 'name': 'Animalia'},
            {'rank': 'phylum', 'name': 'Chordata'},
            {'rank': 'class', 'name': 'Aves'},
            {'rank': 'order', 'name': 'Passeriformes'},
            {'rank': 'family', 'name': 'Meliphagidae'},
            {'rank': 'genus', 'name': 'Philemon'},
            {'rank': '', 'name': ''},
        ],
    }

    dwc_record = to_dwc(taxa=[taxon])[0]
    assert dwc_record == {
        'dwc:taxonID': 12345,
        'dwc:taxonRank': 'species',
        'dwc:scientificName': 'Philemon buceroides',
        'dwc:kingdom': 'Animalia',
        'dwc:phylum': 'Chordata',
        'dwc:class': 'Aves',
        'dwc:order': 'Passeriformes',
        'dwc:family': 'Meliphagidae',
        'dwc:genus': 'Philemon',
    }


def test_dwc_record_to_observation():
    dwc_path = SAMPLE_DATA_DIR / 'observations.dwc'
    observation = load_sample_data('observation.json')['results'][0]
    to_dwc(observation, dwc_path)

    obs = dwc_to_observations(dwc_path)[0]
    assert obs.id == 45524803
    assert obs.captive is False
    assert obs.license_code == 'CC-BY-NC'
    assert obs.location == (32.8430971478, -117.2815829044)
    assert obs.observed_on == datetime(2020, 5, 9, 6, 1, tzinfo=tzoffset(None, -25200))
    assert obs.taxon.id == 48978
    assert obs.taxon.name == 'Dirona picta'
