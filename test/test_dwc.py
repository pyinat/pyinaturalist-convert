from pyinaturalist import get_observations

from pyinaturalist_convert import to_dwc


def test_observation_to_dwc():
    """Get a test observation, and convert it to DwC"""
    response = get_observations(id=45524803)
    observation = response['results'][0]

    # Write to a file
    to_dwc(observation, 'test/sample_data/observations.dwc')

    # Get as a dict, and just test for a few basic terms
    dwc_record = to_dwc(observation)[0]
    assert dwc_record['dwc:catalogNumber'] == 45524803
    assert dwc_record['dwc:decimalLatitude'] == 32.8430971478
    assert dwc_record['dwc:decimalLongitude'] == -117.2815829044
    assert dwc_record['dwc:eventDate'] == '2020-05-09T06:01:00-08:00'
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
