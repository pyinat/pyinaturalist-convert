import copy
import json
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import patch

import pytest
from dateutil.tz import tzoffset

from pyinaturalist_convert import dwc_to_observations, to_dwc
from pyinaturalist_convert.dwc import (
    dwc_record_to_observation,
    get_dwc_lookup,
    taxon_to_dwc_record,
)
from pyinaturalist_convert.dwca import OBS_COLUMNS
from test.conftest import load_sample_data

OBSERVATION = load_sample_data('observation.json')['results'][0]
TAXA_RESPONSE = load_sample_data('get_taxa_by_id.json')
PLACES_RESPONSE = load_sample_data('get_places_by_id.json')
MINIMAL_DWC_RECORD = {
    'dwc:catalogNumber': '1',
    'inat:captive': 'wild',
    'dcterms:modified': '2020-01-01T00:00:00Z',
    'dwc:eventDate': '2020-01-01',
    'dwc:decimalLatitude': None,
    'dwc:decimalLongitude': None,
    'dwc:coordinateUncertaintyInMeters': None,
    'dwc:informationWithheld': None,
    'dcterms:license': None,
}


@contextmanager
def _patch_api():
    """Patch pyinaturalist API calls used during DwC conversion"""
    with (
        patch('pyinaturalist_convert.dwc.get_taxa_by_id', return_value=TAXA_RESPONSE),
        patch('pyinaturalist_convert.dwc.get_places_by_id', return_value=PLACES_RESPONSE),
    ):
        yield


@contextmanager
def patch_taxa():
    """Patch pyinaturalist API calls for taxon data"""
    with patch('pyinaturalist_convert.dwc.get_taxa_by_id', return_value=TAXA_RESPONSE) as m:
        yield m


def test_observation_to_dwc():
    """Get a test observation, and convert it to DwC"""
    # Get as a dict, and just test for a few basic terms
    with _patch_api():
        dwc_record = to_dwc(OBSERVATION, fetch_missing=True)[0]

    assert dwc_record['dwc:catalogNumber'] == 45524803
    assert dwc_record['dwc:decimalLatitude'] == 32.8430971478
    assert dwc_record['dwc:decimalLongitude'] == -117.2815829044
    assert dwc_record['dwc:eventDate'] == '2020-05-09T06:01:00-07:00'
    assert dwc_record['dwc:scientificName'] == 'Dirona picta'
    assert dwc_record['dwc:occurrenceID'] == 'https://www.inaturalist.org/observations/45524803'
    assert dwc_record['dwc:otherCatalogueNumbers'] == '9e0555e0-1f8f-4ad1-b5d5-f06f03dd5ed1'
    assert dwc_record['dwc:occurrenceStatus'] == 'present'
    assert dwc_record['dwc:identificationVerificationStatus'] == 'research'
    assert dwc_record['dwc:associatedReferences'] == 'http://www.gbif.org/occurrence/2626669957'
    assert dwc_record['dwc:year'] == 2020
    assert dwc_record['dwc:month'] == 5
    assert dwc_record['dwc:day'] == 9
    assert dwc_record['dwc:countryCode'] == 'US'
    assert dwc_record['dwc:stateProvince'] == 'California'
    assert dwc_record['dwc:county'] == 'San Diego County'
    assert (
        dwc_record['gbif:projectId'] == 'https://www.inaturalist.org/projects/6904'
        '|https://www.inaturalist.org/projects/685'
        '|https://www.inaturalist.org/projects/14306'
    )
    assert 'inaturalistLogin' not in dwc_record


def test_to_dwc_no_fetch():
    """When fetch_missing=False, API calls should not be made"""
    with (
        patch('pyinaturalist_convert.dwc.get_taxa_by_id') as mock_get_taxa,
        patch('pyinaturalist_convert.dwc.get_places_by_id') as mock_get_places,
    ):
        dwc_record = to_dwc(OBSERVATION, fetch_missing=False)[0]
        mock_get_taxa.assert_not_called()
        mock_get_places.assert_not_called()

    # Without API calls, some fields should be missing
    assert dwc_record['dwc:catalogNumber'] == 45524803
    assert dwc_record.get('dwc:countryCode') is None
    assert dwc_record.get('dwc:stateProvince') is None
    assert dwc_record.get('dwc:county') is None
    assert dwc_record.get('dwc:kingdom') is None
    assert dwc_record['dwc:scientificName'] == 'Dirona picta'


def test_fetch_taxon_ancestors_already_present():
    """_fetch_taxon_ancestors should not fetch taxa that already have ancestor data"""
    obs_with_ancestors = copy.deepcopy(OBSERVATION)
    # This indicates ancestors have been fetched and flattened already
    obs_with_ancestors['taxon']['kingdom'] = 'Animalia'

    with (
        patch('pyinaturalist_convert.dwc.get_taxa_by_id') as mock_get_taxa,
        patch('pyinaturalist_convert.dwc.get_places_by_id', return_value=PLACES_RESPONSE),
    ):
        to_dwc([obs_with_ancestors], fetch_missing=True)
        mock_get_taxa.assert_not_called()


def test_taxon_to_dwc():
    taxon = {
        'id': 12345,
        'rank': 'species',
        'rank_level': 10,
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
        'iconic_taxon_id': 3,
        'preferred_common_name': 'Helmeted Friarbird',
        'parent_id': 54321,
        'updated_at': '2020-01-02T03:04:05Z',
        'source_url': 'https://example.org/taxon/source',
    }

    dwc_record = to_dwc(taxa=[taxon])[0]
    assert dwc_record == {
        'dwc:taxonID': 'https://www.inaturalist.org/taxa/12345',
        'dwc:identifier': 'https://www.inaturalist.org/taxa/12345',
        'dwc:parentNameUsageID': 'https://www.inaturalist.org/taxa/54321',
        'dwc:specificEpithet': 'buceroides',
        'dwc:infraspecificEpithet': None,
        'dcterms:modified': '2020-01-02T03:04:05+00:00',
        'dcterms:references': 'https://example.org/taxon/source',
        'dwc:taxonRank': 'species',
        'dwc:scientificName': 'Philemon buceroides',
        'dwc:kingdom': 'Animalia',
        'dwc:phylum': 'Chordata',
        'dwc:class': 'Aves',
        'dwc:order': 'Passeriformes',
        'dwc:family': 'Meliphagidae',
        'dwc:subfamily': None,
        'dwc:genus': 'Philemon',
        'dwc:subgenus': None,
        'dwc:vernacularName': 'Helmeted Friarbird',
        'inat:iconic_taxon_id': 3,
        'inat:extinct': None,
        'inat:threatened': None,
        'inat:introduced': None,
        'inat:native': None,
        'inat:endemic': None,
        'dwc:establishmentMeans': None,
    }


def test_dwc_record_to_observation(tmp_path):
    dwc_path = tmp_path / 'observations.dwc'
    with _patch_api():
        to_dwc(OBSERVATION, dwc_path, fetch_missing=True)

    obs = dwc_to_observations(dwc_path)[0]
    assert obs.id == 45524803
    assert obs.captive is False
    assert obs.license_code == 'CC-BY-NC'
    assert obs.location == (32.8430971478, -117.2815829044)
    assert obs.observed_on == datetime(2020, 5, 9, 6, 1, tzinfo=tzoffset(None, -25200))
    assert obs.taxon.id == 48978
    assert obs.taxon.iconic_taxon_id == 47115
    assert obs.taxon.name == 'Dirona picta'
    assert obs.taxon.preferred_common_name == 'Colorful Dirona'
    assert len(obs.taxon.ancestors) == 6
    kingdom = obs.taxon.ancestors[0]
    assert kingdom.name == 'Animalia' and kingdom.rank == 'kingdom'


@pytest.mark.parametrize('geoprivacy', ['private', 'obscured'])
def test_geoprivacy_round_trip(tmp_path, geoprivacy):
    """Geoprivacy value survives to_dwc -> dwc_to_observations round-trip"""
    dwc_path = tmp_path / 'observations.dwc'
    observation = {**OBSERVATION, 'geoprivacy': geoprivacy}
    with _patch_api():
        to_dwc(observation, dwc_path, fetch_missing=True)

    obs = dwc_to_observations(dwc_path)[0]
    assert obs.geoprivacy == geoprivacy


def test_taxon_id_as_url():
    """taxon.id should appear as a URL in observation DwC records"""
    with _patch_api():
        dwc_record = to_dwc(OBSERVATION, fetch_missing=True)[0]
    assert dwc_record['dwc:taxonID'] == 'https://www.inaturalist.org/taxa/48978'


@pytest.mark.parametrize('means', ['native', 'introduced', 'endemic'])
def test_establishment_means(means):
    """taxon.preferred_establishment_means maps to dwc:establishmentMeans"""
    observation = {
        **OBSERVATION,
        'taxon': {**OBSERVATION.get('taxon', {}), 'preferred_establishment_means': means},
    }
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record['dwc:establishmentMeans'] == means


def test_establishment_means_absent_when_unset():
    """dwc:establishmentMeans is None when taxon has no preferred_establishment_means"""
    # Sample observation does not have preferred_establishment_means set
    with patch_taxa():
        dwc_record = to_dwc(OBSERVATION)[0]
    assert dwc_record.get('dwc:establishmentMeans') is None


@pytest.mark.parametrize(
    'observation_extra,expected',
    [
        (
            {
                'geoprivacy': 'obscured',
                'positional_accuracy': 4,
                'public_positional_accuracy': 22227,
            },
            4,
        ),
        (
            {
                'taxon_geoprivacy': 'obscured',
                'positional_accuracy': None,
                'public_positional_accuracy': 22227,
            },
            22227,
        ),
    ],
)
def test_coordinate_uncertainty(observation_extra, expected):
    """coordinateUncertaintyInMeters uses private accuracy when available, else public"""
    observation = {**OBSERVATION, **observation_extra}
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record['dwc:coordinateUncertaintyInMeters'] == expected


def test_private_location_preferred():
    """private_location coordinates are used when available"""
    observation = {
        **OBSERVATION,
        'geoprivacy': 'obscured',
        'private_location': [34.0, -118.0],
    }
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record['dwc:decimalLatitude'] == 34.0
    assert dwc_record['dwc:decimalLongitude'] == -118.0


def test_sounds_in_data_object():
    """Observations with sounds produce eol:dataObject entries with type Sound"""
    sound = {
        'id': 263113,
        'license_code': 'cc0',
        'attribution': 'no rights reserved',
        'native_sound_id': None,
        'secret_token': None,
        'file_url': 'https://static.inaturalist.org/sounds/263113.wav?1624793769',
        'file_content_type': 'audio/x-wav',
        'play_local': True,
        'subtype': None,
        'flags': [],
    }
    observation = {**OBSERVATION, 'sounds': [sound]}
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]

    data_objects = dwc_record['eol:dataObject']
    sound_objects = [obj for obj in data_objects if obj.get('dcterms:type') == 'Sound']
    assert len(sound_objects) == 1
    s = sound_objects[0]
    assert s['dcterms:format'] == 'audio/x-wav'
    assert s['dcterms:identifier'] == 'https://static.inaturalist.org/sounds/263113.wav?1624793769'
    assert s['dcterms:license'] == 'http://creativecommons.org/licenses/cc0/4.0'
    assert s['dcterms:publisher'] == 'iNaturalist'
    assert s['dwc:catalogNumber'] == 263113


def test_annotation_mapping():
    observation = {
        **OBSERVATION,
        'annotations': [
            {
                'controlled_attribute': {'label': 'Sex'},
                'controlled_value': {'label': 'cannot be determined'},
                'vote_score': 1,
            },
            {
                'controlled_attribute': {'label': 'Life Stage'},
                'controlled_value': {'label': 'adult'},
                'vote_score': 1,
            },
            {
                'controlled_attribute': {'label': 'Flowers and Fruits'},
                'controlled_value': {'label': 'flowers'},
                'vote_score': 1,
            },
            {
                'controlled_attribute': {'label': 'Flowers and Fruits'},
                'controlled_value': {'label': 'fruits'},
                'vote_score': 1,
            },
            {
                'controlled_attribute': {'label': 'Alive or Dead'},
                'controlled_value': {'label': 'cannot be determined'},
                'vote_score': 1,
            },
            {
                'controlled_attribute': {'label': 'Evidence of Presence'},
                'controlled_value': {'label': 'tracks'},
                'vote_score': 1,
            },
            {
                'controlled_attribute': {'label': 'Evidence of Presence'},
                'controlled_value': {'label': 'scat'},
                'vote_score': 1,
            },
            {
                'controlled_attribute': {'label': 'Leaves'},
                'controlled_value': {'label': 'evergreen'},
                'vote_score': 1,
            },
        ],
    }

    with patch_taxa():
        dwc_record = to_dwc(observation)[0]

    assert dwc_record['dwc:sex'] == 'undetermined'
    assert dwc_record['dwc:lifeStage'] == 'adult'
    assert dwc_record['dwc:reproductiveCondition'] == 'flowers|fruits'
    assert dwc_record['dwc:vitality'] == 'undetermined'
    assert json.loads(dwc_record['dwc:dynamicProperties']) == {
        'evidenceOfPresence': ['scat', 'tracks'],
        'leaves': 'evergreen',
    }


def test_cultivated_captive_round_trip(tmp_path):
    """captive=True (cultivated) survives to_dwc -> dwc_to_observations round-trip"""
    dwc_path = tmp_path / 'observations.dwc'
    observation = {**OBSERVATION, 'captive': True}
    with patch_taxa():
        to_dwc(observation, dwc_path)

    obs = dwc_to_observations(dwc_path)[0]
    assert obs.captive is True


def test_dwc_lookup_covers_obs_columns():
    """get_dwc_lookup() must cover every field in OBS_COLUMNS after namespace stripping"""
    stripped_lookup = {k.split(':')[-1] for k in get_dwc_lookup()}
    missing = [col for col in OBS_COLUMNS if col not in stripped_lookup]
    assert missing == [], f'OBS_COLUMNS fields not in get_dwc_lookup(): {missing}'


@pytest.mark.parametrize(
    'annotations, field',
    [
        ([], 'dwc:sex'),
        ([], 'dwc:lifeStage'),
        ([], 'dwc:reproductiveCondition'),
        ([], 'dwc:vitality'),
        ([], 'dwc:dynamicProperties'),
        (
            [
                {
                    'controlled_attribute': {'label': 'Sex'},
                    'controlled_value': {'label': 'male'},
                    'vote_score': -1,
                }
            ],
            'dwc:sex',
        ),
        (
            [
                {
                    'controlled_attribute': {'label': 'Life Stage'},
                    'controlled_value': {'label': 'notavalidstage'},
                    'vote_score': 1,
                }
            ],
            'dwc:lifeStage',
        ),
        (
            [
                {
                    'controlled_attribute': {'label': 'Flowers and Fruits'},
                    'controlled_value': {'label': 'cannot be determined'},
                    'vote_score': 1,
                }
            ],
            'dwc:reproductiveCondition',
        ),
        (
            [
                {
                    'controlled_attribute': {'label': 'Sex'},
                    'controlled_value': {'label': 'male'},
                    'vote_score': 1,
                }
            ],
            'dwc:dynamicProperties',
        ),
    ],
)
def test_annotation_edge_cases(annotations, field):
    """Test edge cases for annotation formatting that should result in a null value"""
    observation = {**OBSERVATION, 'annotations': annotations}
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record[field] is None


@pytest.mark.parametrize(
    'captive, expected',
    [
        (True, None),
        (False, None),
        (None, None),
    ],
)
def test_format_captive(captive, expected):
    observation = {**OBSERVATION, 'captive': captive}
    # Ensure preferred_establishment_means is not set to not interfere
    observation['taxon'] = observation['taxon'].copy()
    observation['taxon'].pop('preferred_establishment_means', None)

    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record.get('dwc:establishmentMeans') == expected


@pytest.mark.parametrize(
    'quality_grade, expected',
    [
        ('research', 'iNaturalist research-grade observations'),
        ('needs_id', 'iNaturalist observations'),
        ('casual', 'iNaturalist observations'),
        ('', 'iNaturalist observations'),
        (None, 'iNaturalist observations'),
    ],
)
def test_format_dataset_name(quality_grade, expected):
    observation = {**OBSERVATION, 'quality_grade': quality_grade}
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record['dwc:datasetName'] == expected


def test_format_event_date_time_date_only():
    observation = {**OBSERVATION, 'observed_on': '2020-05-09', 'time_observed_at': None}
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record['dwc:eventDate'] == '2020-05-09'
    assert dwc_record['dwc:eventTime'] is None


def test_format_event_date_time_with_time_observed_at():
    observation = {
        **OBSERVATION,
        'time_observed_at': '2020-05-09T06:01:00-07:00',
        'observed_on': '2020-05-09',
    }
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record['dwc:eventDate'] == '2020-05-09T06:01:00-07:00'
    assert dwc_record['dwc:eventTime'] == '06:01:00-07:00'


@pytest.mark.parametrize(
    'obs_attrs, expected',
    [
        ({'geoprivacy': 'private'}, 'Coordinates hidden at the request of the observer'),
        (
            {'geoprivacy': 'obscured', 'public_positional_accuracy': 22227},
            'Coordinate uncertainty increased to 22227m at the request of the observer',
        ),
        (
            {'geoprivacy': 'obscured'},
            'Coordinate uncertainty increased to 4m at the request of the observer',
        ),
        (
            {
                'taxon_geoprivacy': 'obscured',
                'public_positional_accuracy': 500,
            },
            'Coordinate uncertainty increased to 500m to protect threatened taxon',
        ),
        (
            {'taxon_geoprivacy': 'obscured'},
            'Coordinate uncertainty increased to 4m to protect threatened taxon',
        ),
        ({'geoprivacy': None}, None),
        ({}, None),
    ],
)
def test_format_geoprivacy(obs_attrs, expected):
    observation = copy.deepcopy(OBSERVATION)
    observation.pop('geoprivacy', None)
    observation.pop('taxon_geoprivacy', None)
    if 'public_positional_accuracy' not in obs_attrs:
        observation.pop('public_positional_accuracy', None)
    observation.update(obs_attrs)

    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record.get('dwc:informationWithheld') == expected


def test_taxon_geoprivacy_obscured_in_full_record():
    """taxon_geoprivacy='obscured' path in observation_to_dwc_record produces correct message"""
    observation = {
        **OBSERVATION,
        'geoprivacy': None,
        'taxon_geoprivacy': 'obscured',
        'public_positional_accuracy': 500,
    }
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record['dwc:informationWithheld'] == (
        'Coordinate uncertainty increased to 500m to protect threatened taxon'
    )


@pytest.mark.parametrize(
    'dwc_record_delta, obs_attr, expected_value',
    [
        (
            {'dwc:informationWithheld': 'Coordinates hidden at the request of the observer'},
            'geoprivacy',
            'private',
        ),
        (
            {
                'dwc:informationWithheld': 'Coordinate uncertainty increased to 500m at the request of the observer'
            },
            'geoprivacy',
            'obscured',
        ),
        (
            {
                'dwc:informationWithheld': 'Coordinate uncertainty increased to protect threatened taxon'
            },
            'geoprivacy',
            'obscured',
        ),
        ({'dwc:informationWithheld': 'Coordinates removed'}, 'geoprivacy', 'private'),
        ({'dwc:informationWithheld': None}, 'geoprivacy', None),
        (
            {'dcterms:license': 'http://creativecommons.org/licenses/by-nc/4.0/'},
            'license_code',
            'CC-BY-NC',
        ),
        (
            {'dcterms:license': {'#text': 'http://creativecommons.org/licenses/by-nc/4.0/'}},
            'license_code',
            'CC-BY-NC',
        ),
        ({'dcterms:license': 'all rights reserved'}, 'license_code', 'ALL RIGHTS RESERVED'),
        ({'dcterms:license': None}, 'license_code', None),
        ({'dwc:taxonID': 'some-non-url-value'}, 'taxon.id', None),
    ],
)
def test_dwc_to_observation_edge_cases(dwc_record_delta, obs_attr, expected_value):
    """Test edge cases for DwC to observation mapping"""
    dwc_record = {**MINIMAL_DWC_RECORD, **dwc_record_delta}
    obs = dwc_record_to_observation(dwc_record)

    # Handle nested attribute like 'taxon.id'
    if '.' in obs_attr:
        obj, attr = obs_attr.split('.')
        value = getattr(getattr(obs, obj), attr)
    else:
        value = getattr(obs, obs_attr)
    assert value == expected_value


def test_get_first_improving_identification():
    """The first improving identification should be used for dwc:identificationID"""
    observation = {
        **OBSERVATION,
        'identifications': [
            {
                'id': 98,
                'current': True,
                'taxon_id': OBSERVATION['taxon']['id'],
                'category': 'supporting',
                'user': {'name': 'Alice', 'login': 'alice'},
                'body': None,
                'created_at': '2020-05-09T00:00:00Z',
            },
            {
                'id': 99,
                'current': True,
                'taxon_id': OBSERVATION['taxon']['id'],
                'category': 'improving',
                'user': {'name': 'Bob', 'login': 'bob'},
                'body': None,
                'created_at': '2020-05-09T00:00:01Z',
            },
        ],
    }
    with _patch_api():
        dwc_record = to_dwc(observation, fetch_missing=True)[0]
    assert dwc_record['dwc:identificationID'] == 99
    assert dwc_record['dwc:identifiedBy'] == 'Bob'


@pytest.mark.parametrize('identifications', [[], None])
def test_get_first_improving_identification_empty(identifications):
    """If there are no identifications, dwc:identificationID should not be set"""
    observation = {**OBSERVATION, 'identifications': identifications}
    if identifications is None:
        del observation['identifications']

    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert 'dwc:identificationID' not in dwc_record


def test_improving_ident_fields_absent_when_no_improving():
    """When no improving identification exists, identification DwC fields are not set"""
    observation = {
        **OBSERVATION,
        'identifications': [
            {
                'id': 99,
                'current': True,
                'taxon_id': OBSERVATION['taxon']['id'],
                'category': 'supporting',
                'user': {'name': 'Alice', 'login': 'alice'},
                'body': None,
                'created_at': '2020-05-09T00:00:00Z',
            }
        ],
    }
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record.get('dwc:identificationID') is None
    assert dwc_record.get('dwc:identifiedBy') is None


def test_improving_ident_orcid_formatted():
    """identifiedByID is formatted as ORCID URL"""
    observation = {
        **OBSERVATION,
        'identifications': [
            {
                'id': 42,
                'current': True,
                'taxon_id': OBSERVATION['taxon']['id'],
                'category': 'improving',
                'user': {'name': 'Alice', 'login': 'alice', 'orcid': '0000-0001-2345-6789'},
                'body': None,
                'created_at': '2020-05-09T00:00:00Z',
            }
        ],
    }
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record['dwc:identifiedByID'] == 'https://orcid.org/0000-0001-2345-6789'


@pytest.mark.parametrize(
    'license_code, expected',
    [
        (None, None),
        ('', None),
        ('cc0', 'http://creativecommons.org/licenses/cc0/4.0'),
        ('CC-BY', 'http://creativecommons.org/licenses/by/4.0'),
        ('CC-BY-NC', 'http://creativecommons.org/licenses/by-nc/4.0'),
    ],
)
def test_format_license(license_code, expected):
    observation = {**OBSERVATION, 'license_code': license_code}
    with patch_taxa():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record.get('dcterms:license') == expected


_MISSING = object()


@pytest.mark.parametrize(
    'dwc_license, expected_license_code',
    [
        (None, None),
        (_MISSING, None),
        ('http://creativecommons.org/licenses/by-nc/4.0/', 'CC-BY-NC'),
        ('http://creativecommons.org/licenses/by/4.0/', 'CC-BY'),
        # dict license value (can appear from XML parsing)
        ({'#text': 'http://creativecommons.org/licenses/by-nc/4.0/'}, 'CC-BY-NC'),
        # Non-CC license passes through unchanged
        ('all rights reserved', 'ALL RIGHTS RESERVED'),
    ],
)
def test_format_dwc_license(dwc_license, expected_license_code):
    dwc_record = {**MINIMAL_DWC_RECORD, 'dcterms:license': dwc_license}
    if dwc_license is _MISSING:
        del dwc_record['dcterms:license']
    obs = dwc_record_to_observation(dwc_record)
    assert obs.license_code == expected_license_code


@pytest.mark.parametrize(
    'orcid, expected',
    [
        (None, None),
        ('', None),
        ('0000-0001-2345-6789', 'https://orcid.org/0000-0001-2345-6789'),
        ('https://orcid.org/0000-0001-2345-6789', 'https://orcid.org/0000-0001-2345-6789'),
        ('http://orcid.org/0000-0001-2345-6789', 'http://orcid.org/0000-0001-2345-6789'),
    ],
)
def test_format_observer_orcid(orcid, expected):
    """Observer's ORCID is correctly formatted as recordedByID"""
    observation = {**OBSERVATION, 'user': {**OBSERVATION['user'], 'orcid': orcid}}
    with _patch_api():
        dwc_record = to_dwc(observation, fetch_missing=True)[0]
    assert dwc_record.get('dwc:recordedByID') == expected


@pytest.mark.parametrize(
    'user, expected',
    [
        ({'name': 'Alice Smith', 'login': 'alice'}, 'Alice Smith'),
        ({'name': '', 'login': 'alice'}, 'alice'),
        ({'name': None, 'login': 'alice'}, 'alice'),
        ({'login': 'alice'}, 'alice'),
        ({}, None),
    ],
)
def test_format_identifier_name(user, expected):
    """Test formatting of identifier's name from user object"""
    ident = {
        'id': 99,
        'current': True,
        'taxon_id': OBSERVATION['taxon']['id'],
        'category': 'improving',
        'user': user,
        'created_at': '2020-05-09T00:00:00Z',
    }
    observation = {**OBSERVATION, 'identifications': [ident]}
    with _patch_api():
        dwc_record = to_dwc(observation, fetch_missing=True)[0]
    assert dwc_record.get('dwc:identifiedBy') == expected


@pytest.mark.parametrize(
    'user, expected',
    [
        ({'name': 'Alice Smith', 'login': 'alice'}, 'Alice Smith'),
        ({'name': '', 'login': 'alice'}, 'alice'),
        ({'name': None, 'login': 'alice'}, 'alice'),
        ({'login': 'alice'}, 'alice'),
        ({}, None),
    ],
)
def test_format_observer_name(user, expected):
    """Test formatting of observer's name from user object"""
    observation = {**OBSERVATION, 'user': user}
    with _patch_api():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record.get('dwc:recordedBy') == expected


@pytest.mark.parametrize(
    'projects, expected',
    [
        ([], None),
        (None, None),
        (
            [{'project': {'id': 123}}, {'project': {'id': 456}}],
            'https://www.inaturalist.org/projects/123|https://www.inaturalist.org/projects/456',
        ),
        (
            [{'project': {}}, {'project': {'id': 42}}],
            'https://www.inaturalist.org/projects/42',
        ),
    ],
)
def test_format_project_ids(projects, expected):
    observation = {**OBSERVATION, 'project_observations': projects}
    if projects is None:
        del observation['project_observations']
    with _patch_api():
        dwc_record = to_dwc(observation)[0]
    assert dwc_record.get('gbif:projectId') == expected


def test_taxon_to_dwc_subspecies():
    taxon = {
        'id': 99999,
        'rank': 'subspecies',
        'rank_level': 5,
        'name': 'Homo sapiens sapiens',
        'ancestors': [],
        'parent_id': 43584,
        'updated_at': '2021-01-01T00:00:00Z',
    }
    dwc_record = taxon_to_dwc_record(taxon)
    assert dwc_record['dwc:specificEpithet'] == 'sapiens'
    assert dwc_record['dwc:infraspecificEpithet'] == 'sapiens'


def test_taxon_to_dwc_genus_only():
    taxon = {
        'id': 77777,
        'rank': 'genus',
        'rank_level': 20,
        'name': 'Dirona',
        'ancestors': [],
        'updated_at': '2021-01-01T00:00:00Z',
    }
    dwc_record = taxon_to_dwc_record(taxon)
    assert dwc_record.get('dwc:specificEpithet') is None
    assert dwc_record.get('dwc:infraspecificEpithet') is None


def test_taxon_id_not_url_left_as_none():
    """taxonID that is not a /taxa/ URL does not set taxon.id"""
    dwc_record = {
        **MINIMAL_DWC_RECORD,
        'dwc:taxonID': 'some-non-url-value',
    }
    obs = dwc_record_to_observation(dwc_record)
    # taxon.id should not be set from a non-URL value (stays None or original)
    assert obs.taxon.id is None
