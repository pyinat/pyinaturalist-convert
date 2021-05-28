"""Incomplete outline of mapping iNaturalist fields to DwC terms"""
from datetime import datetime
from typing import Dict, List

import xmltodict
from pyinaturalist import get_observations, get_taxa_by_id

from pyinaturalist_convert.converters import AnyObservation, ensure_list, write

# Fields from observation JSON
OBSERVATION_FIELDS = {
    'id': 'dwc:catalogNumber',
    'observed_on': 'dwc:eventDate',
    'quality_grade': 'dwc:datasetName',
    'time_observed_at': 'dwc:eventDate',
    # 'annotations': [], # not found
    'positional_accuracy': 'dwc:coordinateUncertaintyInMeters',
    'license_code': 'dcterms:license',  # not exactly but seems derived from,
    'public_positional_accuracy': 'dwc:coordinateUncertaintyInMeters',
    'created_at': 'xap:CreateDate',  # not matching but probably due to UTC
    'description': 'dcterms:description',
    'updated_at': 'dcterms:modified',
    'uri': 'dcterms:references',  # or 'dwc:occurrenceDetails',
    # 'location': [
    #     'dwc:decimalLongitude',
    #     'dwc:decimalLatitude',
    # ],  # can be derived from 'dwc:decimalLatitude' and 'dwc:decimalLongitude'
    'place_guess': 'dwc:verbatimLocality',
    # 'observed_on': 'dwc:verbatimEventDate',  # but with different standart: YYYY-MM-DD HH:MM:SS-UTC
}

# Fields from taxon JSON
TAXON_FIELDS = {
    'id': 'dwc:taxonID',
    'rank': 'dwc:taxonRank',
    'name': 'dwc:scientificName',
    'kingdom': 'dwc:kingdom',
    'phylum': 'dwc:phylum',
    'class': 'dwc:class',
    'order': 'dwc:order',
    'family': 'dwc:family',
    'genus': 'dwc:genus',
}

# Fields from items in observation['photos']
PHOTO_FIELDS = {
    'url': 'dcterms:identifier',  # or ac:accessURI, media:thumbnailURL, ac:furtherInformationURL, ac:derivedFrom, ac:derivedFrom # change the host to amazon
    'license_code': 'xap:UsageTerms',  # Will need to be translated to a link to creativecommons.org
    'id': 'dcterms:identifier',  # or ac:accessURI, media:thumbnailURL, ac:furtherInformationURL, ac:derivedFrom, ac:derivedFrom
    'attribution': 'dcterms:rights',
}

# Fields that will be constant for all iNaturalist observations
CONSTANTS = {
    'dwc:basisOfRecord': 'HumanObservation',
    'dwc:institutionCode': 'iNaturalist',
    # ...
}

CC_BASE_URL = 'http://creativecommons.org/licenses'
DATETIME_FIELDS = ['observed_on', 'created_at']
XML_NAMESPACES = {
    'xsi:schemaLocation': 'http://rs.tdwg.org/dwc/xsd/simpledarwincore/  http://rs.tdwg.org/dwc/xsd/tdwg_dwc_simple.xsd',
    'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'xmlns:ac': 'http://rs.tdwg.org/ac/terms/',
    'xmlns:dcterms': 'http://purl.org/dc/terms/',
    'xmlns:dwc': 'http://rs.tdwg.org/dwc/terms/',
    'xmlns:dwr': 'http://rs.tdwg.org/dwc/xsd/simpledarwincore/',
    'xmlns:eol': 'http://www.eol.org/transfer/content/1.0',
    'xmlns:geo': 'http://www.w3.org/2003/01/geo/wgs84_pos#',
    'xmlns:media': 'http://eol.org/schema/media/',
    'xmlns:ref': 'http://eol.org/schema/reference/',
    'xmlns:xap': 'http://ns.adobe.com/xap/1.0/',
}


def to_dwc(observations: AnyObservation, filename: str):
    """Convert observations into to a Simple Darwin Core RecordSet"""
    records = [observation_to_dwc_record(obs) for obs in ensure_list(observations)]
    record_set = get_dwc_record_set(records)
    record_xml = xmltodict.unparse(record_set, pretty=True, indent=' ' * 4)
    write(record_xml, filename)


def observation_to_dwc_record(observation) -> Dict:
    """Translate a JSON-formatted observation from API results to a DwC record"""
    # Translate observation fields
    dwc_record = {}
    for inat_field, dwc_field in OBSERVATION_FIELDS.items():
        dwc_record[dwc_field] = observation[inat_field]

    # Translate taxon fields
    taxon = get_taxon_with_ancestors(observation)
    for inat_field, dwc_field in TAXON_FIELDS.items():
        dwc_record[dwc_field] = taxon.get(inat_field)

    # Translate photo fields
    photo = observation['photos'][0]
    dwc_photo = {}
    for inat_field, dwc_field in PHOTO_FIELDS.items():
        dwc_photo[dwc_field] = photo[inat_field]
    dwc_record['eol:dataObject'] = dwc_photo

    # Add constants
    for dwc_field, value in CONSTANTS.items():
        dwc_record[dwc_field] = value

    return dwc_record


def get_dwc_record_set(records: List[Dict]) -> Dict:
    """Get"""
    namespaces = {f'@{k}': v for k, v in XML_NAMESPACES.items()}
    return {'dwr:SimpleDarwinRecordSet': {**namespaces, 'dwr:SimpleDarwinRecord': records}}


def get_taxon_with_ancestors(observation):
    """observation['taxon'] doesn't have full ancestry, so we'll need to get that from the
    /taxa endpoint
    """
    response = get_taxa_by_id(observation['taxon']['id'])
    taxon = response['results'][0]

    # Simplify ancestor records into genus=xxxx, family=xxxx, etc.
    for ancestor in taxon['ancestors']:
        taxon[ancestor['rank']] = ancestor['name']

    return taxon


# TODO
def format_datetime(dt: datetime) -> str:
    pass


def format_license(license_code: str) -> str:
    pass


def format_location(location: List[float]) -> Dict[str, float]:
    pass


def test_observation_to_dwc():
    """Get a test observation, convert it to DwC, and write it to a file"""
    response = get_observations(id=45524803)
    observation = response['results'][0]
    to_dwc(observation, 'obs_45524803.dwc')


if __name__ == '__main__':
    test_observation_to_dwc()
