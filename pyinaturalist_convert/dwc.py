"""Utilities for converting observations to Darwin Core"""
# TODO: May need to use jmespath or jsonpath to more easily reference nested values
#   (or use flatten_dict/json_normalize)
from datetime import datetime
from os.path import join
from typing import Dict, List

from pyinaturalist import get_observations, get_taxa_by_id

from pyinaturalist_convert.converters import AnyObservation, ensure_list, write

# Fields from observation JSON
OBSERVATION_FIELDS = {
    'created_at': 'xap:CreateDate',  # Different format
    'description': 'dcterms:description',
    'id': 'dwc:catalogNumber',
    'license_code': 'dcterms:license',
    'observed_on': 'dwc:eventDate',
    'place_guess': 'dwc:verbatimLocality',
    'positional_accuracy': 'dwc:coordinateUncertaintyInMeters',
    'public_positional_accuracy': 'dwc:coordinateUncertaintyInMeters',
    'quality_grade': 'dwc:datasetName',
    'time_observed_at': 'dwc:eventDate',  # ISO format; use as-is
    'updated_at': 'dcterms:modified',
    'uri': ['dcterms:references', 'dwc:occurrenceDetails', 'dwc:occurrenceID'],
    # 'location': ['dwc:decimalLatitude', 'dwc:decimalLongitude']  # Split coordinates into lat/long fields
    # 'observed_on': 'dwc:verbatimEventDate',  # but with different standart: YYYY-MM-DD HH:MM:SS-UTC
    # 'time_observed_at: 'dwc:eventTime'  # Time portion only, in UTC
    # 'dwc:verbatimEventDate': Probably the user-submitted date from photo metadata; just reuse observed_on?
    # 'dwc:establishmentMeans': 'wild' or 'cultivated'; may need a separate API request to get this info
    # 'dwc:identificationID':  identifications[0].id
    # 'dwc:identifiedBy':  identifications[0].user.name
    # 'dwc:countryCode': 2-letter country code; possibly get from place_guess?
    # 'dwc:stateProvince': Also get from place_guess? Or separate query to /places endpoint?
    # 'dwc:inaturalistLogin': user.login
    # 'dwc:recordedBy': user.name
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
    'id': [  # format ID into photo URL
        'dcterms:identifier',
        'ac:furtherInformationURL',
        'ac:derivedFrom',
    ],
    'license_code': 'xap:UsageTerms',
    'attribution': 'dcterms:rights',
    # 'description': 'dcterms:description',  # From observation.description
    # 'user.name': 'xap:Owner',  # also dcterms:creator; get from inner ['user'] record
    # 'dcterms/format': 'image/jpeg'  (determine from file extension)
    # 'ac:accessURI': (link to 'original' size photo)
    # 'media:thumbnailURL': (link to 'thumbnail' size photo)
    # 'ap:CreateDate': ?  Format: 2020-05-10T19:59:48Z
    # 'dcterms:modified': ?
}

# Fields that will be constant for all iNaturalist observations
CONSTANTS = {
    'dwc:basisOfRecord': 'HumanObservation',
    'dwc:collectionCode': 'Observations',
    'dwc:institutionCode': 'iNaturalist',
    # ...
}
PHOTO_CONSTANTS = {
    'dcterms:publisher': 'iNaturalist',
    # TODO: Is this value different if there are sound recordings?
    'dcterms:type': 'http://purl.org/dc/dcmitype/StillImage',
}

# Other constants needed for converting/formatting
CC_BASE_URL = 'http://creativecommons.org/licenses'
CC_VERSION = '4.0'
DATASET_TITLES = {'casual': 'casual', 'needs_id': 'unidentified', 'research': 'research-grade'}
DATETIME_FIELDS = ['observed_on', 'created_at']
PHOTO_BASE_URL = 'https://www.inaturalist.org/photos'
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
    import xmltodict

    records = [observation_to_dwc_record(obs) for obs in ensure_list(observations)]
    record_set = get_dwc_record_set(records)
    record_xml = xmltodict.unparse(record_set, pretty=True, indent=' ' * 4)
    write(record_xml, filename)


def observation_to_dwc_record(observation) -> Dict:
    """Translate a JSON-formatted observation from API results to a DwC record"""
    # Translate observation fields
    dwc_record = {}
    for inat_field, dwc_fields in OBSERVATION_FIELDS.items():
        for dwc_field in ensure_str_list(dwc_fields):
            dwc_record[dwc_field] = observation[inat_field]
    dwc_record['dcterms:license'] = format_license(observation['license_code'])
    dwc_record['dwc:datasetName'] = format_dataset_name(observation['quality_grade'])

    # Translate taxon fields
    taxon = get_taxon_with_ancestors(observation)
    for inat_field, dwc_fields in TAXON_FIELDS.items():
        for dwc_field in ensure_str_list(dwc_fields):
            dwc_record[dwc_field] = taxon.get(inat_field)

    # Add photos
    photos = [photo_to_data_object(photo) for photo in observation['photos']]
    dwc_record['eol:dataObject'] = photos

    # Add constants
    for dwc_field, value in CONSTANTS.items():
        dwc_record[dwc_field] = value

    return dwc_record


def photo_to_data_object(photo: Dict) -> Dict:
    """Translate observation photo fields to eol:dataObject fields"""
    dwc_photo = {}
    for inat_field, dwc_fields in PHOTO_FIELDS.items():
        for dwc_field in ensure_str_list(dwc_fields):
            dwc_photo[dwc_field] = photo[inat_field]
    for dwc_field, value in PHOTO_CONSTANTS.items():
        dwc_photo[dwc_field] = value

    dwc_photo['xap:UsageTerms'] = format_license(photo['license_code'])
    return dwc_photo


def get_dwc_record_set(records: List[Dict]) -> Dict:
    """Make a DwC RecordSet including XML namespaces and the provided observation records"""
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


def ensure_str_list(value):
    return value if isinstance(value, list) else [value]


def format_dataset_name(quality_grade: str) -> str:
    return f'iNaturalist {DATASET_TITLES.get(quality_grade, "")} observations'


# TODO
def format_datetime(dt: datetime) -> str:
    pass


def format_license(license_code: str) -> str:
    """Format a Creative Commons license code into a URL with its license information.
    Example: ``CC-BY-NC --> https://creativecommons.org/licenses/by-nc/4.0/``
    """
    url_slug = license_code.lower().replace('cc-', '')
    return f'{CC_BASE_URL}/{url_slug}/{CC_VERSION}'


def format_location(location: List[float]) -> Dict[str, float]:
    pass


def test_observation_to_dwc():
    """Get a test observation, convert it to DwC, and write it to a file"""
    response = get_observations(id=45524803)
    observation = response['results'][0]
    to_dwc(observation, join('test', 'sample_data', 'observations.dwc'))


if __name__ == '__main__':
    test_observation_to_dwc()
