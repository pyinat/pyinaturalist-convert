"""Utilities for converting observations to Darwin Core"""
from datetime import datetime
from typing import Dict, List, Optional

from flatten_dict import flatten
from pyinaturalist import get_taxa_by_id

from .constants import PathOrStr
from .converters import AnyObservations, flatten_observations, write

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
    'taxon.id': 'dwc:taxonID',
    'taxon.rank': 'dwc:taxonRank',
    'taxon.name': 'dwc:scientificName',
    'taxon.kingdom': 'dwc:kingdom',
    'taxon.phylum': 'dwc:phylum',
    'taxon.class': 'dwc:class',
    'taxon.order': 'dwc:order',
    'taxon.family': 'dwc:family',
    'taxon.genus': 'dwc:genus',
    'time_observed_at': 'dwc:eventDate',  # ISO format; use as-is
    'updated_at': 'dcterms:modified',
    'uri': ['dcterms:references', 'dwc:occurrenceDetails', 'dwc:occurrenceID'],
    'user.login': 'dwc:inaturalistLogin',
    'user.name': ['dwc:recordedBy', 'dcterms:rightsHolder'],
    'user.orcid': 'dwc:recordedByID',
    # 'observed_on': 'dwc:verbatimEventDate',  # but with different format: March 17, 2008 12:00 UTC
    # 'time_observed_at: 'dwc:eventTime'  # Time portion only, in UTC
    # 'dwc:verbatimEventDate': Probably the user-submitted date from photo metadata; just reuse observed_on?
    # 'dwc:establishmentMeans': 'wild' or 'cultivated'; may need a separate API request to get this info?
    # 'dwc:countryCode': 2-letter country code; possibly get from place_guess?
    # 'dwc:stateProvince': Also get from place_guess? Or separate query to /places endpoint? Or just omit this?
    # 'dwc:sex': From annotations
    # 'dwc:lifeStage': From annotations
}


# TODO: Fields from first ID (observation['identifications'][0])
ID_FIELDS = {
    'created_at': 'dwc:dateIdentified',
    'id': 'dwc:identificationID',
    'body': 'dwc:identificationRemarks',
    'user.name': 'dwc:identifiedBy',
    'user.orcid': 'dwc:identifiedByID',
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
    # 'dcterms/format': 'image/jpeg'  (determine from file extension)
    # 'ac:accessURI': (link to 'original' size photo)
    # 'media:thumbnailURL': (link to 'thumbnail' size photo)
}

# Fields from observation JSON to add to photo info in eol:dataObject
PHOTO_OBS_FIELDS = {
    'description': 'dcterms:description',
    'user.name': ['dcterms:creator', 'xap:Owner'],
    # 'ap:CreateDate': ?  Format: 2020-05-10T19:59:48Z
    # 'dcterms:modified': ?
}

# Fields that will be constant for all iNaturalist observations
CONSTANTS = {
    'dwc:basisOfRecord': 'HumanObservation',
    'dwc:collectionCode': 'Observations',
    'dwc:institutionCode': 'iNaturalist',
    'dwc:geodeticDatum': 'EPSG:4326',
}
PHOTO_CONSTANTS = {
    'dcterms:publisher': 'iNaturalist',
    # TODO: Is this value different if there are sound recordings?
    'dcterms:type': 'http://purl.org/dc/dcmitype/StillImage',
}

# Other fields that are added with additional formatting:
# 'license_code': 'dcterms:license'
# 'quality_grade': 'dwc:datasetName'
# 'captive': ['inat:captive', 'dwc:establishmentMeans']
# 'location': ['dwc:decimalLatitude', 'dwc:decimalLongitude']

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
    'xmlns:inat': 'https://www.inaturalist.org/schema/terms/',
}


def to_dwc(observations: AnyObservations, filename: PathOrStr = None) -> Optional[List[Dict]]:
    """Convert observations into to a Simple Darwin Core RecordSet.

    Args:
        observations: Observation records to convert
        filename: Path to write XML output

    Returns:
        If no filename is provided, records will be returned a list of dictionaries.
    """
    import xmltodict

    records = [observation_to_dwc_record(obs) for obs in flatten_observations(observations)]
    if not filename:
        return records

    record_set = get_dwc_record_set(records)
    record_xml = xmltodict.unparse(record_set, pretty=True, indent=' ' * 4)
    write(record_xml, filename)
    return None


def get_dwc_record_set(records: List[Dict]) -> Dict:
    """Make a DwC RecordSet including XML namespaces and the provided observation records"""
    namespaces = {f'@{k}': v for k, v in XML_NAMESPACES.items()}
    return {'dwr:SimpleDarwinRecordSet': {**namespaces, 'dwr:SimpleDarwinRecord': records}}


def observation_to_dwc_record(observation: Dict) -> Dict:
    """Translate a flattened JSON observation from API results to a DwC record"""
    dwc_record = {}
    observation = add_taxon_ancestors(observation)

    # Add main observation + taxon fields
    for inat_field, dwc_fields in OBSERVATION_FIELDS.items():
        for dwc_field in ensure_str_list(dwc_fields):
            dwc_record[dwc_field] = observation.get(inat_field)

    # Add identification fields
    if observation['identifications']:
        first_id = flatten(observation['identifications'][0], reducer='dot')
        for inat_field, dwc_field in ID_FIELDS.items():
            dwc_record[dwc_field] = first_id.get(inat_field)

    # Add additional formatted fields
    dwc_record.update(format_location(observation['location']))
    establishment_means = 'cultivated' if observation['captive'] else 'wild'
    dwc_record['inat:captive'] = establishment_means
    dwc_record['dwc:establishmentMeans'] = establishment_means
    dwc_record['dwc:datasetName'] = format_dataset_name(observation['quality_grade'])
    dwc_record['dcterms:license'] = format_license(observation['license_code'])

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


def add_taxon_ancestors(observation):
    """observation['taxon'] doesn't have full ancestry, so we'll need to get that from the
    /taxa endpoint
    """
    response = get_taxa_by_id(observation['taxon.id'])
    taxon = response['results'][0]

    # Simplify ancestor records into genus=xxxx, family=xxxx, etc.
    for ancestor in taxon['ancestors']:
        observation[f"taxon.{ancestor['rank']}"] = ancestor['name']

    return observation


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
    return {'dwc:decimalLatitude': location[0], 'dwc:decimalLongitude': location[1]}
