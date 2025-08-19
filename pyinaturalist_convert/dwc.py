"""Convert observations to and from `Darwin Core <https://www.tdwg.org/standards/dwc>`_.

**Extra dependencies**: ``xmltodict``

**Example**::

    >>> from pyinaturalist import iNatClient
    >>> from pyinaturalist_convert import to_dwc, dwc_to_observations

    >>> # Search observations and convert to Darwin Core:
    >>> client = iNatClient()
    >>> observations = client.observations.search(user_id='my_username')
    >>> to_dwc(observations, 'my_observations.dwc')

    >>> # Convert Darwin Core back to Observation objects:
    >>> observations = dwc_to_observations('my_observations.dwc')

**Main functions:**

.. autosummary::
    :nosignatures:

    to_dwc
    dwc_to_observations
"""

# TODO: For sound recordings: eol:dataObject.dcterms:type and any other fields?
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from dateutil.parser import parse as parse_date
from flatten_dict import flatten, unflatten
from pyinaturalist import RANKS, Coordinates, Observation, Photo, get_taxa_by_id
from pyinaturalist.converters import try_float_pair, try_int

from .constants import PathOrStr
from .converters import AnyObservations, AnyTaxa, flatten_observations, to_dicts, write

# Top-level fields from observation JSON
OBSERVATION_FIELDS = {
    'created_at': 'xap:CreateDate',
    'description': 'dwc:occurrenceRemarks',
    'id': 'dwc:catalogNumber',
    'license_code': 'dcterms:license',
    'observed_on': 'dwc:verbatimEventDate',
    'place_guess': 'dwc:verbatimLocality',
    'positional_accuracy': 'dwc:coordinateUncertaintyInMeters',
    'taxon.id': 'dwc:taxonID',
    'taxon.rank': 'dwc:taxonRank',
    'taxon.name': 'dwc:scientificName',
    'taxon.preferred_common_name': 'dwc:vernacularName',
    'taxon.kingdom': 'dwc:kingdom',
    'taxon.phylum': 'dwc:phylum',
    'taxon.class': 'dwc:class',
    'taxon.order': 'dwc:order',
    'taxon.family': 'dwc:family',
    'taxon.subfamily': 'dwc:subfamily',
    'taxon.genus': 'dwc:genus',
    'taxon.subgenus': 'dwc:subgenus',
    'taxon.variety': 'dwc:cultivarEpithet',
    'taxon.iconic_taxon_id': 'inat:iconic_taxon_id',
    'updated_at': 'dcterms:modified',
    'uri': ['dcterms:references', 'dwc:occurrenceDetails', 'dwc:occurrenceID'],
    'user.login': 'dwc:inaturalistLogin',
    'user.name': ['dwc:recordedBy', 'dcterms:rightsHolder'],
    'user.orcid': 'dwc:recordedByID',
}


# Fields from first ID (observation['identifications'][0])
ID_FIELDS = {
    'created_at': 'dwc:dateIdentified',
    'id': 'dwc:identificationID',
    'body': 'dwc:identificationRemarks',
    'user.name': 'dwc:identifiedBy',
    'user.orcid': 'dwc:identifiedByID',
}

# Fields from items in observation['photos']
PHOTO_FIELDS = {
    'id': 'dcterms:identifier',
    'url': 'ac:derivedFrom',
    'attribution': 'dcterms:rights',
}


# Fields from observation JSON to add to photo info in eol:dataObject
PHOTO_OBS_FIELDS = {
    'description': 'dcterms:description',
    'observed_on': 'ap:CreateDate',
    'user.name': ['dcterms:creator', 'xap:Owner'],
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
    'dcterms:type': 'http://purl.org/dc/dcmitype/StillImage',
}

# Other constants needed for converting/formatting
CC_BASE_URL = 'http://creativecommons.org/licenses'
CC_URL_PATTERN = re.compile(r'.*\/licenses\/([\w-]+)/.*')
CC_VERSION = '4.0'
DATASET_TITLES = {'casual': 'casual', 'needs_id': 'unconfirmed', 'research': 'research-grade'}
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

# For reference: other observation fields that are added with additional formatting:
#   license_code: dcterms:license
#   quality_grade: dwc:datasetName
#   captive: [inat:captive, dwc:establishmentMeans]
#   location: [dwc:decimalLatitude, dwc:decimalLongitude]
#   observed_on: dwc:eventDate  # ISO datetime
#   observed_on: dwc:eventTime  # ISO datetime, Time portion only
#   geoprivacy: informationWithheld

# Photo fields:
#   ac:accessURI: link to 'original' size photo
#   media:thumbnailURL: link to 'thumbnail' size photo
#   ac:furtherInformationURL: Link to photo info page
#   dcterms:format: MIME type, based on file extension
#   xap:UsageTerms: license code URL

# Additional fields that could potentially be added:
#   dwc:sex: From annotations
#   dwc:lifeStage: From annotations
#   dwc:countryCode: 2-letter country code; possibly get from place_guess
#   dwc:stateProvince:This may require a separate query to /places endpoint, so skipping for now


def to_dwc(
    observations: Optional[AnyObservations] = None,
    filename: Optional[PathOrStr] = None,
    taxa: Optional[AnyTaxa] = None,
) -> Optional[List[Dict]]:
    """Convert observations into to a Simple Darwin Core RecordSet.

    Args:
        observations: Observation records to convert
        filename: Path to write XML output
        taxa: Convert taxon records instead of observations

    Returns:
        A list of observation dictionaries (if no filename is provided)
    """
    if observations:
        records = [observation_to_dwc_record(obs) for obs in flatten_observations(observations)]
    elif taxa:
        records = [taxon_to_dwc_record(taxon) for taxon in to_dicts(taxa)]
    if filename:
        write(get_dwc_record_set(records), filename)
        return None
    else:
        return records


def get_dwc_record_set(records: List[Dict]) -> str:
    """Make a DwC RecordSet as an XML string, including namespaces and the provided observation
    records
    """
    import xmltodict

    namespaces = {f'@{k}': v for k, v in XML_NAMESPACES.items()}
    records = {**namespaces, 'dwr:SimpleDarwinRecord': records}  # type: ignore
    return xmltodict.unparse({'dwr:SimpleDarwinRecordSet': records}, pretty=True, indent=' ' * 4)


def observation_to_dwc_record(observation: Dict) -> Dict:
    """Translate a flattened JSON observation from API results to a DwC record"""
    dwc_record = {}
    observation = _add_taxon_ancestors(observation)

    # Add main observation + taxon fields
    for inat_field, dwc_fields in OBSERVATION_FIELDS.items():
        for dwc_field in _ensure_list(dwc_fields):
            dwc_record[dwc_field] = observation.get(inat_field)

    # Add identification fields
    if observation['identifications']:
        first_id = flatten(observation['identifications'][0], reducer='dot')
        for inat_field, dwc_field in ID_FIELDS.items():
            dwc_record[dwc_field] = first_id.get(inat_field)

    # Add photos
    dwc_record['eol:dataObject'] = [
        _photo_to_data_object(observation, photo) for photo in observation['photos']
    ]

    # Add constants
    for dwc_field, value in CONSTANTS.items():
        dwc_record[dwc_field] = value

    # Add fields that require some formatting
    dwc_record.update(_format_location(observation.get('location')))
    dwc_record['inat:captive'] = _format_captive(observation['captive'])
    dwc_record['dwc:establishmentMeans'] = _format_captive(observation['captive'])
    dwc_record['dwc:datasetName'] = _format_dataset_name(observation['quality_grade'])
    dwc_record['dwc:eventDate'] = _format_datetime(observation['observed_on'])
    dwc_record['dwc:eventTime'] = _format_time(observation['observed_on'])
    dwc_record['dwc:informationWithheld'] = _format_geoprivacy(observation)
    dwc_record['dcterms:license'] = _format_license(observation['license_code'])

    return dwc_record


def taxon_to_dwc_record(taxon: Dict) -> Dict:
    """Translate a taxon from API results to a partial DwC record (taxonomy terms only)"""
    # Translate 'ancestors' from API results to 'rank': 'name' fields
    for ancestor in taxon['ancestors'] + [taxon]:
        taxon[ancestor['rank']] = ancestor['name']

    return {
        dwc_field: taxon.get(inat_field.replace('taxon.', '').replace('inat.', ''))
        for inat_field, dwc_field in OBSERVATION_FIELDS.items()
        if inat_field.startswith('taxon.')
    }


def _photo_to_data_object(observation: Dict, photo: Dict) -> Dict:
    """Translate observation photo fields to eol:dataObject fields"""
    dwc_photo = {}
    for inat_field, dwc_field in PHOTO_FIELDS.items():
        dwc_photo[dwc_field] = photo[inat_field]
    for inat_field, dwc_fields in PHOTO_OBS_FIELDS.items():
        for dwc_field in _ensure_list(dwc_fields):
            dwc_photo[dwc_field] = observation.get(inat_field)
    for dwc_field, value in PHOTO_CONSTANTS.items():
        dwc_photo[dwc_field] = value

    photo_obj = Photo.from_json(photo)
    dwc_photo['ac:accessURI'] = photo_obj.original_url
    dwc_photo['ac:furtherInformationURL'] = photo_obj.info_url
    dwc_photo['dcterms:format'] = photo_obj.mimetype
    dwc_photo['media:thumbnailURL'] = photo_obj.thumbnail_url
    dwc_photo['xap:UsageTerms'] = _format_license(photo_obj.license_code)
    return dwc_photo


def _add_taxon_ancestors(observation):
    """observation['taxon'] doesn't have full ancestry, so we'll need to get that from the
    /taxa endpoint
    """
    response = get_taxa_by_id(observation['taxon.id'])
    taxon = response['results'][0]

    # Simplify ancestor records into genus=xxxx, family=xxxx, etc.
    for ancestor in taxon['ancestors']:
        observation[f'taxon.{ancestor["rank"]}'] = ancestor['name']

    return observation


def _ensure_list(value):
    return value if isinstance(value, list) else [value]


def _format_captive(captive: bool) -> str:
    return 'cultivated' if captive else 'wild'


def _format_dataset_name(quality_grade: str) -> str:
    return f'iNaturalist {DATASET_TITLES.get(quality_grade, "")} observations'


def _format_datetime(dt: Union[datetime, str]) -> str:
    if isinstance(dt, str):
        return dt
    return dt.replace(microsecond=0).isoformat()


def _format_geoprivacy(observation: Dict) -> Optional[str]:
    if observation['geoprivacy'] == 'obscured':
        return (
            f'Coordinate uncertainty increased to {observation["positional_accuracy"]}'
            'at the request of the observer'
        )
    elif observation['geoprivacy'] == 'private':
        return 'Coordinates removed at the request of the observer'
    else:
        return None


def _format_license(license_code: str) -> Optional[str]:
    """Format a Creative Commons license code into a URL with its license information.
    Example: ``CC-BY-NC --> https://creativecommons.org/licenses/by-nc/4.0/``
    """
    if not license_code:
        return None
    url_slug = license_code.lower().replace('cc-', '')
    return f'{CC_BASE_URL}/{url_slug}/{CC_VERSION}'


def _format_location(location: Optional[List[float]]) -> Dict[str, float]:
    if not location:
        return {}
    return {'dwc:decimalLatitude': location[0], 'dwc:decimalLongitude': location[1]}


def _format_time(dt: Union[datetime, str]) -> str:
    if isinstance(dt, str):
        dt = parse_date(dt)
    return dt.strftime('%H:%M%z')


def dwc_to_observations(filename: PathOrStr) -> List[Observation]:
    """Load observations from a Darwin Core file

    Args:
        filename: Path to a DwC file
    """
    import xmltodict

    with open(filename) as f:
        dwc = xmltodict.parse(f.read())
    records = dwc['dwr:SimpleDarwinRecordSet']['dwr:SimpleDarwinRecord']
    return [dwc_record_to_observation(record) for record in _ensure_list(records)]


# TODO: Translate eol:dataObject to photos
def dwc_record_to_observation(dwc_record: Dict[str, Any]) -> Observation:
    """Translate a DwC Record to an Observation object

    Args:
        dwc_record: A DwC Record as a dictionary
    """
    lookup = get_dwc_lookup()

    json_record = {json_key: dwc_record.get(dwc_key) for dwc_key, json_key in lookup.items()}
    json_record['captive'] = dwc_record.get('captive') == 'cultivated'
    json_record['geoprivacy'] = _format_dwc_geoprivacy(dwc_record)
    json_record['license_code'] = _format_dwc_license(dwc_record)
    json_record['location'] = _format_dwc_location(dwc_record)
    json_record['positional_accuracy'] = try_int(json_record.get('positional_accuracy'))
    json_record['taxon.ancestors'] = _format_dwc_ancestors(dwc_record)
    json_record['taxon.partial'] = True

    for field in ['id', 'taxon.id', 'taxon.iconic_taxon_id']:
        json_record[field] = try_int(json_record.get(field))
    if isinstance(json_record['updated_at'], list):
        json_record['updated_at'] = json_record['updated_at'][0]
    if isinstance(json_record['user.name'], list):
        json_record['user.name'] = json_record['user.name'][0]
    json_record = unflatten(json_record, splitter='dot')

    return Observation.from_json(json_record)


def get_dwc_lookup() -> Dict[str, str]:
    """Get a lookup table of DwC terms to standard field names"""
    lookup = {}
    for k, v in OBSERVATION_FIELDS.items():
        if isinstance(v, list):
            lookup.update(dict.fromkeys(v, k))
        else:
            lookup[v] = k
    lookup['dwc:captive'] = 'captive'
    lookup['dwc:decimalLatitude'] = 'latitude'
    lookup['dwc:decimalLongitude'] = 'longitude'
    lookup['dwc:informationWithheld'] = 'geoprivacy'
    lookup['dwc:eventDate'] = 'observed_on'
    return lookup


def _format_dwc_ancestors(dwc_record: Dict) -> List[Dict[str, str]]:
    ancestors = []
    for rank in RANKS[::-1]:
        if name := dwc_record.get(f'dwc:{rank}'):
            ancestors.append({'rank': rank, 'name': name})
    return ancestors


def _format_dwc_geoprivacy(dwc_record: Dict) -> Optional[str]:
    if not dwc_record.get('informationWithheld'):
        return None
    elif 'Coordinate uncertainty increased' in dwc_record['informationWithheld']:
        return 'obscured'
    elif 'Coordinates hidden' in dwc_record['informationWithheld']:
        return 'private'
    else:
        return 'open'


def _format_dwc_license(dwc_record: Dict) -> Optional[str]:
    """Format a CC license URL to a license code"""
    license = dwc_record.get('dcterms:license')
    if not license:
        return None

    if isinstance(license, dict):
        license = list(license.values())[0]
    if match := CC_URL_PATTERN.match(license):
        license = f'CC-{match.groups()[0]}'.upper()
    return license


def _format_dwc_location(dwc_record: Dict) -> Optional[Coordinates]:
    location = try_float_pair(
        dwc_record.get('dwc:decimalLatitude'),
        dwc_record.get('dwc:decimalLongitude'),
    )
    return list(location) if location else None
