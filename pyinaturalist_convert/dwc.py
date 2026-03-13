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

import re
from datetime import datetime
from typing import Any, Optional

from dateutil.parser import parse as parse_date
from flatten_dict import unflatten
from pyinaturalist import (
    RANKS,
    Coordinates,
    Observation,
    Photo,
    Sound,
    get_places_by_id,
    get_taxa_by_id,
)
from pyinaturalist.converters import try_float_pair, try_int

from .constants import (
    CC_BASE_URL,
    CC_URL_PATTERN,
    CC_VERSION,
    GBIF_LIFE_STAGES,
    PathOrStr,
)
from .converters import AnyObservations, AnyTaxa, flatten_observations, to_dicts, write

# For reference: observation fields added with additional formatting in observation_to_dwc_record:
#   description: dwc:occurrenceRemarks      (via _dwc_filter_text)
#   license_code: dcterms:license           (via _format_license)
#   observed_on: dwc:verbatimEventDate      (observed_on_string with fallback)
#   place_guess: dwc:verbatimLocality       (via _dwc_filter_text)
#   taxon.id: dwc:taxonID                   (as URL form)
#   quality_grade: dwc:datasetName
#   captive: inat:captive                           (wild/cultivated; custom iNat term)
#   taxon.preferred_establishment_means: dwc:establishmentMeans
#   location: [dwc:decimalLatitude, dwc:decimalLongitude]
#   observed_on: dwc:eventDate              (ISO datetime)
#   observed_on: dwc:eventTime              (ISO datetime, time portion only)
#   geoprivacy: dwc:informationWithheld
#   positional_accuracy / public_positional_accuracy: dwc:coordinateUncertaintyInMeters  (private preferred)

# Photo fields:
#   ac:accessURI: link to 'original' size photo
#   media:thumbnailURL: link to 'thumbnail' size photo
#   ac:furtherInformationURL: Link to photo info page
#   dcterms:format: MIME type, based on file extension
#   xmp:UsageTerms: license code URL

# Additional fields that could potentially be added:
#   dwc:verbatimCoordinates: raw coordinate string before processing

# Top-level fields from observation JSON
# Note: some fields are handled separately with formatting in observation_to_dwc_record:
#   description -> dwc:occurrenceRemarks  (via _dwc_filter_text)
#   observed_on -> dwc:verbatimEventDate  (via observed_on_string logic)
#   place_guess -> dwc:verbatimLocality   (via _dwc_filter_text)
#   license_code -> dcterms:license       (via _format_license)
#   taxon.id -> dwc:taxonID               (as URL form)
OBSERVATION_FIELDS = {
    'created_at': 'xmp:CreateDate',
    'id': 'dwc:catalogNumber',
    'species_guess': 'dwc:verbatimIdentification',
    'taxon.rank': 'dwc:taxonRank',
    'taxon.name': 'dwc:scientificName',
    'taxon.preferred_common_name': 'dwc:vernacularName',
    'taxon.preferred_establishment_means': 'dwc:establishmentMeans',
    'taxon.kingdom': 'dwc:kingdom',
    'taxon.phylum': 'dwc:phylum',
    'taxon.class': 'dwc:class',
    'taxon.order': 'dwc:order',
    'taxon.family': 'dwc:family',
    'taxon.subfamily': 'dwc:subfamily',
    'taxon.genus': 'dwc:genus',
    'taxon.subgenus': 'dwc:subgenus',
    'taxon.variety': 'dwc:infraspecificEpithet',
    'taxon.iconic_taxon_id': 'inat:iconic_taxon_id',
    'taxon.extinct': 'inat:extinct',
    'taxon.threatened': 'inat:threatened',
    'taxon.introduced': 'inat:introduced',
    'taxon.native': 'inat:native',
    'taxon.endemic': 'inat:endemic',
    'updated_at': 'dcterms:modified',
    'uri': 'dcterms:references',
    # user.login and user.orcid are handled separately with formatting in observation_to_dwc_record
}

# Identification fields from the first improving identification
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
    'observed_on': 'xmp:CreateDate',
    'user.name': ['dcterms:creator', 'xmp:Owner'],
}

# Fields that will be constant for all iNaturalist observations
CONSTANTS = {
    'dwc:basisOfRecord': 'HumanObservation',
    'dwc:collectionCode': 'Observations',
    'dwc:institutionCode': 'iNaturalist',
    'dwc:geodeticDatum': 'EPSG:4326',
    'dwc:occurrenceStatus': 'present',
}
PHOTO_CONSTANTS = {
    'dcterms:publisher': 'iNaturalist',
    'dcterms:type': 'http://purl.org/dc/dcmitype/StillImage',
}

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
    'xmlns:xmp': 'http://ns.adobe.com/xap/1.0/',
    'xmlns:inat': 'https://www.inaturalist.org/schema/terms/',
    'xmlns:gbif': 'http://rs.gbif.org/terms/1.0/',
}


def to_dwc(
    observations: Optional[AnyObservations] = None,
    filename: Optional[PathOrStr] = None,
    taxa: Optional[AnyTaxa] = None,
) -> Optional[list[dict]]:
    """Convert observations into to a Simple Darwin Core RecordSet.

    Args:
        observations: Observation records to convert
        filename: Path to write XML output
        taxa: Convert taxon records instead of observations

    Returns:
        A list of observation dictionaries (if no filename is provided)
    """
    records: list[dict] = []
    if observations:
        obs_list = list(flatten_observations(observations))
        obs_list = _batch_add_taxon_ancestors(obs_list)
        place_cache = _batch_fetch_places(obs_list)
        records = [observation_to_dwc_record(obs, place_cache=place_cache) for obs in obs_list]
    elif taxa:
        records = [taxon_to_dwc_record(taxon) for taxon in to_dicts(taxa)]
    if filename:
        write(get_dwc_record_set(records), filename)
        return None
    else:
        return records


def get_dwc_record_set(records: list[dict]) -> str:
    """Make a DwC RecordSet as an XML string, including namespaces and the provided observation
    records
    """
    import xmltodict

    namespaces = {f'@{k}': v for k, v in XML_NAMESPACES.items()}
    records = {**namespaces, 'dwr:SimpleDarwinRecord': records}  # type: ignore
    return xmltodict.unparse({'dwr:SimpleDarwinRecordSet': records}, pretty=True, indent=' ' * 4)


def observation_to_dwc_record(observation: dict, place_cache: dict | None = None) -> dict:
    """Translate a flattened JSON observation from API results to a DwC record"""
    dwc_record = {}

    # Add main observation + taxon fields
    for inat_field, dwc_fields in OBSERVATION_FIELDS.items():
        for dwc_field in _ensure_list(dwc_fields):
            dwc_record[dwc_field] = observation.get(inat_field)

    # Add identification fields (first improving identification)
    first_id = _get_first_improving_identification(observation)
    if first_id:
        dwc_record['dwc:identificationID'] = first_id.get('id')
        dwc_record['dwc:dateIdentified'] = _format_datetime(first_id.get('created_at'))
        dwc_record['dwc:identificationRemarks'] = _dwc_filter_text(first_id.get('body'))
        dwc_record['dwc:identifiedBy'] = _format_person_name(first_id.get('user', {}))
        dwc_record['dwc:identifiedByID'] = _format_orcid(first_id.get('user', {}).get('orcid'))

    # Add taxonID as URL form (overrides raw integer from OBSERVATION_FIELDS)
    taxon_id = observation.get('taxon.id')
    if taxon_id:
        dwc_record['dwc:taxonID'] = f'https://www.inaturalist.org/taxa/{taxon_id}'

    # Use private positional_accuracy when available (private coord export), otherwise public
    dwc_record['dwc:coordinateUncertaintyInMeters'] = observation.get(
        'positional_accuracy'
    ) or observation.get('public_positional_accuracy')

    # Add photos and sounds
    dwc_record['eol:dataObject'] = [
        _photo_to_data_object(observation, photo) for photo in observation['photos']
    ] + [_sound_to_data_object(observation, sound) for sound in observation.get('sounds', [])]

    # Add constants
    for dwc_field, value in CONSTANTS.items():
        dwc_record[dwc_field] = value

    # Add fields that require some formatting
    dwc_record['dwc:occurrenceID'] = observation.get('uri')
    dwc_record['dwc:otherCatalogueNumbers'] = observation.get('uuid')
    dwc_record.update(
        _format_location(observation.get('private_location') or observation.get('location'))
    )
    dwc_record['inat:captive'] = _format_captive(observation['captive'])
    dwc_record['dwc:datasetName'] = _format_dataset_name(observation['quality_grade'])
    dwc_record['dwc:identificationVerificationStatus'] = observation.get('quality_grade')
    dwc_record['dwc:eventDate'] = _format_event_date(observation)
    dwc_record['dwc:eventTime'] = _format_event_time(observation)
    dwc_record['dwc:informationWithheld'] = _format_geoprivacy(observation)
    dwc_record['dcterms:license'] = _format_license(observation['license_code'])
    dwc_record['dwc:associatedReferences'] = _format_outlinks(observation)
    dwc_record.update(_format_annotations(observation))
    dwc_record['dwc:occurrenceRemarks'] = _dwc_filter_text(observation.get('description'))
    dwc_record['dwc:verbatimEventDate'] = _dwc_filter_text(
        observation.get('observed_on_string') or observation.get('observed_on')
    )
    dwc_record['dwc:verbatimLocality'] = _dwc_filter_text(
        observation.get('private_place_guess') or observation.get('place_guess')
    )

    # Person / login fields
    dwc_record['dwc:recordedBy'] = _format_person_name_from_flat(observation)
    dwc_record['dcterms:rightsHolder'] = _format_person_name_from_flat(observation)
    dwc_record['dwc:recordedByID'] = _format_orcid(observation.get('user.orcid'))

    # Quality metadata + positioning
    dwc_record['inat:numIdentificationAgreements'] = observation.get(
        'num_identification_agreements'
    )
    dwc_record['inat:numIdentificationDisagreements'] = observation.get(
        'num_identification_disagreements'
    )
    dwc_record['inat:positioningDevice'] = observation.get('positioning_device')
    dwc_record['inat:positioningMethod'] = observation.get('positioning_method')

    # Publishing country (if present in payload)
    dwc_record['gbif:publishingCountry'] = _get_publishing_country(observation)
    dwc_record.update(_format_place_fields_from_cache(observation, place_cache or {}))

    dwc_record['dwc:year'] = observation.get('observed_on_details.year')
    dwc_record['dwc:month'] = observation.get('observed_on_details.month')
    dwc_record['dwc:day'] = observation.get('observed_on_details.day')

    dwc_record['gbif:projectId'] = _format_project_ids(observation)

    return dwc_record


def taxon_to_dwc_record(taxon: dict) -> dict:
    """Translate a taxon from API results to a partial DwC record (taxonomy terms only)"""
    # Translate 'ancestors' from API results to 'rank': 'name' fields
    for ancestor in taxon['ancestors'] + [taxon]:
        taxon[ancestor['rank']] = ancestor['name']

    result = {
        dwc_field: taxon.get(inat_field.replace('taxon.', '').replace('inat.', ''))
        for inat_field, dwc_field in OBSERVATION_FIELDS.items()
        if inat_field.startswith('taxon.')
    }

    taxon_id = taxon.get('id')
    if taxon_id:
        taxon_url = f'https://www.inaturalist.org/taxa/{taxon_id}'
        result['dwc:identifier'] = taxon_url
        result['dwc:taxonID'] = taxon_url

    if taxon.get('parent_id'):
        result['dwc:parentNameUsageID'] = f'https://www.inaturalist.org/taxa/{taxon["parent_id"]}'

    name = taxon.get('name') or ''
    name_parts = name.split()
    rank = (taxon.get('rank') or '').lower()
    rank_level = taxon.get('rank_level')

    is_species_or_lower = (rank_level is not None and rank_level <= 10) or rank in {
        'species',
        'subspecies',
        'variety',
        'form',
    }
    is_below_species = (rank_level is not None and rank_level < 10) or rank in {
        'subspecies',
        'variety',
        'form',
    }

    if is_species_or_lower and len(name_parts) >= 2:
        result['dwc:specificEpithet'] = name_parts[1]
    if is_below_species and len(name_parts) >= 3:
        result['dwc:infraspecificEpithet'] = name_parts[2]

    result['dcterms:modified'] = _format_datetime(taxon.get('updated_at'))
    result['dcterms:references'] = taxon.get('source_url') or taxon.get('source', {}).get('url')

    return result


def _photo_to_data_object(observation: dict, photo: dict) -> dict:
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
    dwc_photo['xmp:UsageTerms'] = _format_license(photo_obj.license_code)
    return dwc_photo


def _sound_to_data_object(observation: dict, sound: dict) -> dict:
    """Translate observation sound fields to eol:dataObject fields"""
    sound_obj = Sound.from_json(sound)
    observer_name = _format_person_name_from_flat(observation)
    return {
        'dcterms:type': 'Sound',
        'dcterms:format': sound_obj.file_content_type,
        'dcterms:identifier': sound_obj.file_url,
        'dcterms:references': sound_obj.file_url,
        'dcterms:created': _format_datetime(sound_obj.created_at) or observation.get('observed_on'),
        'dcterms:creator': observer_name,
        'dcterms:publisher': 'iNaturalist',
        'dcterms:license': _format_license(sound_obj.license_code),
        'dcterms:rightsHolder': observer_name,
        'dwc:catalogNumber': sound_obj.id,
    }


def _batch_add_taxon_ancestors(observations: list[dict]) -> list[dict]:
    """Fetch taxon ancestors for all observations in a single API call.

    Note: get_taxa_by_id() accepts a list of IDs; for very large datasets (hundreds of unique
    taxa) consider chunking the list if API limits are encountered.
    """
    taxon_ids = {obs['taxon.id'] for obs in observations if obs.get('taxon.id')}
    if not taxon_ids:
        return observations

    response = get_taxa_by_id(list(taxon_ids))

    # Build cache: taxon_id -> {rank: name}
    ancestor_cache: dict[int, dict[str, str]] = {}
    for taxon in response.get('results', []):
        ancestors = {ancestor['rank']: ancestor['name'] for ancestor in taxon.get('ancestors', [])}
        ancestor_cache[taxon['id']] = ancestors

    # Populate ancestor fields on each observation
    for obs in observations:
        taxon_id = obs.get('taxon.id')
        if taxon_id and taxon_id in ancestor_cache:
            for rank, name in ancestor_cache[taxon_id].items():
                obs[f'taxon.{rank}'] = name

    return observations


def _batch_fetch_places(observations: list[dict]) -> dict[int, dict]:
    """Fetch place details for all place IDs across all observations in a single API call.

    Returns a dict mapping place_id -> place dict.
    Note: get_places_by_id() accepts a list of IDs; chunk if needed for very large datasets.
    """
    all_place_ids: set[int] = set()
    for obs in observations:
        all_place_ids.update(obs.get('place_ids', []))
    if not all_place_ids:
        return {}

    response = get_places_by_id(list(all_place_ids))
    return {place['id']: place for place in response.get('results', [])}


def _ensure_list(value: Any) -> list:
    return value if isinstance(value, list) else [value]


def _format_captive(captive: bool) -> str:
    return 'cultivated' if captive else 'wild'


def _format_dataset_name(quality_grade: str) -> str:
    if quality_grade == 'research':
        return 'iNaturalist research-grade observations'
    return 'iNaturalist observations'


def _format_datetime(dt: datetime | str | None) -> Optional[str]:
    if not dt:
        return None
    if isinstance(dt, str):
        try:
            parsed = parse_date(dt)
            return parsed.replace(microsecond=0).isoformat()
        except (ValueError, TypeError):
            return dt
    return dt.replace(microsecond=0).isoformat()


def _format_geoprivacy(observation: dict) -> Optional[str]:
    if observation.get('geoprivacy') == 'private':
        return 'Coordinates hidden at the request of the observer'
    elif observation.get('geoprivacy') == 'obscured':
        accuracy = observation.get('public_positional_accuracy') or observation.get(
            'positional_accuracy'
        )
        return (
            f'Coordinate uncertainty increased to {accuracy}m at the request of the observer'
            if accuracy
            else 'Coordinate uncertainty increased at the request of the observer'
        )
    elif observation.get('taxon_geoprivacy') == 'obscured':
        accuracy = observation.get('public_positional_accuracy') or observation.get(
            'positional_accuracy'
        )
        return (
            f'Coordinate uncertainty increased to {accuracy}m to protect threatened taxon'
            if accuracy
            else 'Coordinate uncertainty increased to protect threatened taxon'
        )
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


def _format_location(location: Optional[list[float]]) -> dict[str, float]:
    if not location:
        return {}
    return {'dwc:decimalLatitude': location[0], 'dwc:decimalLongitude': location[1]}


def _dwc_filter_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    return re.sub(r'\s+', ' ', str(text)).strip()


def _has_time_component(value: str) -> bool:
    return bool(re.search(r'\d{2}:\d{2}', value))


def _format_event_date(observation: dict) -> Optional[str]:
    dt_value = observation.get('time_observed_at') or observation.get('observed_on')
    if not dt_value:
        return None
    if isinstance(dt_value, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}', dt_value):
        return dt_value
    return _format_datetime(dt_value)


def _format_event_time(observation: dict) -> Optional[str]:
    dt_value = observation.get('time_observed_at')
    if not dt_value:
        observed_on = observation.get('observed_on')
        if isinstance(observed_on, str) and _has_time_component(observed_on):
            dt_value = observed_on
        elif isinstance(observed_on, datetime):
            dt_value = observed_on
        else:
            return None
    formatted = _format_datetime(dt_value)
    if not formatted or 'T' not in formatted:
        return None
    return formatted.split('T', 1)[1]


def _format_person_name(user: dict) -> Optional[str]:
    name = user.get('name')
    return name if name else user.get('login')


def _format_person_name_from_flat(observation: dict) -> Optional[str]:
    name = observation.get('user.name')
    return name if name else observation.get('user.login')


def _camelize_lower(value: str) -> str:
    parts = value.split('_')
    return parts[0] + ''.join(p[:1].upper() + p[1:] for p in parts[1:])


def _format_orcid(orcid: Optional[str]) -> Optional[str]:
    if not orcid:
        return None
    if orcid.startswith(('http://', 'https://')):
        return orcid
    return f'https://orcid.org/{orcid}'


def _get_first_improving_identification(observation: dict) -> Optional[dict]:
    identifications = observation.get('identifications') or []
    if not identifications:
        return None
    taxon_id = observation.get('taxon.id')
    current_idents = sorted(
        [i for i in identifications if i.get('current')],
        key=lambda x: x.get('id', 0),
    )
    for ident in current_idents:
        if ident.get('taxon_id') == taxon_id and ident.get('category') == 'improving':
            return ident
    return None


def _get_publishing_country(observation: dict) -> Optional[str]:
    code = observation.get('site.place.code') or observation.get('site.place_code')
    if code and len(code) == 2:
        return code.upper()
    return None


def _format_place_fields_from_cache(
    observation: dict, place_cache: dict
) -> dict[str, Optional[str]]:
    """Look up place fields for this observation using the pre-fetched place cache.

    Maps admin_level 0 -> dwc:countryCode (place code), 10 -> dwc:stateProvince (name),
    20 -> dwc:county (name).
    """
    country_code = None
    state_province = None
    county = None

    for place_id in observation.get('place_ids', []):
        place = place_cache.get(place_id)
        if not place:
            continue
        admin_level = place.get('admin_level')
        if admin_level == 0:
            country_code = place.get('place_type_code') or place.get('code')
        elif admin_level == 10:
            state_province = place.get('name')
        elif admin_level == 20:
            county = place.get('name')

    return {
        'dwc:countryCode': country_code,
        'dwc:stateProvince': state_province,
        'dwc:county': county,
    }


def _format_project_ids(observation: dict) -> Optional[str]:
    """Build a pipe-separated list of project URLs from project_observations."""
    project_observations = observation.get('project_observations', []) or []
    urls = []
    for p_obs in project_observations:
        project = p_obs.get('project') or {}
        project_id = project.get('id') or p_obs.get('id')
        if project_id:
            urls.append(f'https://www.inaturalist.org/projects/{project_id}')
    return '|'.join(urls) if urls else None


def _normalize_undetermined(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return 'undetermined' if value == 'cannot be determined' else value


def _format_annotations(observation: dict) -> dict:
    """Extract DwC terms from iNat annotations, following iNat DwC mappings"""
    result: dict[str, Optional[str]] = {
        'dwc:sex': None,
        'dwc:lifeStage': None,
        'dwc:reproductiveCondition': None,
        'dwc:vitality': None,
        'dwc:dynamicProperties': None,
    }

    annotations = observation.get('annotations', []) or []
    if not annotations:
        return result

    def _annotation_values(label: str) -> list[str]:
        values: list[str] = []
        for annotation in annotations:
            try:
                attr_label = annotation['controlled_attribute']['label']
                value_label = annotation['controlled_value']['label']
                vote_score = annotation.get('vote_score')
            except (KeyError, TypeError):
                continue
            if vote_score is not None and vote_score < 0:
                continue
            if attr_label and attr_label.lower() == label.lower():
                values.append(str(value_label).lower())
        return values

    def _filter_life_stage(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        return value if value in GBIF_LIFE_STAGES else None

    sex_values = _annotation_values('sex')
    if sex_values:
        result['dwc:sex'] = _normalize_undetermined(sex_values[0])

    life_stage_values = _annotation_values('life stage')
    if life_stage_values:
        result['dwc:lifeStage'] = _filter_life_stage(life_stage_values[0])

    repro_values = _annotation_values('flowers and fruits')
    if repro_values:
        joined = '|'.join(v for v in repro_values if v != 'cannot be determined')
        result['dwc:reproductiveCondition'] = joined or None

    vitality_values = _annotation_values('alive or dead')
    if vitality_values:
        result['dwc:vitality'] = _normalize_undetermined(vitality_values[0])

    dynamic_properties: dict[str, object] = {}
    for label in ['Evidence of Presence', 'Leaves']:
        values = _annotation_values(label)
        if not values:
            continue
        key = _camelize_lower(label.lower().replace(' ', '_'))
        dynamic_properties[key] = values[0] if len(values) == 1 else sorted(values)

    if dynamic_properties:
        import json

        result['dwc:dynamicProperties'] = json.dumps(dynamic_properties, separators=(',', ':'))

    return result


def _format_outlinks(observation: dict) -> Optional[str]:
    """Extract the GBIF URL from observation outlinks, if present"""
    for outlink in observation.get('outlinks', []):
        if outlink.get('source') == 'GBIF':
            return outlink.get('url')
    return None


def dwc_to_observations(filename: PathOrStr) -> list[Observation]:
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
def dwc_record_to_observation(dwc_record: dict[str, Any]) -> Observation:
    """Translate a DwC Record to an Observation object

    Args:
        dwc_record: A DwC Record as a dictionary
    """
    lookup = get_dwc_lookup()

    json_record = {json_key: dwc_record.get(dwc_key) for dwc_key, json_key in lookup.items()}
    json_record['captive'] = dwc_record.get('inat:captive') == 'cultivated'
    json_record['geoprivacy'] = _format_dwc_geoprivacy(dwc_record)
    json_record['license_code'] = _format_dwc_license(dwc_record)
    json_record['location'] = _format_dwc_location(dwc_record)
    json_record['positional_accuracy'] = try_int(json_record.get('positional_accuracy'))
    json_record['taxon.ancestors'] = _format_dwc_ancestors(dwc_record)
    json_record['taxon.partial'] = True

    # taxonID is stored as a URL (e.g. https://www.inaturalist.org/taxa/48978); extract the integer
    taxon_id_url = dwc_record.get('dwc:taxonID')
    if taxon_id_url and isinstance(taxon_id_url, str) and '/taxa/' in taxon_id_url:
        json_record['taxon.id'] = try_int(taxon_id_url.rsplit('/', 1)[-1])

    for field in ['id', 'taxon.id', 'taxon.iconic_taxon_id']:
        json_record[field] = try_int(json_record.get(field))
    if isinstance(json_record['updated_at'], list):
        json_record['updated_at'] = json_record['updated_at'][0]
    if isinstance(json_record['user.name'], list):
        json_record['user.name'] = json_record['user.name'][0]
    json_record = unflatten(json_record, splitter='dot')

    return Observation.from_json(json_record)


def get_dwc_lookup() -> dict[str, str]:
    """Get a lookup table of DwC terms to standard field names"""
    lookup = {}
    for k, v in OBSERVATION_FIELDS.items():
        if isinstance(v, list):
            lookup.update(dict.fromkeys(v, k))
        else:
            lookup[v] = k
    lookup['inat:captive'] = 'captive'
    lookup['dwc:coordinateUncertaintyInMeters'] = 'positional_accuracy'
    lookup['dwc:decimalLatitude'] = 'latitude'
    lookup['dwc:decimalLongitude'] = 'longitude'
    lookup['dwc:informationWithheld'] = 'geoprivacy'
    lookup['dwc:eventDate'] = 'observed_on'
    lookup['dwc:occurrenceID'] = 'uri'
    lookup['dwc:otherCatalogueNumbers'] = 'uuid'
    lookup['dwc:recordedBy'] = 'user.name'
    lookup['dcterms:rightsHolder'] = 'user.name'
    lookup['inat:inaturalistLogin'] = 'user.login'
    lookup['dwc:occurrenceRemarks'] = 'description'
    lookup['dwc:taxonID'] = 'taxon.id'
    return lookup


def _format_dwc_ancestors(dwc_record: dict) -> list[dict[str, str]]:
    ancestors = []
    for rank in RANKS[::-1]:
        if name := dwc_record.get(f'dwc:{rank}'):
            ancestors.append({'rank': rank, 'name': name})
    return ancestors


def _format_dwc_geoprivacy(dwc_record: dict) -> Optional[str]:
    if not dwc_record.get('dwc:informationWithheld'):
        return None
    elif 'Coordinate uncertainty increased' in dwc_record['dwc:informationWithheld']:
        return 'obscured'
    elif 'Coordinates hidden' in dwc_record['dwc:informationWithheld']:
        return 'private'
    elif 'Coordinates removed' in dwc_record['dwc:informationWithheld']:
        return 'private'
    else:
        return 'open'


def _format_dwc_license(dwc_record: dict) -> Optional[str]:
    """Format a CC license URL to a license code"""
    license = dwc_record.get('dcterms:license')
    if not license:
        return None

    if isinstance(license, dict):
        license = next(iter(license.values()), None)
        if not license:
            return None
    if match := CC_URL_PATTERN.match(license):
        license = f'CC-{match.groups()[0]}'.upper()
    return license


def _format_dwc_location(dwc_record: dict) -> Optional[Coordinates]:
    location = try_float_pair(
        dwc_record.get('dwc:decimalLatitude'),
        dwc_record.get('dwc:decimalLongitude'),
    )
    return list(location) if location else None
