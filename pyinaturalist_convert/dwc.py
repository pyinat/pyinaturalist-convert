"""Incomplete outline of mapping iNaturalist fields to DwC terms"""
import xmltodict
from pyinaturalist import get_observations, get_taxa_by_id

# Fields from observation JSON
OBSERVATION_FIELDS = {
    'id': 'dwc:catalogNumber',
    'observed_on': 'dwc:eventDate',
    # ...
}

# Fields from taxon JSON
TAXON_FIELDS = {
    'id': 'dwc:taxonID',
    'name': 'dwc:scientificName',
    # ...
}

# Fields from items in observation['photos']
PHOTO_FIELDS = {
    'url': 'dcterms:identifier',
    'license_code': 'xap:UsageTerms',  # Will need to be translated to a link to creativecommons.org
    # ...
}

# Fields that will be constant for all iNaturalist observations
CONSTANTS = {
    'dwc:basisOfRecord': 'HumanObservation',
    'dwc:institutionCode': 'iNaturalist',
    # ...
}


def observation_to_dwc(observation) -> str:
    """Translate a JSON-formatted observation record from API results to DwC format"""
    # Translate observation fields
    dwc_observation = {}
    for inat_field, dwc_field in OBSERVATION_FIELDS.items():
        dwc_observation[dwc_field] = observation[inat_field]

    # Translate taxon fields
    taxon = get_taxon_with_ancestors(observation)
    for inat_field, dwc_field in TAXON_FIELDS.items():
        dwc_observation[dwc_field] = taxon[inat_field]

    # Translate photo fields
    photo = observation['photos'][0]
    dwc_photo = {}
    for inat_field, dwc_field in PHOTO_FIELDS.items():
        dwc_photo[dwc_field] = photo[inat_field]
    dwc_observation['eol:dataObject'] = dwc_photo

    # Add constants
    for dwc_field, value in CONSTANTS.items():
        dwc_observation[dwc_field] = value

    # Add to a complete SimpleDarwinRecordSet and convert to XML
    dwc_records = {'dwr:SimpleDarwinRecordSet': {'dwr:SimpleDarwinRecord': dwc_observation}}
    return xmltodict.unparse(dwc_records, pretty=True)


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


def test_observation_to_dwc():
    """Get a test observation, convert it to DwC, and write it to a file"""
    response = get_observations(id=45524803)
    observation = response['results'][0]
    dwc_xml = observation_to_dwc(observation)
    with open('obs_45524803.dwc', 'w') as f:
        f.write(dwc_xml)


if __name__ == '__main__':
    test_observation_to_dwc()
