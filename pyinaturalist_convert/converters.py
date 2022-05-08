"""Base utilities for converting observation data to alternative formats"""
from copy import deepcopy
from logging import getLogger
from os import makedirs
from os.path import dirname, expanduser
from typing import Dict, Iterable, List, Optional, Sequence, Union

import tabulate
from flatten_dict import flatten
from pyinaturalist import BaseModel, JsonResponse, ModelObjects, Observation, ResponseResult, Taxon
from requests import Response
from tablib import Dataset

TABLIB_FORMATS = [
    'csv',
    'html',
    'jira',
    'json',
    'latex',
    'ods',
    'rst',
    'tsv',
    'xls',
    'xlsx',
    'yaml',
]
TABULATE_FORMATS = sorted(set(tabulate._table_formats) - set(TABLIB_FORMATS))  # type: ignore
PANDAS_FORMATS = ['feather', 'gbq', 'hdf', 'parquet', 'sql', 'xarray']

CollectionTypes = Union[Dataset, Response, JsonResponse, Iterable[ResponseResult]]
InputTypes = Union[CollectionTypes, ModelObjects]
AnyObservations = Union[CollectionTypes, Observation, Iterable[Observation]]
AnyTaxa = Union[CollectionTypes, Taxon, Iterable[Taxon]]

logger = getLogger(__name__)


def to_dict_list(value: InputTypes) -> List[Dict]:
    """Convert any supported input type into a list of observation dicts"""
    if not value:
        return []
    if isinstance(value, Dataset):
        return value.dict
    if isinstance(value, Response):
        value = value.json()
    if isinstance(value, dict) and 'results' in value:
        value = value['results']
    if isinstance(value, BaseModel):
        return [value.to_dict()]
    if isinstance(value, Sequence) and isinstance(value[0], BaseModel):
        return [v.to_dict() for v in value]
    if isinstance(value, Sequence):
        return list(value)
    else:
        return [value]


def flatten_observations(observations: AnyObservations, flatten_lists: bool = False):
    if flatten_lists:
        observations = simplify_observations(observations)
    return [flatten(obs, reducer='dot') for obs in to_dict_list(observations)]


def flatten_observation(observation: ResponseResult, flatten_lists: bool = False):
    if flatten_lists:
        observation = _simplify_observation(observation)
    return flatten(observation, reducer='dot')


def to_csv(observations: AnyObservations, filename: str = None) -> Optional[str]:
    """Convert observations to CSV"""
    csv_observations = to_dataset(observations).get_csv()
    if filename:
        write(csv_observations, filename)
        return None
    else:
        return csv_observations


def to_dataframe(observations: AnyObservations):
    """Convert observations into a pandas DataFrame"""
    import pandas as pd

    return pd.json_normalize(simplify_observations(observations))


def to_dataset(observations: AnyObservations) -> Dataset:
    """Convert observations to a generic tabular dataset. This can be converted to any of the
    `formats supported by tablib <https://tablib.readthedocs.io/en/stable/formats>`_.
    """
    if isinstance(observations, Dataset):
        return observations

    flat_observations = flatten_observations(observations, flatten_lists=True)
    dataset = Dataset()
    headers, flat_observations = _fix_dimensions(flat_observations)
    dataset.headers = headers
    dataset.extend([item.values() for item in flat_observations])
    return dataset


def to_excel(observations: AnyObservations, filename: str):
    """Convert observations to an Excel spreadsheet (xlsx)"""
    xlsx_observations = to_dataset(observations).get_xlsx()
    write(xlsx_observations, filename, 'wb')


def to_feather(observations: AnyObservations, filename: str):
    """Convert observations into a feather file"""
    df = to_dataframe(observations)
    df.to_feather(filename)


def to_hdf(observations: AnyObservations, filename: str):
    """Convert observations into a HDF5 file"""
    df = to_dataframe(observations)
    df.to_hdf(filename, 'observations')


def to_parquet(observations: AnyObservations, filename: str):
    """Convert observations into a parquet file"""
    df = to_dataframe(observations)
    df.to_parquet(filename)


def to_observation_objs(value: AnyObservations) -> List[Observation]:
    """Convert any supported input type into a list of Observation objects"""
    return Observation.from_json_list(to_dict_list(value))


def simplify_observations(observations: AnyObservations) -> List[ResponseResult]:
    """Flatten out some nested data structures within observation records:

    * annotations
    * comments
    * identifications
    * non-owner IDs
    """
    return [_simplify_observation(o) for o in to_dict_list(observations)]


def write(content, filename, mode='w'):
    """Write converted observation data to a file, creating parent dirs first"""
    filename = expanduser(filename)
    logger.info(f'Writing to {filename}')
    if dirname(filename):
        makedirs(dirname(filename), exist_ok=True)
    with open(filename, mode) as f:
        f.write(content)
        if not content.endswith('\n'):
            f.write('\n')


def _simplify_observation(obs):
    # Reduce annotations to IDs and values
    obs = deepcopy(obs)
    obs['annotations'] = [
        {str(a['controlled_attribute_id']): a['controlled_value_id']} for a in obs['annotations']
    ]

    # Reduce identifications to just a list of identification IDs and taxon IDs
    # TODO: Better condensed format that still works with parquet
    obs['identifications'] = [{str(i['id']): i['taxon_id']} for i in obs['identifications']]
    obs['non_owner_ids'] = [{str(i['id']): i['taxon_id']} for i in obs['non_owner_ids']]

    # Reduce comments to usernames and comment text
    obs['comments'] = [{c['user']['login']: c['body']} for c in obs['comments']]
    del obs['observation_photos']

    # Add first observation photo as a top-level field
    photos = obs.get('photos', [{}])
    obs['photo_url'] = photos[0].get('url')
    return obs


# TODO: Use Observation model to do most of this
def _fix_dimensions(flat_observations):
    """Temporary ugly hack to work around missing fields in some observations"""
    optional_fields = ['taxon.complete_rank', 'taxon.preferred_common_name']
    headers = set(flat_observations[0].keys()) | set(optional_fields)
    for obs in flat_observations:
        for field in optional_fields:
            obs.setdefault(field, None)
    return headers, flat_observations
