"""Base utilities for converting observation data to alternative formats"""
from copy import deepcopy
from logging import getLogger
from os import makedirs
from os.path import dirname, expanduser
from typing import List, Sequence, Union

import tabulate
from flatten_dict import flatten
from pyinaturalist.constants import ResponseOrResults, ResponseResult
from pyinaturalist.models import Observation  # noqa
from requests import Response
from tablib import Dataset

# from pyinaturalist.formatters import simplify_observations

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

AnyObservation = Union[Dataset, Observation, Response, ResponseOrResults]
logger = getLogger(__name__)


# TODO: Handle Obervation model objects
# TODO: Handle reuqests.Respose objects
def ensure_list(obj: AnyObservation) -> List:
    if isinstance(obj, Dataset):
        return obj
    if isinstance(obj, dict) and 'results' in obj:
        obj = obj['results']
    if isinstance(obj, Sequence):
        return list(obj)
    else:
        return [obj]


def flatten_list(observations: AnyObservation):
    return [flatten(obs, reducer='dot') for obs in ensure_list(observations)]


def to_csv(observations: AnyObservation, filename: str = None) -> str:
    """Convert observations to CSV"""
    csv_observations = to_dataset(observations).get_csv()
    if filename:
        write(csv_observations, filename)
    return csv_observations


def to_dataframe(observations: AnyObservation):
    """Convert observations into a pandas DataFrame"""
    import pandas as pd

    return pd.json_normalize(simplify_observations(observations))


def to_dataset(observations: AnyObservation) -> Dataset:
    """Convert observations to a generic tabular dataset. This can be converted to any of the
    `formats supported by tablib <https://tablib.readthedocs.io/en/stable/formats>`_.
    """
    if isinstance(observations, Dataset):
        return observations

    flat_observations = [flatten(obs, reducer='dot') for obs in simplify_observations(observations)]
    dataset = Dataset()
    headers, flat_observations = _fix_dimensions(flat_observations)
    dataset.headers = headers
    dataset.extend([item.values() for item in flat_observations])
    return dataset


def to_excel(observations: AnyObservation, filename: str):
    """Convert observations to an Excel spreadsheet (xlsx)"""
    xlsx_observations = to_dataset(observations).get_xlsx()
    write(xlsx_observations, filename, 'wb')


def to_feather(observations: AnyObservation, filename: str):
    """Convert observations into a feather file"""
    df = to_dataframe(observations)
    df.to_feather(filename)


def to_hdf(observations: AnyObservation, filename: str):
    """Convert observations into a HDF5 file"""
    df = to_dataframe(observations)
    df.to_hdf(filename, 'observations')


def to_parquet(observations: AnyObservation, filename: str):
    """Convert observations into a parquet file"""
    df = to_dataframe(observations)
    df.to_parquet(filename)


def simplify_observations(observations: AnyObservation) -> List[ResponseResult]:
    """Flatten out some nested data structures within observation records:

    * annotations
    * comments
    * identifications
    * non-owner IDs
    """
    return [_simplify_observation(o) for o in ensure_list(observations)]


def write(content, filename, mode='w'):
    """Write converted observation data to a file, creating parent dirs first"""
    filename = expanduser(filename)
    logger.info(f'Writing to {filename}')
    if dirname(filename):
        makedirs(dirname(filename), exist_ok=True)
    with open(filename, mode) as f:
        f.write(content)


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

    return obs


def _fix_dimensions(flat_observations):
    """Temporary ugly hack to work around missing fields in some observations"""
    # TODO: Use Observation model to get headers instead?
    optional_fields = ['taxon.complete_rank', 'taxon.preferred_common_name']
    headers = set(flat_observations[0].keys()) | set(optional_fields)
    for obs in flat_observations:
        for field in optional_fields:
            obs.setdefault(field, None)
    return headers, flat_observations
