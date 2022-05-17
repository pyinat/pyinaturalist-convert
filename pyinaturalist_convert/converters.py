"""Base utilities for converting observation data to common formats"""
from copy import deepcopy
from logging import getLogger
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Type, Union

import numpy as np
import pandas as pd
from flatten_dict import flatten, unflatten
from pandas import DataFrame
from pyinaturalist import BaseModel, JsonResponse, ModelObjects, Observation, ResponseResult, Taxon
from requests import Response
from tablib import Dataset

# TODO: Use Observation model to do most of the cleanup (e.g., for _fix_dimensions())
# TODO: Better condensed format for simplify_observations() that still works with parquet
# TODO: For large datasets that require more than one conversion step, chained generators would be
# useful to minimize memory useage from intermediate variables.

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
PANDAS_FORMATS = ['csv', 'feather', 'hdf', 'parquet', 'sql']

CollectionTypes = Union[Dataset, Response, JsonResponse, Iterable[ResponseResult]]
InputTypes = Union[CollectionTypes, ModelObjects]
AnyObservations = Union[CollectionTypes, Observation, Iterable[Observation]]
AnyTaxa = Union[CollectionTypes, Taxon, Iterable[Taxon]]
PathOrStr = Union[Path, str]

logger = getLogger(__name__)


def to_dicts(value: InputTypes) -> List[Dict]:
    """Convert any supported input type into a list of observation (or other record type) dicts"""
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
    elif isinstance(value, Sequence) and isinstance(value[0], BaseModel):
        return [v.to_dict() for v in value]
    elif isinstance(value, Sequence):
        return list(value)
    else:
        return [value]


def to_models(value: InputTypes, model: Type[BaseModel] = Observation) -> List[BaseModel]:
    """Convert any supported input type into a list of Observation (or other record type) objects"""
    if isinstance(value, BaseModel):
        return [value]
    elif isinstance(value, Sequence) and isinstance(value[0], BaseModel):
        return list(value)
    else:
        return model.from_json_list(to_dicts(value))


def to_observations(value: InputTypes) -> List[Observation]:
    """Convert any supported input type into a list of Observation objects"""
    return to_models(value, Observation)


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
    return pd.json_normalize(simplify_observations(observations))


def to_dataset(observations: AnyObservations) -> Dataset:
    """Convert observations to a generic tabular dataset. This can be converted to any of the
    `formats supported by tablib <https://tablib.readthedocs.io/en/stable/formats.html>`_.
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


def df_to_dicts(df: 'DataFrame') -> List[JsonResponse]:
    """Convert a pandas DataFrame into nested dicts (similar to API response JSON)"""
    df = df.replace([np.nan], [None])
    return [unflatten(flat_dict, splitter='dot') for flat_dict in df.to_dict('records')]


def read(filename: PathOrStr) -> List[Observation]:
    """Load observations from any supported file format
    This code also serves as reference for how to load observations from various formats.

    Note: For CSV files from the iNat export tool, use :py:func:`.load_csv_exports` instead.
    """

    file_path = Path(filename).expanduser()
    ext = file_path.suffix.lower().replace('.', '')
    if ext == 'json':
        return Observation.from_json_file(file_path)
    elif ext in PANDAS_FORMATS:
        return _read_pd_formats(file_path, ext)
    else:
        raise ValueError(f'File format not yet supported: {file_path.suffix}')


# TODO: If CSV, inspect if it's from the iNat export tool and use load_csv_exports instead
def _read_pd_formats(file_path: Path, ext: str):
    if file_path.suffix == 'csv':
        df = pd.read_csv(file_path)
    elif file_path.suffix == 'feather':
        df = pd.read_feather(file_path)
    elif file_path.suffix == 'hdf':
        df = pd.read_hdf(file_path, 'observations')
    elif file_path.suffix == 'parquet':
        df = pd.read_parquet(file_path)
    elif file_path.suffix == 'xlsx':
        df = pd.read_excel(file_path)
    else:
        return []
    return Observation.from_json_list(df_to_dicts(df))


def write(content: Union[str, bytes], filename: PathOrStr, mode='w'):
    """Write converted observation data to a file, creating parent dirs first"""
    logger.info(f'Writing to {filename}')
    file_path = Path(filename).expanduser()
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open(mode) as f:
        f.write(content)
        if isinstance(content, str) and not content.endswith('\n'):
            f.write('\n')


def flatten_observations(observations: AnyObservations, flatten_lists: bool = False):
    if flatten_lists:
        observations = simplify_observations(observations)
    return [flatten(obs, reducer='dot') for obs in to_dicts(observations)]


def flatten_observation(observation: ResponseResult, flatten_lists: bool = False):
    if flatten_lists:
        observation = _simplify_observation(observation)
    return flatten(observation, reducer='dot')


def simplify_observations(observations: AnyObservations) -> List[ResponseResult]:
    """Flatten out some nested data structures within observation records:

    * annotations
    * comments
    * identifications
    * first photo URL
    """
    return [_simplify_observation(o) for o in to_dicts(observations)]


def _simplify_observation(obs):
    # Reduce annotations to IDs and values
    obs = deepcopy(obs)
    obs['annotations'] = [
        {str(a['controlled_attribute_id']): a['controlled_value_id']}
        for a in obs.get('annotations', [])
    ]

    # Reduce identifications to identification IDs and taxon IDs
    obs['identifications'] = [{str(i['id']): i['taxon_id']} for i in obs.get('identifications', [])]

    # Reduce comments to usernames and comment text
    obs['comments'] = [{c['user']['login']: c['body']} for c in obs.get('comments', [])]

    # Add first observation photo as a top-level field
    obs['photo_url'] = obs.get('photos', [{}])[0].get('url')

    # Drop some (typically) redundant collections
    obs.pop('observation_photos', None)
    obs.pop('non_owner_ids', None)

    return obs


def _fix_dimensions(flat_observations):
    """Temporary ugly hack to work around missing fields in some observations"""
    optional_fields = ['taxon.complete_rank', 'taxon.preferred_common_name']
    headers = set(flat_observations[0].keys()) | set(optional_fields)
    for obs in flat_observations:
        for field in optional_fields:
            obs.setdefault(field, None)
    return headers, flat_observations
