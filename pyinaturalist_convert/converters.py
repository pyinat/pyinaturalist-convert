"""Base utilities for converting observation data to common formats

.. automodsumm:: pyinaturalist_convert.converters
   :functions-only:
   :nosignatures:
"""
import json
from copy import deepcopy
from logging import getLogger
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Type, Union

import numpy as np
import pandas as pd
from flatten_dict import flatten, unflatten
from pandas import DataFrame
from pyinaturalist import BaseModel, JsonResponse, ModelObjects, Observation, ResponseResult, Taxon
from requests import Response
from tablib import Dataset

# TODO: Readme examples for read()
# TODO: Flatten annotations and ofvs into top-level {term_label: value_label} fields
# TODO: to_csv(): Maybe try to keep simple ID lists and manually parse when reading CSV?
# TODO: dict lists not returning correctly when reading parquet and feather
# TODO: Better condensed format for simplify_observations() that still works with parquet
# TODO: For large datasets that require more than one conversion step, chained generators would be
# useful to minimize memory usage from intermediate variables.

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
PANDAS_FORMATS = ['csv', 'feather', 'hdf', 'parquet', 'xlsx']

CollectionTypes = Union[DataFrame, Dataset, Response, JsonResponse, Iterable[ResponseResult]]
InputTypes = Union[CollectionTypes, ModelObjects]
AnyObservations = Union[CollectionTypes, Observation, Iterable[Observation]]
AnyTaxa = Union[CollectionTypes, Taxon, Iterable[Taxon]]
PathOrStr = Union[Path, str]

logger = getLogger(__name__)


def to_observations(value: InputTypes) -> Iterable[Observation]:
    """Convert any supported input type into Observation objects. Input types include:

    * :py:class:`pandas.DataFrame`
    * :py:class:`tablib.Dataset`
    * :py:class:`requests.Response`
    * API response JSON
    """
    return _to_models(value, Observation)


def to_taxa(value: InputTypes) -> Iterable[Taxon]:
    """Convert any supported input type into Taxon objects"""
    return _to_models(value, Taxon)


def _to_models(value: InputTypes, model: Type[BaseModel] = Observation) -> Iterable[BaseModel]:
    """Convert any supported input type into a list of Observation (or other record type) objects"""
    # If the value already contains model object(s), don't convert them to dicts and back to models
    if isinstance(value, BaseModel):
        return [value]
    elif isinstance(value, Sequence) and isinstance(value[0], BaseModel):
        return value
    else:
        return model.from_json_list(to_dicts(value))


def to_dicts(value: InputTypes) -> Iterable[Dict]:
    """Convert any supported input type into a observation (or other record type) dicts"""
    if not value:
        return []
    if isinstance(value, DataFrame):
        return _df_to_dicts(value)
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
        return value
    else:
        return [value]


def to_csv(observations: AnyObservations, filename: str = None):
    """Convert observations to CSV"""
    df = pd.DataFrame(flatten_observations(observations, tabular=True))
    df.to_csv(filename, index=False)


def to_dataframe(observations: AnyObservations):
    """Convert observations into a pandas DataFrame"""
    flat_observations = flatten_observations(observations, semitabular=True)
    return pd.DataFrame(flat_observations).dropna(axis=1, how='all')


def to_dataset(observations: AnyObservations) -> Dataset:
    """Convert observations to a generic tabular dataset. This can be converted to any of the
    `formats supported by tablib <https://tablib.readthedocs.io/en/stable/formats.html>`_.
    """
    if isinstance(observations, Dataset):
        return observations

    flat_observations = flatten_observations(observations, semitabular=True)
    dataset = Dataset()
    headers, flat_observations = _fix_dimensions(flat_observations)
    dataset.headers = headers
    dataset.extend([[item[k] for k in headers] for item in flat_observations])
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


def to_json(observations: AnyObservations, filename: str):
    write(json.dumps(observations, indent=2, default=str), filename)


def to_parquet(observations: AnyObservations, filename: str):
    """Convert observations into a parquet file"""
    df = to_dataframe(observations)
    df.to_parquet(filename)


def read(filename: PathOrStr) -> List[Observation]:
    """Load observations from any supported file format
    This code also serves as reference for how to load observations from various formats.

    Note: For CSV files from the iNat export tool, use :py:func:`.load_csv_exports` instead.
    """
    from .csv import is_csv_export, load_csv_exports

    file_path = Path(filename).expanduser()
    ext = file_path.suffix.lower().replace('.', '')
    if ext == 'json':
        return Observation.from_json_file(file_path)
    # For CSV, check if it came from the export tool or from API results
    elif ext == 'csv' and is_csv_export(file_path):
        df = load_csv_exports(file_path)
    elif ext == 'csv':
        df = pd.read_csv(file_path)
    elif ext == 'feather':
        df = pd.read_feather(file_path)
    elif ext == 'hdf':
        df = pd.read_hdf(file_path, 'observations')
    elif ext == 'parquet':
        df = pd.read_parquet(file_path)
    elif ext == 'xlsx':
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f'File format not yet supported: {file_path.suffix}')

    return Observation.from_json_list(_df_to_dicts(df))


def write(content: Union[str, bytes], filename: PathOrStr, mode='w'):
    """Write converted observation data to a file, creating parent dirs first"""
    logger.info(f'Writing to {filename}')
    file_path = Path(filename).expanduser()
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open(mode) as f:
        f.write(content)
        # Ensure trailing newline
        if isinstance(content, str) and not content.endswith('\n'):
            f.write('\n')


def flatten_observations(
    observations: AnyObservations, tabular: bool = False, semitabular: bool = False
):
    """Flatten nested dict attributes, for example ``{"taxon": {"id": 1}} -> {"taxon.id": 1}``

    Args:
        semitabular: Accept one level of nested collections, for formats that can handle them
            (like parquet)
        tabular: Drop all collections that can't be flattened (for CSV)
    """
    observations = to_dicts(observations)
    if tabular:
        observations = _drop_observation_lists(observations)
    elif semitabular:
        observations = _flatten_observation_lists(observations)
    return [flatten(obs, reducer='dot') for obs in observations]


def _df_to_dicts(df: 'DataFrame') -> List[JsonResponse]:
    """Convert a pandas DataFrame into nested dicts (similar to API response JSON)"""
    df = df.replace([np.nan], [None])
    observations = [unflatten(flat_dict, splitter='dot') for flat_dict in df.to_dict('records')]
    # TODO: Handle array types in Observation.from_json()
    for obs in observations:
        coords = obs.get('location')
        obs['location'] = tuple(coords) if coords is not None else None
    return observations


def _drop_observation_lists(observations: Iterable[Dict]) -> List[ResponseResult]:
    """Drop list fields, which can't easily be represented in CSV"""

    def _drop(obs):
        photos = obs.get('photos', [])
        obs['photo_url'] = photos[0]['url'] if photos else None
        taxon = obs['taxon']
        obs['taxon']['parent_id'] = taxon['ancestor_ids'][-1] if taxon.get('ancestor_ids') else None
        if obs.get('location'):
            obs['latitude'] = obs['location'][0]
            obs['longitude'] = obs['location'][1]
        return {k: v for k, v in obs.items() if not isinstance(v, (list, tuple))}

    return [_drop(obs) for obs in observations]


def _flatten_observation_lists(observations: Iterable[Dict]) -> List[ResponseResult]:
    """Flatten out some nested data structures within observation records:

    * annotations
    * comments
    * identifications
    * observation field values
    * photos
    """

    def _flatten(obs: Dict):
        # Reduce annotations to IDs and values
        obs = deepcopy(obs)
        obs['annotations'] = [
            {str(a['controlled_attribute_id']): a['controlled_value_id']}
            for a in obs.get('annotations', [])
        ]

        # Reduce identifications to identification IDs and taxon IDs
        obs['identifications'] = [
            {str(i['id']): i['taxon_id']} for i in obs.get('identifications', [])
        ]

        # Reduce comments to usernames and comment text
        obs['comments'] = [{c['user']['login']: c['body']} for c in obs.get('comments', [])]

        # Reduce photos to IDs and URLs, and add first photo URL as a top-level field
        photos = obs.get('photos', [])
        obs['photos'] = [{str(p['id']): p['url']} for p in photos]
        obs['photo_url'] = photos[0]['url'] if photos else None

        # Reduce observation field values to field IDs and values
        obs['ofvs'] = {str(ofv['field_id']): ofv['value'] for ofv in obs.get('ofvs', [])}

        # Drop some (typically) redundant collections
        obs.pop('observation_photos', None)
        obs.pop('non_owner_ids', None)
        return obs

    return [_flatten(obs) for obs in observations]


def _fix_dimensions(flat_observations):
    """Add missing fields to ensure dimensions are consistent"""
    optional_fields = ['taxon.complete_rank', 'taxon.preferred_common_name']
    headers = list(set(flat_observations[0].keys()) | set(optional_fields))
    for obs in flat_observations:
        for field in headers:
            obs.setdefault(field, None)
    return sorted(headers), flat_observations
