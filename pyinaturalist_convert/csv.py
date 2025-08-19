"""Import CSV observation data from the
`iNaturalist bulk export tool <https://www.inaturalist.org/observations/export>`_.

**Extra dependencies**: ``pandas`` (for fast conversion into API-compatible results)

**Example:**

    Load CSV file into a dataframe:

    >>> from pyinaturalist_convert import load_csv_exports
    >>> df = load_csv_exports('~/Downloads/my_observations.csv')

**Note:** This format can also be loaded by :py:func:`~pyinaturalist_convert.converters.read()`:

**Example:**

    Load CSV file into Observation objects:

    >>> from pyinaturalist_convert import read
    >>> observations = load_csv_exports('~/Downloads/my_observations.csv')
"""

import re
from csv import DictReader
from glob import glob
from logging import getLogger
from os.path import basename
from pathlib import Path
from typing import TYPE_CHECKING, List

from pyinaturalist import JsonResponse
from pyinaturalist.constants import RANKS
from pyinaturalist.converters import try_datetime

from .converters import PathOrStr, to_dataframe

if TYPE_CHECKING:
    from pandas import DataFrame

# Explicit datatypes for columns loaded from CSV
DTYPES = {
    'obscured': bool,
    'id': int,
    'latitude': float,
    'longitude': float,
    'num_identification_agreements': int,
    'num_identification_disagreements': int,
    'photo.id': int,
    'photo.iqa_aesthetic': float,
    'photo.iqa_technical': float,
    'positional_accuracy': float,
    'public_positional_accuracy': float,
    'taxon.id': int,
    'user.activity_count': int,
    'user.iconic_taxon_identifications_count': int,
    'user.iconic_taxon_rg_observations_count': int,
    'user.id': int,
    'user.identifications_count': int,
    'user.journal_posts_count': int,
    'user.observations_count': int,
    'user.site_id': int,
    'user.species_count': int,
    'user.suspended': bool,
    # TODO: Convert datetimes to UTC and datetime64
    # 'observed_on': 'datetime64',
    # 'created_at': 'datetime64',
    # 'updated_at': 'datetime64',
    # 'user.created_at': 'datetime64',
}

# Columns to drop
DROP_COLUMNS = [
    'cached_votes_total',
    'flags',
    'oauth_application_id',
    'observed_on_string',
    'positioning_method',
    'positioning_device',
    'scientific',
    'spam',
    'time_observed_at',
    'time_zone',
    'user.spam',
    'user.suspended',
    'user.universal_search_rank',
]

# Columns from CSV export to rename to match API response
RENAME_COLUMNS = {
    'common_name': 'taxon.preferred_common_name',
    'coordinates_obscured': 'obscured',
    'license': 'license_code',
    'taxon_': 'taxon.',
    'url': 'uri',
    'image_uri': 'photo.url',
    'sound_uri': 'sound.url',
    'user_': 'user.',
    '_name': '',
}

PHOTO_ID_PATTERN = re.compile(r'.*photos/(.*)/.*\.(\w+)')

logger = getLogger(__name__)


# TODO: Use pandas if installed, otherwise fallback to tablib?
def load_csv_exports(*file_paths: PathOrStr) -> 'DataFrame':
    """Read one or more CSV files from ithe Nat export tool into a dataframe

    Args:
        file_paths: One or more file paths or glob patterns to load
    """
    import pandas as pd

    resolved_paths = _resolve_file_paths(*file_paths)
    logger.info(
        f'Reading {len(resolved_paths)} exports:\n'
        + '\n'.join([f'\t{basename(f)}' for f in resolved_paths])
    )

    df = pd.concat((pd.read_csv(f) for f in resolved_paths), ignore_index=True)
    return _format_export(df)


def is_csv_export(file_path: PathOrStr) -> bool:
    """Check if a file is a CSV export from the iNaturalist export tool (to distinguish from
    converted API results)
    """
    with open(file_path, encoding='utf-8') as f:
        reader = DictReader(f)
        fields = next(reader).keys()
    # Just check for a field name that's only in the export and not in API results
    return 'captive_cultivated' in fields


def _resolve_file_paths(*file_paths: PathOrStr) -> List[Path]:
    """Given file paths and/or glob patterns, return a list of resolved file paths"""
    file_path_strs = [str(p) for p in file_paths]
    resolved_paths = [p for p in file_path_strs if '*' not in p]
    for path in [p for p in file_path_strs if '*' in p]:
        resolved_paths.extend(glob(path))
    return [Path(p).expanduser() for p in resolved_paths]


def _format_columns(df: 'DataFrame') -> 'DataFrame':
    """Some datatype conversions that apply to both CSV exports and API response data"""
    # Convert to expected datatypes
    for col, dtype in DTYPES.items():
        if col in df:
            df[col] = df[col].fillna(dtype()).astype(dtype)

    # Drop any empty columns
    df = df.dropna(axis=1, how='all')
    return df.fillna('')


def _format_response(response: JsonResponse) -> 'DataFrame':
    """Convert and format API response data into a dataframe"""
    df = to_dataframe(response['results'])
    df['photo.url'] = df['photos'].apply(lambda x: x[0]['url'])
    df['photo.id'] = df['photos'].apply(lambda x: x[0]['id'])
    df = _format_columns(df)
    return df


def _format_export(df: 'DataFrame') -> 'DataFrame':
    """Format an exported CSV file to be more consistent with API response format"""
    logger.info(f'Formatting {len(df)} observation records')

    # Rename, convert, and drop selected columns
    df = df.rename(columns={col: _rename_column(col) for col in sorted(df.columns)})
    df = _format_columns(df)

    # Convert datetimes
    df['observed_on'] = df['observed_on_string'].apply(lambda x: try_datetime(x) or x)
    df['created_at'] = df['created_at'].apply(lambda x: try_datetime(x) or x)
    df['updated_at'] = df['updated_at'].apply(lambda x: try_datetime(x) or x)

    # Fill out taxon name and rank
    df['taxon.rank'] = df.apply(_get_min_rank, axis=1)
    df['taxon.name'] = df.apply(lambda x: x.get(f'taxon.{x["taxon.rank"]}'), axis=1)

    # Format coordinates
    df['location'] = df.apply(lambda x: [x['latitude'], x['longitude']], axis=1)
    df = df.drop(columns=['latitude', 'longitude'])

    # Add some other missing columns
    df['photo.id'] = df['photo.url'].apply(_get_photo_id)

    # Drop unused columns
    df = df.drop(columns=[k for k in DROP_COLUMNS if k in df])
    return df


def _get_min_rank(series):
    for rank in RANKS:
        if series.get(f'taxon.{rank}'):
            return rank
    return ''


def _get_photo_id(image_url):
    """Get a photo ID from its URL (for CSV exports, which only include a URL)"""
    match = re.match(PHOTO_ID_PATTERN, str(image_url))
    return match.group(1) if match else ''


def _rename_column(col):
    for str_1, str_2 in RENAME_COLUMNS.items():
        col = col.replace(str_1, str_2)
    return col
