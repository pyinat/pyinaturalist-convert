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
import sqlite3
from csv import DictReader
from glob import glob
from logging import getLogger
from os.path import basename
from pathlib import Path
from typing import TYPE_CHECKING

from pyinaturalist import JsonResponse
from pyinaturalist.constants import RANKS
from pyinaturalist.converters import try_datetime

from .constants import DB_PATH, PathOrStr
from .converters import to_dataframe

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

PHOTO_ID_PATTERN = re.compile(r'.*photos/(.*)/.*\.(?:\w+)')

# DbObservation columns that can be directly populated from a CSV export row
_DB_OBS_COLUMNS = [
    'id',
    'captive',
    'created_at',
    'description',
    'geoprivacy',
    'identifications_count',
    'latitude',
    'license_code',
    'longitude',
    'observed_on',
    'place_guess',
    'positional_accuracy',
    'quality_grade',
    'taxon_id',
    'updated_at',
    'user_id',
    'user_login',
]

logger = getLogger(__name__)


def csv_export_to_db(
    csv_path: PathOrStr,
    db_path: PathOrStr = DB_PATH,
    batch_size: int = 1000,
) -> int:
    """Import an iNaturalist CSV export directly into a SQLite database.

    Much faster than the equivalent two-step process of loading into a DataFrame and then saving
    Observation objects, because it avoids the intermediate in-memory representation and goes
    row-by-row.

    Also calls :py:func:`~pyinaturalist_convert.db.create_tables` if the DB doesn't yet have the
    expected schema.

    Args:
        csv_path: Path to the iNaturalist bulk export CSV file
        db_path: Path to the SQLite database file (created if it doesn't exist)
        batch_size: Number of rows to insert per batch

    Returns:
        Number of observations inserted
    """
    from .db import create_tables

    create_tables(db_path)

    columns = _DB_OBS_COLUMNS
    placeholders = ', '.join('?' * len(columns))
    col_names = ', '.join(columns)
    insert_sql = f'INSERT OR REPLACE INTO observation ({col_names}) VALUES ({placeholders})'

    total = 0
    batch: list[tuple] = []

    with (
        open(csv_path, encoding='utf-8') as csv_file,
        sqlite3.connect(db_path) as conn,
    ):
        reader = DictReader(csv_file)
        for row in reader:
            db_dict = _csv_row_to_db_dict(row)
            batch.append(tuple(db_dict[col] for col in columns))

            if len(batch) >= batch_size:
                conn.executemany(insert_sql, batch)
                total += len(batch)
                batch = []

        if batch:
            conn.executemany(insert_sql, batch)
            total += len(batch)

        conn.commit()

    logger.info(f'Inserted {total} observations from {csv_path} into {db_path}')
    return total


def _csv_row_to_db_dict(row: dict) -> dict:
    """Map a single raw CSV export row to a dict of DbObservation column values."""

    def _int_or_none(val: str) -> int | None:
        try:
            return int(val) if val else None
        except (ValueError, TypeError):
            return None

    def _float_or_none(val: str) -> float | None:
        try:
            return float(val) if val else None
        except (ValueError, TypeError):
            return None

    agreements = _int_or_none(row.get('num_identification_agreements')) or 0
    disagreements = _int_or_none(row.get('num_identification_disagreements')) or 0

    captive_str = row.get('captive_cultivated', '').strip().lower()
    captive: bool | None = (
        True if captive_str == 'true' else (False if captive_str == 'false' else None)
    )

    return {
        'id': _int_or_none(row.get('id')),
        'captive': captive,
        'created_at': row.get('created_at') or None,
        'description': row.get('description') or None,
        'geoprivacy': row.get('geoprivacy') or None,
        'identifications_count': agreements + disagreements,
        'latitude': _float_or_none(row.get('latitude')),
        'license_code': row.get('license') or None,
        'longitude': _float_or_none(row.get('longitude')),
        'observed_on': row.get('observed_on') or None,
        'place_guess': row.get('place_guess') or None,
        'positional_accuracy': _int_or_none(row.get('positional_accuracy')),
        'quality_grade': row.get('quality_grade') or None,
        'taxon_id': _int_or_none(row.get('taxon_id')),
        'updated_at': row.get('updated_at') or None,
        'user_id': _int_or_none(row.get('user_id')),
        'user_login': row.get('user_login') or None,
    }


def load_csv_exports(*file_paths: PathOrStr) -> 'DataFrame':
    """Read one or more CSV files from the Nat export tool into a dataframe

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
    import csv

    with open(file_path, encoding='utf-8') as f:
        headers = next(csv.reader(f))
    # Just check for a field name that's only in the export and not in API results
    return 'captive_cultivated' in headers


def _resolve_file_paths(*file_paths: PathOrStr) -> list[Path]:
    """Given file paths and/or glob patterns, return a list of resolved file paths"""
    resolved_paths = []
    for p in file_paths:
        if '*' in str(p):
            resolved_paths.extend(glob(str(p)))
        else:
            resolved_paths.append(str(p))
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
