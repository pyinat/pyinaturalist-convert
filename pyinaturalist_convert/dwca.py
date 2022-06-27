"""Download and convert the
`iNaturalist GBIF and taxonomy datasets <https://www.inaturalist.org/pages/developers>`_
from DwC-A to SQLite.

**Extra dependencies**: ``sqlalchemy``

**Example**: Download everything and load into a SQLite database::

    >>> from pyinaturalist_convert import load_dwca_tables
    >>> load_dwca_tables()

.. note::
    By default, data is saved in the recommended platform-specific data directory, for example
    ``~\\AppData\\Local\\`` on Windows, or ``~/.local/share/`` on Linux. Use the ``db_path``
    argument to use a different location.

**Main functions:**

.. autosummary::
    :nosignatures:

    load_dwca_tables
    load_dwca_observations
    load_dwca_taxa
"""
# TODO: Lookup and replace user_login with user_id
# TODO: Translate DwC lifeStage and sex to iNat annotations
import sqlite3
import subprocess
from logging import getLogger
from os.path import basename, splitext
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Set, Tuple

from .constants import (
    DATA_DIR,
    DB_PATH,
    DWCA_OBS_CSV,
    DWCA_TAXA_URL,
    DWCA_TAXON_CSV,
    DWCA_URL,
    ICONIC_TAXA,
    TAXON_COUNTS,
    PathOrStr,
)
from .db import create_tables
from .download import (
    CSVProgress,
    check_download,
    download_file,
    get_progress,
    get_progress_spinner,
    unzip_progress,
)
from .dwc import get_dwc_lookup
from .sqlite import load_table, vacuum_analyze

if TYPE_CHECKING:
    from pandas import DataFrame


OBS_COLUMNS = [
    'catalogNumber',
    'captive',
    'coordinateUncertaintyInMeters',
    'decimalLatitude',
    'decimalLongitude',
    'eventDate',
    'inaturalistLogin',
    'informationWithheld',
    'modified',
    'occurrenceRemarks',
    'taxonID',
]
TAXON_COLUMN_MAP = {
    'id': 'id',
    'parentNameUsageID': 'parent_id',
    'scientificName': 'name',
    'taxonRank': 'rank',
}
TAXON_TABLE = 'taxon'
OBS_TABLE = 'observation'

logger = getLogger(__name__)


def load_dwca_tables(db_path: PathOrStr = DB_PATH):
    """Download observation and taxonomy archives and load into a SQLite database.

    As of 2022-05, this will require about 42GB of free disk space while loading, and the final
    database will be around 8GB.

    Args:
        db_path: Path to SQLite database
    """
    download_dwca_observations()
    download_dwca_taxa()
    with CSVProgress(DWCA_OBS_CSV, DWCA_TAXON_CSV) as progress:
        load_dwca_observations(db_path=db_path, progress=progress)
        load_dwca_taxa(db_path=db_path, progress=progress)
    vacuum_analyze(['observation', 'taxon'], db_path)


def download_dwca_observations(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the DwC-A research-grade observations dataset. Reuses local data if it
    already exists and is up to date.

    Example to load into a SQLite database (using the `sqlite3` shell, from bash):

    .. code-block:: bash

        export DATA_DIR="$HOME/.local/share/pyinaturalist"
        sqlite3 -csv $DATA_DIR/observations.db ".import $DATA_DIR/gbif-observations-dwca/observations.csv observations"

    Args:
        dest_dir: Alternative download directory
    """
    _download_archive(DWCA_URL, dest_dir)


def download_dwca_taxa(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the DwC-A taxonomy dataset. Reuses local data if it already exists and
    is up to date.

    Args:
        dest_dir: Alternative download directory
    """
    _download_archive(DWCA_TAXA_URL, dest_dir)


def load_dwca_observations(
    csv_path: PathOrStr = DWCA_OBS_CSV,
    db_path: PathOrStr = DB_PATH,
    progress: CSVProgress = None,
):
    """Create or update an observations SQLite table from the GBIF DwC-A archive. This keeps only the most
    relevant subset of columns available in the archive, in a format consistent with API results and
    other sources.

    To load everything as-is, see :py:func:`.load_full_dwca_observations`.
    """
    create_tables(db_path)
    column_map = _get_obs_column_map(OBS_COLUMNS)
    progress = progress or CSVProgress(csv_path)
    with progress:
        load_table(csv_path, db_path, 'observation', column_map, progress=progress)
    _cleanup_observations(db_path)


def load_full_dwca_observations(
    csv_path: PathOrStr = DWCA_OBS_CSV,
    db_path: PathOrStr = DB_PATH,
):
    """Create an observations SQLite table from the GBIF DwC-A archive, using all columns exactly
    as they appear in the archive.

    This requires the ``sqlite3`` executable to be installed on the system, since its ``.import``
    command is by far the fastest way to load this.
    """
    logger.info(f'Loading {csv_path} into {db_path}')
    subprocess.run(f'sqlite3 -csv {db_path} ".import {csv_path} observation"', shell=True)


def load_dwca_taxa(
    csv_path: PathOrStr = DWCA_TAXON_CSV,
    db_path: PathOrStr = DB_PATH,
    column_map: Dict = TAXON_COLUMN_MAP,
    progress: CSVProgress = None,
):
    """Create or update a taxonomy SQLite table from the GBIF DwC-A archive"""
    create_tables(db_path)

    def get_parent_id(row: Dict):
        """Get parent taxon ID from URL"""
        try:
            row['parentNameUsageID'] = int(row['parentNameUsageID'].split('/')[-1])
        except (KeyError, TypeError, ValueError):
            row['parentNameUsageID'] = None
        return row

    progress = progress or CSVProgress(csv_path)
    with progress:
        load_table(
            csv_path, db_path, 'taxon', column_map, transform=get_parent_id, progress=progress
        )
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE taxon SET parent_id=NULL WHERE parent_id=''")


def get_observation_taxon_counts(db_path: PathOrStr = DB_PATH) -> Dict[int, int]:
    """Get taxon counts based on GBIF export (exact rank counts only, no aggregage counts)"""
    if not Path(db_path).is_file():
        logger.warning(f'Observation database {db_path} not found')
        return {}

    logger.info(f'Getting base taxon counts from {db_path}')
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            'SELECT taxon_id, COUNT(*) AS count FROM observation '
            'WHERE taxon_id IS NOT NULL GROUP BY taxon_id;'
        ).fetchall()

        return {
            int(row['taxon_id']): int(row['count'])
            for row in sorted(rows, key=lambda r: r['count'], reverse=True)
        }


def aggregate_taxon_counts(
    db_path: PathOrStr = DB_PATH, counts_path: PathOrStr = TAXON_COUNTS
) -> 'DataFrame':
    """Aggregate taxon observation counts up to all ancestors, and save results back to taxonomy
    database.

    What we have to work with from the GBIF dataset are IDs, parent IDs, and a subset of ancestor
    names (not full ancestry). This starts at the bottom of the tree (with leaf taxa), and works up
    to the root. Due to uneven tree depths, at each level it's necessary to check which taxa at that
    level have had all their children counted.

    This would likely be better as a recursive function starting from the root, but dataframes don't
    lend themselves well to recursion. This is good enough for now, but could potentially be much
    faster.

    Args:
        db_path: Path to SQLite database
        taxon_counts_path: Save a copy of taxon counts in a separate file (Parquet format)
    """
    from pandas import DataFrame

    df = _get_taxon_df(db_path)

    # Get taxon counts from observations table
    taxon_counts_dict = get_observation_taxon_counts(db_path)
    taxon_counts = DataFrame(taxon_counts_dict.items(), columns=['id', 'count'])
    taxon_counts = taxon_counts.set_index('id')
    df = _join_counts(df, taxon_counts)
    df = df.rename_axis('id').reset_index()

    def add_child_counts(row):
        """Add child counts to the given taxon, if all children have been counted"""
        children = df[df['parent_id'] == row['id']]
        if children['id'].isin(processed_ids).all():
            row['count'] += children['count'].sum()
            progress.advance(task, 1)
        else:
            skipped_ids.add(row['id'])
        return row

    level = 1
    level_ids = set(_get_leaf_taxa_parents(db_path))
    processed_ids = set(_get_leaf_taxa(db_path))
    skipped_ids: Set[int]

    progress = get_progress()
    task = progress.add_task('[cyan]Processing...', total=len(df) - len(processed_ids))
    with progress:
        while len(level_ids) > 0:
            logger.info(f'Aggregating taxon counts at level {level} ({len(level_ids)} taxa)')
            skipped_ids = set()
            mask = df['id'].isin(level_ids)
            df.loc[mask] = df.loc[mask].apply(add_child_counts, axis=1)
            level_ids, processed_ids = _get_next_level(df, level_ids, processed_ids, skipped_ids)
            level += 1

    # Save a copy of minimal {id: count} mapping
    if counts_path:
        counts_path = Path(counts_path)
        counts_path.parent.mkdir(parents=True, exist_ok=True)
        min_df = df.set_index('id')
        min_df = min_df[min_df['count'] > 0][['count']]
        min_df = min_df.sort_values('count', ascending=False)
        min_df.to_parquet(counts_path)

    # Merge results into SQLite db
    _save_taxon_df(df, db_path)
    return df


def update_taxon_counts(
    db_path: PathOrStr = DB_PATH, counts_path: PathOrStr = TAXON_COUNTS
) -> 'DataFrame':
    """Load previously saved taxon counts (from :py:func:`.aggregate_taxon_counts` into the local
    taxon database
    """
    import pandas as pd

    taxon_counts = pd.read_parquet(counts_path)
    df = _get_taxon_df(db_path)
    df = _join_counts(df, taxon_counts)
    _save_taxon_df(df, db_path)
    return df


def _download_archive(url: str, dest_dir: PathOrStr = DATA_DIR):
    dest_dir = Path(dest_dir).expanduser()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / basename(url)

    # Skip download if we're already up to date
    if check_download(dest_file, url=url, release_interval=7):
        return

    # Otherwise, download and extract files
    download_file(url, dest_file)
    unzip_progress(dest_file, dest_dir / splitext(basename(url))[0])


def _cleanup_observations(db_path: PathOrStr = DB_PATH):
    """Run the following post-processing steps after loading observations:
    * Translate dwc:informationWithheld into standard geoprivacy values
    * Translate captive values into True/False
    * Vacuum/analyze
    """
    spinner = get_progress_spinner('Post-processing')
    with spinner, sqlite3.connect(db_path) as conn:
        logger.info('Finding observations with open geoprivacy')
        conn.execute("UPDATE observation SET geoprivacy='open' " "WHERE geoprivacy IS NULL")

        logger.info('Finding observations with obscured geoprivacy')
        conn.execute(
            "UPDATE observation SET geoprivacy='obscured' "
            "WHERE geoprivacy LIKE 'Coordinate uncertainty increased%'"
        )

        logger.info('Finding observations with private geoprivacy')
        conn.execute(
            "UPDATE observation SET geoprivacy='private' "
            "WHERE geoprivacy LIKE 'Coordinates hidden%'"
        )

        logger.info('Formatting captive/wild status')
        conn.execute("UPDATE observation SET captive=FALSE WHERE captive='wild'")
        conn.execute("UPDATE observation SET captive=TRUE WHERE captive IS NOT FALSE")


def _get_taxon_df(db_path: PathOrStr = DB_PATH) -> 'DataFrame':
    """Load taxon table into a dataframe"""
    import pandas as pd

    logger.info(f'Loading taxa from {db_path}')
    df = pd.read_sql_query('SELECT * FROM taxon', sqlite3.connect(db_path))
    df['parent_id'] = df['parent_id'].astype(pd.Int64Dtype())
    return df


def _save_taxon_df(df: 'DataFrame', db_path: PathOrStr = DB_PATH):
    """Save taxon dataframe back to SQLite; clear and reuse existing table to keep indexes"""
    logger.info('Saving taxon counts to database')
    with sqlite3.connect(db_path) as conn:
        conn.execute('DELETE FROM taxon')
        df.to_sql('taxon', conn, if_exists='append', index=False)
        conn.execute('VACUUM')


def _join_counts(df: 'DataFrame', taxon_counts: 'DataFrame') -> 'DataFrame':
    """Join taxon dataframe with updated taxon counts"""
    from numpy import int64

    df = df.drop('count', axis=1)
    df = df.join(taxon_counts)
    df['count'] = df['count'].fillna(0).astype(int64)
    return df


def _get_leaf_taxa(db_path: PathOrStr = DB_PATH) -> List[int]:
    """Get leaf taxa (species, subspecies, and any other taxa with no descendants)"""
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            'SELECT DISTINCT t1.id FROM taxon t1 '
            'LEFT JOIN taxon t2 ON t2.parent_id = t1.id '
            'WHERE t2.id IS NULL'
        )
        return [row[0] for row in rows]


def _get_leaf_taxa_parents(db_path: PathOrStr = DB_PATH) -> List[int]:
    """Get taxa with only one level of descendants"""
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            'SELECT DISTINCT t1.parent_id FROM taxon t1 '
            'LEFT JOIN taxon t2 ON t2.parent_id = t1.id '
            'WHERE t2.id IS NULL'
        )
        return [row[0] for row in rows]


def _get_next_level(df, level_ids, processed_ids, skipped_ids) -> Tuple[set, set]:
    """Get unique parent taxa of the current level, minus any previously processed taxa"""
    with_parents = df[df['id'].isin(level_ids) & ~df['parent_id'].isnull()]
    next_level_ids = set(with_parents['parent_id'].unique())
    processed_ids = processed_ids | (level_ids - skipped_ids)
    level_ids = next_level_ids - processed_ids
    return level_ids, processed_ids


def _get_obs_column_map(fields: List[str]) -> Dict[str, str]:
    """Translate subset of DwC terms to API-compatible field names"""
    lookup = {k.split(':')[-1]: v.replace('.', '_') for k, v in get_dwc_lookup().items()}
    return {field: lookup[field] for field in fields}
