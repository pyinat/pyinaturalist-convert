"""Utilities for working with the iNat GBIF DwC archive"""
# TODO: ORM models for these
# TODO: Faster way to load observation table without subprocess
import sqlite3
import subprocess
from logging import getLogger
from os.path import basename, splitext
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Set, Tuple

from pyinaturalist import enable_logging

from .constants import (
    DATA_DIR,
    DWCA_TAXA_URL,
    DWCA_URL,
    OBS_CSV,
    OBS_DB,
    TAXON_COUNTS,
    TAXON_CSV,
    TAXON_DB,
    PathOrStr,
)
from .download import check_download, download_file, get_progress, unzip_progress
from .sqlite import load_table

TAXON_COLUMN_MAP = {
    'id': 'id',
    'parentNameUsageID': 'parent_id',
    'scientificName': 'name',
    'taxonRank': 'rank',
}
# Other available fields:
# 'kingdom',
# 'phylum',
# 'class',
# 'order',
# 'family',
# 'genus',
# 'specificEpithet'
# 'infraspecificEpithet'
# 'modified',
# 'references',

if TYPE_CHECKING:
    from pandas import DataFrame

# debug
enable_logging()
getLogger('pyinaturalist_convert').setLevel('DEBUG')

logger = getLogger(__name__)


def download_dwca(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the GBIF DwC-A export. Reuses local data if it already exists and is
    up to date.

    Example to load into a SQLite database (using the `sqlite3` shell, from bash):

    .. code-block:: bash

        export DATA_DIR="$HOME/.local/share/pyinaturalist"
        sqlite3 -csv $DATA_DIR/observations.db ".import $DATA_DIR/gbif-observations-dwca/observations.csv observations"

    Args:
        dest_dir: Alternative download directory
    """
    _download_archive(DWCA_URL, dest_dir)


def download_dwca_taxa(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the DwC-A taxonomy export. Reuses local data if it already exists and is
    up to date.

    Args:
        dest_dir: Alternative download directory
    """
    _download_archive(DWCA_TAXA_URL, dest_dir)


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


def load_dwca_tables():
    """Download observation and taxonomy archives and load into SQLite tables"""
    download_dwca()
    download_dwca_taxa()
    load_observation_table()
    load_taxon_table()
    aggregate_taxon_counts()


def load_observation_table(
    csv_path: PathOrStr = OBS_CSV,
    db_path: PathOrStr = OBS_DB,
    table_name: str = 'observations',
):
    """Create an observations SQLite table from the GBIF DwC-A archive.

    Currently this requires the ``sqlite3`` executable to be installed on the system, since its
    ``.import`` command is many times faster than doing the equivalent with the python ``sqlite3``
    module.
    """
    logger.info(f'Loading {csv_path} into {db_path}')
    subprocess.run(f'sqlite3 -csv {db_path} ".import {csv_path} {table_name}"', shell=True)

    # Super slow alternative:
    # column_map ={'id': 'id', 'taxonID': 'taxon_id'}  # etc.
    # with CSVProgress(csv_path) as progress:
    #     load_table(csv_path, db_path, table_name, column_map, progress=progress)


def load_taxon_table(
    csv_path: PathOrStr = TAXON_CSV,
    db_path: PathOrStr = TAXON_DB,
    table_name: str = 'taxa',
    column_map: Dict = TAXON_COLUMN_MAP,
):
    """Create a taxonomy SQLite table from the GBIF DwC-A archive"""

    def get_parent_id(row: Dict):
        """Get parent taxon ID from URL"""
        try:
            row['parentNameUsageID'] = int(row['parentNameUsageID'].split('/')[-1])
        except (TypeError, ValueError):
            row['parentNameUsageID'] = None
        return row

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS {table_name} ('
            'id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT, rank TEXT, count INTEGER DEFAULT 0, '
            f'FOREIGN KEY (parent_id) REFERENCES {table_name}(id))'
        )

    load_table(csv_path, db_path, table_name, column_map, transform=get_parent_id)

    with sqlite3.connect(db_path) as conn:
        conn.execute(f"UPDATE {table_name} SET parent_id=NULL WHERE parent_id=''")
        conn.execute(f"CREATE INDEX IF NOT EXISTS taxon_name_idx ON {table_name}(name)")


def get_observation_taxon_counts(db_path: PathOrStr = OBS_DB) -> Dict[int, int]:
    """Get taxon counts based on GBIF export (exact rank counts only, no aggregage counts)"""
    if not Path(db_path).is_file():
        logger.warning(f'Observation database {db_path} not found')
        return {}

    logger.info(f'Getting taxon counts from {db_path}')
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE INDEX IF NOT EXISTS taxon_id_idx ON observations(taxonID)")
        conn.execute("DELETE FROM observations WHERE taxonID IS NULL or taxonID = ''")

        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT taxonID, COUNT(*) AS count FROM observations GROUP BY taxonID;"
        ).fetchall()

        return {
            int(row['taxonID']): int(row['count'])
            for row in sorted(rows, key=lambda r: r['count'], reverse=True)
        }


def aggregate_taxon_counts(db_path: PathOrStr = TAXON_DB, obs_db_path: PathOrStr = OBS_DB):
    """Aggregate taxon observation counts up to all ancestors, and save results back to taxonomy
    database.

    What we have to work with from the GBIF dataset are IDs, parent IDs, and a subset of ancestor
    names (not full ancestry). This starts at the bottom of the tree (with leaf taxa), and works up
    to the root. Due to uneven tree depths, at each level it's necessary to check which taxa at that
    level have had all their children counted.

    This would likely be better as a recursive function starting from the root, but dataframes don't
    lend themselves well to recursion. This is good enough for now, but could potentially be much
    faster.
    """
    import pandas as pd

    df = _get_taxon_df(db_path)

    # Get taxon counts from observations table
    taxon_counts_dict = get_observation_taxon_counts(obs_db_path)
    taxon_counts = pd.DataFrame(taxon_counts_dict.items(), columns=['id', 'count'])
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
    level_ids = set(_get_leaf_taxa_parents())
    processed_ids = set(_get_leaf_taxa())
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
    df = df.set_index('id')
    min_df = df[df['count'] > 0][['id', 'count']]
    min_df = min_df.sort_values('count', ascending=False)
    min_df.to_parquet(TAXON_COUNTS)

    # Save back to SQLite; clear and reuse existing table to keep indexes
    _save_taxa_df(df, db_path)
    return df


def update_taxon_counts(db_path: PathOrStr = TAXON_DB, counts_path: PathOrStr = TAXON_COUNTS):
    """Load previously saved taxon counts (from :py:func:`.aggregate_taxon_counts` into the local
    taxon database
    """
    import pandas as pd

    taxon_counts = pd.read_parquet(counts_path)
    df = _get_taxon_df(db_path)
    df = _join_counts(df, taxon_counts)
    _save_taxa_df(df, db_path)


def _get_taxon_df(db_path: PathOrStr = TAXON_DB) -> 'DataFrame':
    """Load taxon table into a dataframe"""
    import pandas as pd

    logger.info(f'Loading taxa from {db_path}')
    df = pd.read_sql_query('SELECT * FROM taxa', sqlite3.connect(db_path), index_col='id')
    df['parent_id'] = df['parent_id'].astype(pd.Int64Dtype())
    return df


def _save_taxa_df(df: 'DataFrame', db_path: PathOrStr = TAXON_DB):
    """Save taxon dataframe back to SQLite; clear and reuse existing table to keep indexes"""
    with sqlite3.connect(db_path) as conn:
        conn.execute('DELETE FROM taxa')
        df.to_sql('taxa', conn, if_exists='append')
        conn.execute('VACUUM')


def _join_counts(df: 'DataFrame', taxon_counts: 'DataFrame') -> 'DataFrame':
    """Join taxon dataframe with updated taxon counts"""
    import numpy as np

    df = df.drop('count', axis=1)
    df = df.join(taxon_counts)
    df['count'] = df['count'].fillna(0).astype(np.int64)
    return df


def _get_leaf_taxa(db_path: PathOrStr = TAXON_DB) -> List[int]:
    """Get leaf taxa (species, subspecies, and any other taxa with no descendants)"""
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            'SELECT DISTINCT t1.id FROM taxa t1 '
            'LEFT JOIN taxa t2 ON t2.parent_id = t1.id '
            'WHERE t2.id IS NULL'
        )
        return [row[0] for row in rows]


def _get_leaf_taxa_parents(db_path: PathOrStr = TAXON_DB) -> List[int]:
    """Get taxa with only one level of descendants"""
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            'SELECT DISTINCT t1.parent_id FROM taxa t1 '
            'LEFT JOIN taxa t2 ON t2.parent_id = t1.id '
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
