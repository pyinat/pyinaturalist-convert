"""
Helper utilities for navigating tabular taxonomy data as a tree and adding additional derived
information to it.

**Extra dependencies**:
    * ``pandas``
    * ``sqlalchemy``

**Example**::

    >>> from pyinaturalist_convert import load_dwca_tables, aggregate_taxon_db
    >>> load_dwca_tables()
    >>> aggregate_taxon_db()

**Main functions:**

.. autosummary::
    :nosignatures:

    aggregate_taxon_db
    get_observation_taxon_counts
"""

import sqlite3
from logging import getLogger
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Callable, Dict, List, Optional

from pyinaturalist.constants import ICONIC_TAXA

from .constants import DB_PATH, DWCA_TAXON_CSV_DIR, TAXON_AGGREGATES_PATH, PathOrStr

if TYPE_CHECKING:
    from pandas import DataFrame

DEFAULT_LANG_CSV = DWCA_TAXON_CSV_DIR / 'VernacularNames-english.csv'
# All columns computed by aggregate_taxon_db
PRECOMPUTED_COLUMNS = [
    'ancestor_ids',
    'child_ids',
    'iconic_taxon_id',
    'observations_count_rg',
    'leaf_taxa_count',
    'preferred_common_name',
]

logger = getLogger(__name__)


def aggregate_taxon_db(
    db_path: PathOrStr = DB_PATH,
    backup_path: PathOrStr = TAXON_AGGREGATES_PATH,
    common_names_path: PathOrStr = DEFAULT_LANG_CSV,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
) -> 'DataFrame':
    """Add aggregate and hierarchical values to the taxon database:

    * Ancestor IDs
    * Child IDs
    * Iconic taxon ID
    * Aggregated observation taxon counts
    * Aggregated leaf taxon counts
    * Common names

    Requires GBIF datasets to be downloaded and processed first.

    Args:
        db_path: Path to SQLite database
        backup_path: Path to save a minimal copy of aggregate values
        common_names_path: Path to a CSV file containing taxon common names.
            See the DwC-A taxonomy dataset for available languages.
        progress_callback: Optional callback(stage, current, total) for progress updates
    """
    start = time()
    logger.info('Starting taxonomy aggregation')

    # Step 1: Compute depth and ancestors
    logger.info('Computing depth and ancestor paths...')
    df = _compute_ancestors(db_path)
    if progress_callback:
        progress_callback('depth_ancestors', 1, 5)

    # Step 2: Get observation counts from observations table
    logger.info('Getting observation taxon counts...')
    taxon_counts_dict = get_observation_taxon_counts(db_path)
    df['observations_count_rg'] = df['id'].map(taxon_counts_dict).fillna(0).astype('int64')
    if progress_callback:
        progress_callback('obs_counts', 2, 5)

    # Step 3: Build children index for O(1) lookups
    logger.info('Building children index...')
    children_index = _build_children_index(df)
    if progress_callback:
        progress_callback('children_index', 3, 5)

    # Step 4: Aggregate bottom-up by level
    logger.info('Aggregating by level...')
    df = _aggregate_by_level(df, children_index)
    if progress_callback:
        progress_callback('aggregation', 4, 5)

    # Step 5: Load common names
    logger.info('Loading common names...')
    common_names = _get_common_names(common_names_path)
    df['preferred_common_name'] = df['id'].map(common_names)
    if progress_callback:
        progress_callback('common_names', 5, 5)

    df = df.drop(columns=['depth'])
    _save_taxon_agg(df, backup_path)
    _save_taxon_df(df, db_path)

    logger.info(f'Completed taxonomy aggregation in {time() - start:.2f}s')
    return df


def get_observation_taxon_counts(db_path: PathOrStr = DB_PATH) -> Dict[int, int]:
    """Get taxon counts based on GBIF export (exact rank counts only, no aggregate counts)"""
    if not Path(db_path).is_file():
        logger.warning(f'Observation database {db_path} not found')
        return {}

    logger.info(f'Getting base taxon counts from {db_path}')
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode = WAL')
        rows = conn.execute(
            'SELECT taxon_id, COUNT(*) AS count FROM observation '
            'WHERE taxon_id IS NOT NULL '
            'GROUP BY taxon_id;'
        ).fetchall()

        return {
            int(row['taxon_id']): int(row['count'])
            for row in sorted(rows, key=lambda r: r['count'], reverse=True)
        }


def _get_common_names(common_names_path: PathOrStr = DEFAULT_LANG_CSV) -> Dict[int, str]:
    """Get common names for the specified language from DwC-A taxonomy files."""
    import pandas as pd

    csv_path = Path(common_names_path).expanduser()
    if not csv_path.is_file():
        logger.warning(f'File not found: {csv_path}; common names will not be loaded')
        return {}

    logger.info(f'Loading common names from {common_names_path}')
    df = pd.read_csv(csv_path)

    # Get the first match for each taxon ID; appears to be already sorted by relevance
    df = df.drop_duplicates(subset='id', keep='first')
    df = df.set_index('id')
    return df['vernacularName'].to_dict()


def _get_taxon_df(db_path: PathOrStr = DB_PATH) -> 'DataFrame':
    """Load taxon table into a dataframe"""
    import pandas as pd

    logger.info(f'Loading taxa from {db_path}')
    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA journal_mode = WAL')
        df = pd.read_sql_query('SELECT * FROM taxon', conn)
    df['parent_id'] = df['parent_id'].astype(pd.Int64Dtype())
    return df


def _save_taxon_df(df: 'DataFrame', db_path: PathOrStr = DB_PATH):
    """Save taxon dataframe back to SQLite; clear and reuse existing table to keep indexes"""
    from pyinaturalist_convert.db import create_tables

    # Backup to CSV in the rare case that this fails
    db_path = Path(db_path)
    backup_path = db_path.parent / 'taxa_backup.csv'
    df.to_csv(backup_path)

    logger.info('Saving taxon counts to database')
    create_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute('PRAGMA busy_timeout = 30000')
            conn.execute('PRAGMA journal_mode = WAL')
            conn.execute('DELETE FROM taxon')
            conn.commit()
            df.to_sql('taxon', conn, if_exists='append', index=False)
        except (IOError, sqlite3.DatabaseError) as e:
            logger.exception(e)
            logger.warning(f'Failed writing to database; backup available at {backup_path}')
        else:
            backup_path.unlink()


def update_taxon_agg(
    db_path: PathOrStr = DB_PATH, agg_path: PathOrStr = TAXON_AGGREGATES_PATH
) -> 'DataFrame':
    """Update an existing taxon database with new aggregate values"""
    import pandas as pd

    agg_values = pd.read_parquet(agg_path)
    df = _get_taxon_df(db_path)
    df = _join_taxon_agg(df, agg_values)
    _save_taxon_df(df, db_path)
    return df


def _join_taxon_agg(df: 'DataFrame', taxon_agg: 'DataFrame') -> 'DataFrame':
    """Join taxon dataframe with updated taxon aggregate values"""
    from numpy import int64

    # Drop columns to be updated
    for col in PRECOMPUTED_COLUMNS:
        if col in df and col in taxon_agg:
            df = df.drop(col, axis=1)

    # Join dataframes
    if 'id' in df:
        df = df.set_index('id')
    if 'id' in taxon_agg:
        taxon_agg = taxon_agg.set_index('id')
    df = df.join(taxon_agg)

    # Default count columns to 0
    for col in ['observations_count_rg', 'leaf_taxa_count']:
        if col in df:
            df[col] = df[col].fillna(0).astype(int64)

    return df.rename_axis('id').reset_index()


def _save_taxon_agg(df: 'DataFrame', agg_path: PathOrStr = TAXON_AGGREGATES_PATH):
    """Save a minimal copy of taxon aggregate values"""
    agg_path = Path(agg_path)
    agg_path.parent.mkdir(parents=True, exist_ok=True)
    df2 = df.set_index('id')
    df2 = df2[PRECOMPUTED_COLUMNS]
    df2 = df2.sort_values('observations_count_rg', ascending=False)
    df2.to_parquet(agg_path)


def _compute_ancestors(db_path: PathOrStr = DB_PATH) -> 'DataFrame':
    """Compute ancestors and depth"""
    import pandas as pd

    # ancestor_ids is built as comma-separated string from root to parent (not including self)
    cte_query = """
    WITH RECURSIVE taxon_tree AS (
        -- Base case: root taxon (parent_id IS NULL)
        SELECT
            id,
            parent_id,
            name,
            rank,
            0 as depth,
            '' as ancestor_ids
        FROM taxon
        WHERE parent_id IS NULL

        UNION ALL

        -- Recursive case: children
        SELECT
            t.id,
            t.parent_id,
            t.name,
            t.rank,
            tt.depth + 1,
            CASE
                WHEN tt.ancestor_ids = '' THEN CAST(tt.id AS TEXT)
                ELSE tt.ancestor_ids || ',' || CAST(tt.id AS TEXT)
            END
        FROM taxon t
        JOIN taxon_tree tt ON t.parent_id = tt.id
    )
    SELECT * FROM taxon_tree;
    """

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA journal_mode = WAL')
        df = pd.read_sql_query(cte_query, conn)

    # Convert types
    df['parent_id'] = df['parent_id'].astype(pd.Int64Dtype())
    df['depth'] = df['depth'].astype('int64')

    # Replace empty string with None for ancestor_ids
    df.loc[df['ancestor_ids'] == '', 'ancestor_ids'] = None

    logger.info(f'Computed depth for {len(df)} taxa, max depth: {df["depth"].max()}')
    return df


def _build_children_index(df: 'DataFrame') -> Dict[int, List[int]]:
    """Build a mapping from parent_id to list of child_ids.

    Single O(n) pass using groupby for O(1) child lookups later.
    """
    children_df = df[df['parent_id'].notna()][['id', 'parent_id']]
    children_index: Dict[int, List[int]] = (
        children_df.groupby('parent_id')['id'].apply(list).to_dict()
    )
    logger.info(f'Built children index with {len(children_index)} parent entries')
    return children_index


def _aggregate_by_level(
    df: 'DataFrame',
    children_index: Dict[int, List[int]],
) -> 'DataFrame':
    """Aggregate observation counts and leaf counts bottom-up by tree level.

    Processes from max_depth to 0, using vectorized operations per level.
    All nodes at the same depth are independent and can be processed in parallel.
    """

    max_depth = df['depth'].max()
    logger.info(f'Aggregating {len(df)} taxa across {max_depth + 1} levels')

    # Initialize columns
    df['leaf_taxa_count'] = 0
    df['child_ids'] = None
    df['iconic_taxon_id'] = None

    # Create id-to-index mapping for fast lookups
    df = df.set_index('id')

    # Process from leaves to root
    for depth in range(max_depth, -1, -1):
        level_mask = df['depth'] == depth
        level_ids = df.index[level_mask].tolist()

        if not level_ids:
            continue

        # For each taxon at this level
        for taxon_id in level_ids:
            child_ids = children_index.get(taxon_id, [])

            # Set child_ids string
            if child_ids:
                df.at[taxon_id, 'child_ids'] = ','.join(map(str, child_ids))

                # Aggregate from children
                child_obs_sum = df.loc[child_ids, 'observations_count_rg'].sum()
                child_leaf_sum = df.loc[child_ids, 'leaf_taxa_count'].sum()

                df.at[taxon_id, 'observations_count_rg'] += child_obs_sum
                df.at[taxon_id, 'leaf_taxa_count'] = child_leaf_sum
            else:
                # Leaf taxon
                df.at[taxon_id, 'leaf_taxa_count'] = 1

            # Compute iconic_taxon_id
            ancestor_str = df.at[taxon_id, 'ancestor_ids']
            df.at[taxon_id, 'iconic_taxon_id'] = _compute_iconic_taxon_id(taxon_id, ancestor_str)

        if depth % 5 == 0:
            logger.debug(f'  Processed level {depth} ({len(level_ids)} taxa)')

    return df.reset_index()


def _compute_iconic_taxon_id(taxon_id: int, ancestor_ids_str: Optional[str]) -> Optional[int]:
    """Compute the most specific iconic taxon for a given taxon.

    Checks current taxon first, then ancestors in reverse order (deepest first).
    """
    # Check if current taxon is iconic
    if taxon_id in ICONIC_TAXA:
        return taxon_id

    # Check ancestors in reverse order (most specific first)
    # ancestor_ids_str is None for root, or a comma-separated string
    if isinstance(ancestor_ids_str, str):
        ancestor_ids = [int(x) for x in ancestor_ids_str.split(',')]
        for ancestor_id in reversed(ancestor_ids):
            if ancestor_id in ICONIC_TAXA:
                return ancestor_id

    return None


def _update_progress(
    progress_queue: Queue, task_queue: Queue, log_queue: Queue, total: int
):  # pragma: no cover
    """Pull from a multiprocessing queue and update progress"""
    progress = ParallelMultiProgress(total=total)
    pending: list[tuple[str, int]] = []
    refresh_rate = 10  # ticks per second

    with progress:
        while True:
            # Show any one-off log messages
            while not log_queue.empty():
                progress.log(log_queue.get())

            # Check for new tasks (max 1 per tick)
            if not task_queue.empty():
                task_name, task_desc, total = task_queue.get()
                progress.start_job(task_name, total, task_desc)

            # Check for new progress
            while not progress_queue.empty():
                pending.append(progress_queue.get())

            # Update progress bars
            completed = pending.copy()
            pending = []
            for task_name, n_completed in completed:
                if task_name in progress.job_names:
                    progress.advance(task_name, n_completed)
                # Received progress for a task that hasn't been added yet; check next iteration
                else:
                    pending.append((task_name, n_completed))
            sleep(1 / refresh_rate)
