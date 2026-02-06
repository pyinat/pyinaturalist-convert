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
from concurrent.futures import ProcessPoolExecutor
from logging import getLogger
from multiprocessing import Manager, Process
from multiprocessing import Queue as MPQueue
from pathlib import Path
from time import sleep, time
from typing import TYPE_CHECKING, Optional

from pyinaturalist import ICONIC_TAXA, RANK_LEVELS

from .constants import DB_PATH, DWCA_TAXON_CSV_DIR, TAXON_AGGREGATES_PATH, PathOrStr
from .download import ParallelMultiProgress

if TYPE_CHECKING:
    from pandas import DataFrame

DEFAULT_LANG_CSV = DWCA_TAXON_CSV_DIR / 'VernacularNames-english.csv'
# Bacteria, viruses, etc.
EXCLUDE_IDS = [67333, 131236, 151817, 1228707, 1285874]
# All columns computed by aggregate_taxon_db
PRECOMPUTED_COLUMNS = [
    'ancestor_ids',
    'child_ids',
    'iconic_taxon_id',
    'observations_count_rg',
    'leaf_taxa_count',
    'preferred_common_name',
]
PARALLEL_THRESHOLD = 6000  # Partition size over which parallelization should be used
CHUNK_SIZE = 2000  # Chunk size per parallel worker

logger = getLogger(__name__)


def aggregate_taxon_db(
    db_path: PathOrStr = DB_PATH,
    backup_path: PathOrStr = TAXON_AGGREGATES_PATH,
    common_names_path: PathOrStr = DEFAULT_LANG_CSV,
    max_workers: Optional[int] = None,
    progress_bars: bool = True,
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
        max_workers: Max worker processes for parallel aggregation (None = cpu_count)
        progress_bars: Show detailed progress bars in addition to log output
    """
    start = time()
    progress = RichProgress() if progress_bars else LoggerProgress()

    # get total number of taxa for progress bar
    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA journal_mode = WAL')
        total_taxa = conn.execute('SELECT COUNT(*) FROM taxon;').fetchone()[0]
    progress.start(total=total_taxa)

    try:
        df = _aggregate_taxon_db(
            db_path,
            backup_path,
            common_names_path,
            progress,
            max_workers=max_workers,
        )
        progress.log(f'Completed taxonomy aggregation in {time() - start:.2f}s')
    except Exception as e:
        logger.exception(e)
    finally:
        progress.stop()
    return df


def _aggregate_taxon_db(
    db_path: PathOrStr,
    backup_path: PathOrStr,
    common_names_path: PathOrStr,
    progress: 'LoggerProgress',
    max_workers: Optional[int] = None,
) -> 'DataFrame':
    # Compute depth and ancestors
    progress.log('Computing ancestry...')
    df = _compute_ancestry(db_path)

    # Get observation counts from observations table
    progress.log('Loading observation taxon counts...')
    taxon_counts_dict = get_observation_taxon_counts(db_path)
    df['observations_count_rg'] = df['id'].map(taxon_counts_dict).fillna(0).astype('int64')

    # Aggregate bottom-up by level
    progress.log('Building children index...')
    children_index = _build_children_index(df)
    progress.log('Starting aggregation...')
    df = _aggregate_by_level(
        df,
        children_index,
        progress=progress,
        max_workers=max_workers,
    )

    # Load common names
    progress.log('Loading common names...')
    common_names = _get_common_names(common_names_path)
    df['preferred_common_name'] = df['id'].map(common_names)

    progress.log('Saving results...')
    df = df.drop(columns=['depth'])
    _save_taxon_agg(df, backup_path)
    _save_taxon_df(df, db_path)
    return df


def get_observation_taxon_counts(db_path: PathOrStr = DB_PATH) -> dict[int, int]:
    """Get taxon counts based on GBIF export (exact rank counts only, no aggregate counts)"""
    if not Path(db_path).is_file():
        logger.warning(f'Observation database {db_path} not found')
        return {}

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode = WAL')
        rows = conn.execute(
            'SELECT taxon_id, COUNT(*) AS count FROM observation '
            'WHERE taxon_id IS NOT NULL '
            'GROUP BY taxon_id;'
        ).fetchall()
        results = {
            int(row['taxon_id']): int(row['count'])
            for row in sorted(rows, key=lambda r: r['count'], reverse=True)
        }

    return results


def _get_common_names(common_names_path: PathOrStr = DEFAULT_LANG_CSV) -> dict[int, str]:
    """Get common names for the specified language from DwC-A taxonomy files."""
    import pandas as pd

    csv_path = Path(common_names_path).expanduser()
    if not csv_path.is_file():
        logger.warning(f'File not found: {csv_path}; common names will not be loaded')
        return {}

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


def _compute_ancestry(db_path: PathOrStr = DB_PATH) -> 'DataFrame':
    """Recursively compute ancestors and depth in SQL, and load into a dataframe"""
    import pandas as pd

    # ancestor_ids is built as comma-separated string from root to parent (not including self)
    query = """
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
        df = pd.read_sql_query(query, conn)

    # Convert types
    df['parent_id'] = df['parent_id'].astype(pd.Int64Dtype())
    df['depth'] = df['depth'].astype('int64')
    df.loc[df['ancestor_ids'] == '', 'ancestor_ids'] = None
    return df


def _build_children_index(df: 'DataFrame') -> dict[int, list[int]]:
    """Build a mapping from parent_id to list of child_ids.

    Single O(n) pass using groupby for O(1) child lookups later.
    """
    children_df = df[df['parent_id'].notna()][['id', 'parent_id']]
    children_index: dict[int, list[int]] = (
        children_df.groupby('parent_id')['id'].apply(list).to_dict()
    )
    return children_index


def _aggregate_by_level(
    df: 'DataFrame',
    children_index: dict[int, list[int]],
    progress: 'LoggerProgress',
    max_workers: Optional[int] = None,
) -> 'DataFrame':
    """Aggregate values from the bottom up, starting with leaf nodes"""
    df['leaf_taxa_count'] = 0
    df['child_ids'] = None
    df['iconic_taxon_id'] = None
    df = df.set_index('id')
    max_depth = df['depth'].max()
    max_depth + 1

    # Pre-compute iconic taxa as a set for faster lookups in workers
    iconic_taxa_set = set(ICONIC_TAXA.keys())

    # Process from leaves to root
    progress.start_task('taxa', total=len(df), description='Aggregating')
    for depth in range(max_depth, -1, -1):
        level_ids = df.index[df['depth'] == depth].tolist()
        level_ranks = _format_rank_range(df.loc[level_ids, 'rank'].unique().tolist())
        progress.log(f'Aggregating level {depth} ({level_ranks})...')

        # Use parallel processing for large levels
        if len(level_ids) < PARALLEL_THRESHOLD:
            df = _aggregate_level(
                df,
                level_ids,
                children_index,
                progress=progress,
            )
        else:
            df = _aggregate_level_parallel(
                df,
                level_ids,
                children_index,
                iconic_taxa_set,
                progress=progress,
                max_workers=max_workers,
            )
    return df.reset_index()


def _aggregate_level(
    df: 'DataFrame',
    level_ids: list[int],
    children_index: dict[int, list[int]],
    progress: 'LoggerProgress',
) -> 'DataFrame':
    """Process a single level sequentially."""

    progress.log(f'  Processing {len(level_ids)} taxa')
    for i, taxon_id in enumerate(level_ids):
        child_ids = children_index.get(taxon_id, [])
        if child_ids:
            df.at[taxon_id, 'child_ids'] = ','.join(map(str, child_ids))
            child_obs_sum = df.loc[child_ids, 'observations_count_rg'].sum()
            child_leaf_sum = df.loc[child_ids, 'leaf_taxa_count'].sum()
            df.at[taxon_id, 'observations_count_rg'] += child_obs_sum
            df.at[taxon_id, 'leaf_taxa_count'] = child_leaf_sum
        else:
            df.at[taxon_id, 'leaf_taxa_count'] = 1

        ancestor_str = df.at[taxon_id, 'ancestor_ids']
        df.at[taxon_id, 'iconic_taxon_id'] = _get_iconic_taxon_id(taxon_id, ancestor_str)

        if (i + 1) % 100 == 0:
            progress.advance('taxa', 100)

    # Report any remaining progress
    remaining = len(level_ids) % 100
    if remaining > 0:
        progress.advance('taxa', remaining)

    return df


def _aggregate_level_parallel(
    df: 'DataFrame',
    level_ids: list[int],
    children_index: dict[int, list[int]],
    iconic_taxa_set: set,
    progress: 'LoggerProgress',
    max_workers: Optional[int] = None,
) -> 'DataFrame':
    """Process a single level using parallel workers"""
    # Prepare data for each taxon - precompute child sums to minimize dataframe access in workers
    chunk_data = []
    for taxon_id in level_ids:
        child_ids = children_index.get(taxon_id, [])
        own_obs = df.at[taxon_id, 'observations_count_rg']
        ancestor_str = df.at[taxon_id, 'ancestor_ids']
        child_obs_sum = df.loc[child_ids, 'observations_count_rg'].sum() if child_ids else 0
        child_leaf_sum = df.loc[child_ids, 'leaf_taxa_count'].sum() if child_ids else 0

        chunk_data.append(
            (taxon_id, child_ids, own_obs, child_obs_sum, child_leaf_sum, ancestor_str)
        )

    # Split into chunks
    chunks = [chunk_data[i : i + CHUNK_SIZE] for i in range(0, len(chunk_data), CHUNK_SIZE)]
    progress.log(f'  Processing {len(level_ids)} taxa in {len(chunks)} chunks')

    # Process chunks
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_process_taxa_chunk, chunk, iconic_taxa_set, progress.progress_queue)
            for chunk in chunks
        ]
        all_results = [f.result() for f in futures]

    # Merge results
    progress.log('  Merging chunks')
    for chunk_results in all_results:
        for taxon_id, total_obs, leaf_count, child_ids_str, iconic_taxon_id in chunk_results:
            df.at[taxon_id, 'observations_count_rg'] = total_obs
            df.at[taxon_id, 'leaf_taxa_count'] = leaf_count
            df.at[taxon_id, 'child_ids'] = child_ids_str
            df.at[taxon_id, 'iconic_taxon_id'] = iconic_taxon_id

    return df


def _process_taxa_chunk(chunk_data, iconic_taxa_set, progress_queue) -> list:
    """Worker function to process a chunk of taxa in parallel.

    Returns:
        List of (taxon_id, total_obs, leaf_count, child_ids_str, iconic_taxon_id)
    """
    results = []
    total = len(chunk_data)
    report_interval = max(1, total // 10)  # Report ~10 times per chunk

    for i, (taxon_id, child_ids, own_obs, child_obs_sum, child_leaf_sum, ancestor_str) in enumerate(
        chunk_data
    ):
        child_ids_str = ','.join(map(str, child_ids)) if child_ids else None
        total_obs = own_obs + child_obs_sum if child_ids else own_obs
        leaf_count = child_leaf_sum if child_ids else 1

        # Compute iconic_taxon_id
        iconic_taxon_id = None
        if isinstance(ancestor_str, str):
            ancestor_ids = [int(x) for x in ancestor_str.split(',')] + [taxon_id]
            for ancestor_id in reversed(ancestor_ids):
                if ancestor_id in iconic_taxa_set:
                    iconic_taxon_id = ancestor_id
                    break

        results.append((taxon_id, total_obs, leaf_count, child_ids_str, iconic_taxon_id))

        # Report progress periodically
        if (i + 1) % report_interval == 0:
            progress_queue.put(('taxa', report_interval))

    # Report any remaining progress
    if (remaining := total % report_interval) > 0:
        progress_queue.put(('taxa', remaining))

    return results


def _format_rank_range(ranks: list[str]) -> str:
    if len(ranks) == 1:
        return ranks[0]
    sorted_ranks = sorted(ranks, key=lambda r: RANK_LEVELS.get(r, 100))
    return f'{sorted_ranks[0]} through {sorted_ranks[-1]}'


def _get_iconic_taxon_id(taxon_id: int, ancestor_ids_str: Optional[str]) -> Optional[int]:
    """Get the most specific iconic taxon for a given taxon"""
    # Check ancestors + self in reverse order (most specific first)
    if isinstance(ancestor_ids_str, str):
        ancestor_ids = [int(x) for x in ancestor_ids_str.split(',')] + [taxon_id]
        for ancestor_id in reversed(ancestor_ids):
            if ancestor_id in ICONIC_TAXA:
                return ancestor_id
    return None


class LoggerProgress:
    """Base class for progress display. Just logs messages to a logger, with placeholders for
    progress bars.
    """

    def __init__(self):
        manager = Manager()
        self.progress_queue = manager.Queue()
        self.task_queue = manager.Queue()
        self.log_queue = manager.Queue()
        self._progress_proc: Optional[Process] = None

    def start(self, total: int):
        pass

    def stop(self):
        pass

    def advance(self, name: str, amount: int = 1):
        pass

    def log(self, message: str):
        logger.info(message)

    def start_task(self, name: str, total: int, description: str = ''):
        logger.info(f'Starting task: {description or name} ({total} items)')


class RichProgress(LoggerProgress):
    """Container for multiprocessing queues used for progress reporting."""

    def start(self, total: int = 1):
        """Start the progress display process."""
        self._progress_proc = Process(
            target=_update_progress,
            args=(self.progress_queue, self.task_queue, self.log_queue, total),
        )
        self._progress_proc.start()

    def stop(self):
        """Stop the progress display process."""
        if self._progress_proc is not None:
            # Signal completion
            self.task_queue.put(None)
            self._progress_proc.join(timeout=5)
            if self._progress_proc.is_alive():
                self._progress_proc.terminate()
            self._progress_proc = None

    def log(self, message: str):
        """Send a log message to the progress display."""
        self.log_queue.put(message)

    def start_task(self, name: str, total: int, description: str = ''):
        """Register a new task with the progress display."""
        self.task_queue.put((name, description or name, total))

    def advance(self, name: str, amount: int = 1):
        """Advance progress for a task."""
        self.progress_queue.put((name, amount))


def _update_progress(
    progress_queue: MPQueue,
    task_queue: MPQueue,
    log_queue: MPQueue,
    total: int,
):
    """Pull from multiprocessing queues and update progress"""
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
                item = task_queue.get_nowait()
                if item is None:
                    # Stop signal received
                    return
                task_name, task_desc, task_total = item
                progress.start_job(task_name, task_total, task_desc)

            # Collect progress updates
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
