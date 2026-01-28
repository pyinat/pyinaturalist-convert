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
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

from pyinaturalist.constants import ICONIC_TAXA

from .constants import DB_PATH, DWCA_TAXON_CSV_DIR, TAXON_AGGREGATES_PATH, PathOrStr
from .download import ParallelMultiProgress

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
PARALLEL_THRESHOLD = 10000  # Partition size over which parallelization should be used
CHUNK_SIZE = 2000  # Chunk size per parallel worker

logger = getLogger(__name__)

ProgressCallback = Callable[[str, int, int], None]


def aggregate_taxon_db_with_progress(
    db_path: PathOrStr = DB_PATH,
    backup_path: PathOrStr = TAXON_AGGREGATES_PATH,
    common_names_path: PathOrStr = DEFAULT_LANG_CSV,
    max_workers: Optional[int] = None,
) -> 'DataFrame':
    """Run taxonomy aggregation with rich progress display.

    This wrapper runs aggregation with visual progress bars showing:
    - Overall progress across levels
    - Per-level progress for parallel processing
    - Log messages from workers

    Uses multiprocessing queues so parallel workers can report their progress.

    Args:
        db_path: Path to SQLite database
        backup_path: Path to save a minimal copy of aggregate values
        common_names_path: Path to a CSV file containing taxon common names
        max_workers: Max worker processes for parallel aggregation

    Returns:
        DataFrame with aggregated taxonomy data
    """
    # Create progress queues
    queues = ProgressQueues()

    # Estimate total work (will be refined once we know the tree depth)
    queues.start(total=100)

    try:
        # Create a callback that uses the queues for main-process progress
        current_task: Dict[str, Optional[str]] = {'name': None}

        def progress_callback(step_name: str, current: int, total: int) -> None:
            """Update progress via queues."""
            if step_name != current_task['name']:
                current_task['name'] = step_name
                queues.start_task(step_name, total, step_name)
            if current > 0:
                queues.advance(step_name, 1)

        # Pass queues so parallel workers can report progress directly
        result = aggregate_taxon_db(
            db_path=db_path,
            backup_path=backup_path,
            common_names_path=common_names_path,
            progress_callback=progress_callback,
            max_workers=max_workers,
            progress_queue=queues.progress_queue,
            task_queue=queues.task_queue,
        )
    finally:
        queues.stop()

    return result


def aggregate_taxon_db(
    db_path: PathOrStr = DB_PATH,
    backup_path: PathOrStr = TAXON_AGGREGATES_PATH,
    common_names_path: PathOrStr = DEFAULT_LANG_CSV,
    progress_callback: Optional[ProgressCallback] = None,
    max_workers: Optional[int] = None,
    progress_queue: Optional[MPQueue] = None,
    task_queue: Optional[MPQueue] = None,
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
        progress_callback: Optional callback(step_name, current, total) for progress reporting.
            Called during aggregation with step name and progress values.
        max_workers: Max worker processes for parallel aggregation (None = cpu_count).
            Only used for levels with >10,000 taxa.
        progress_queue: Optional queue for progress updates from parallel workers.
        task_queue: Optional queue for registering tasks with progress display.
    """
    start = time()
    logger.info('Starting taxonomy aggregation')

    # Compute depth and ancestors
    logger.info('Computing depth and ancestor paths...')
    if progress_callback:
        progress_callback('Computing ancestors', 0, 1)
    df = _compute_ancestors(db_path)

    # Get observation counts from observations table
    logger.info('Getting observation taxon counts...')
    if progress_callback:
        progress_callback('Loading observation counts', 0, 1)
    taxon_counts_dict = get_observation_taxon_counts(db_path)
    df['observations_count_rg'] = df['id'].map(taxon_counts_dict).fillna(0).astype('int64')

    # Aggregate bottom-up by level
    logger.info('Building children index...')
    children_index = _build_children_index(df)
    logger.info('Aggregating by level...')
    df = _aggregate_by_level(
        df,
        children_index,
        progress_callback,
        max_workers,
        progress_queue=progress_queue,
        task_queue=task_queue,
    )

    # Load common names
    logger.info('Loading common names...')
    common_names = _get_common_names(common_names_path)
    df['preferred_common_name'] = df['id'].map(common_names)

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
        results = {
            int(row['taxon_id']): int(row['count'])
            for row in sorted(rows, key=lambda r: r['count'], reverse=True)
        }

    return results


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
    progress_callback: Optional[ProgressCallback] = None,
    max_workers: Optional[int] = None,
    progress_queue: Optional[MPQueue] = None,
    task_queue: Optional[MPQueue] = None,
) -> 'DataFrame':
    """Aggregate values from the bottom up, starting with leaf nodes.

    Uses parallel processing for levels with many taxa (>PARALLEL_THRESHOLD).

    Args:
        df: DataFrame with taxa, must have 'depth', 'observations_count_rg', 'ancestor_ids'
        children_index: Mapping from parent_id to list of child_ids
        progress_callback: Optional callback(step_name, current, total) for progress reporting
        max_workers: Max worker processes for parallel levels (None = cpu_count)
        progress_queue: Optional queue for progress updates from workers
        task_queue: Optional queue for registering new tasks
    """
    max_depth = df['depth'].max()
    total_levels = max_depth + 1
    logger.info(f'Aggregating {len(df)} taxa across {total_levels} levels')

    df['leaf_taxa_count'] = 0
    df['child_ids'] = None
    df['iconic_taxon_id'] = None
    df = df.set_index('id')

    # Pre-compute iconic taxa as a set for faster lookups in workers
    iconic_taxa_set = set(ICONIC_TAXA.keys())

    # Process from leaves to root
    for depth in range(max_depth, -1, -1):
        level_ids = df.index[df['depth'] == depth].tolist()
        level_size = len(level_ids)
        level_name = f'Level {depth}'

        if progress_callback:
            progress_callback('Aggregating levels', max_depth - depth + 1, total_levels)

        # Use parallel processing for large levels
        if level_size >= PARALLEL_THRESHOLD:
            df = _aggregate_level_parallel(
                df,
                level_ids,
                children_index,
                iconic_taxa_set,
                max_workers,
                progress_queue=progress_queue,
                task_queue=task_queue,
                level_name=level_name,
            )
        else:
            df = _aggregate_level_sequential(df, level_ids, children_index)

        if depth % 5 == 0:
            logger.debug(f'  Processed level {depth} ({level_size} taxa)')

    return df.reset_index()


def _aggregate_level_sequential(
    df: 'DataFrame',
    level_ids: List[int],
    children_index: Dict[int, List[int]],
) -> 'DataFrame':
    """Process a single level sequentially (for small levels)."""
    for taxon_id in level_ids:
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

    return df


def _aggregate_level_parallel(
    df: 'DataFrame',
    level_ids: List[int],
    children_index: Dict[int, List[int]],
    iconic_taxa_set: set,
    max_workers: Optional[int] = None,
    progress_queue: Optional[MPQueue] = None,
    task_queue: Optional[MPQueue] = None,
    level_name: str = 'level',
) -> 'DataFrame':
    """Process a single level using parallel workers (for large levels).

    Args:
        df: DataFrame with taxa indexed by id
        level_ids: List of taxon IDs at this level
        children_index: Mapping from parent_id to list of child_ids
        iconic_taxa_set: Set of iconic taxon IDs for lookup
        max_workers: Max worker processes (None = cpu_count)
        progress_queue: Optional queue for progress updates from workers
        task_queue: Optional queue for registering new tasks
        level_name: Name for progress reporting
    """
    # Prepare data for each taxon - pre-compute child sums to minimize DataFrame access in workers
    chunk_data = []
    for taxon_id in level_ids:
        child_ids = children_index.get(taxon_id, [])
        own_obs = df.at[taxon_id, 'observations_count_rg']
        ancestor_str = df.at[taxon_id, 'ancestor_ids']

        if child_ids:
            child_obs_sum = df.loc[child_ids, 'observations_count_rg'].sum()
            child_leaf_sum = df.loc[child_ids, 'leaf_taxa_count'].sum()
        else:
            child_obs_sum = 0
            child_leaf_sum = 0

        chunk_data.append(
            (taxon_id, child_ids, own_obs, child_obs_sum, child_leaf_sum, ancestor_str)
        )

    # Split into chunks
    chunks = [chunk_data[i : i + CHUNK_SIZE] for i in range(0, len(chunk_data), CHUNK_SIZE)]
    num_chunks = len(chunks)

    logger.debug(f'  Processing {len(level_ids)} taxa in {num_chunks} chunks with parallel workers')

    # Register task with progress system if available
    if task_queue is not None:
        task_queue.put((level_name, f'Aggregating {level_name}', len(level_ids)))

    # Process chunks in parallel
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # All chunks report progress against the same task (level_name)
        chunk_args = [(chunk, iconic_taxa_set, level_name, progress_queue) for chunk in chunks]
        all_results = list(executor.map(_process_taxa_chunk, chunk_args))

    # Merge results back into DataFrame
    for chunk_results in all_results:
        for taxon_id, total_obs, leaf_count, child_ids_str, iconic_taxon_id in chunk_results:
            df.at[taxon_id, 'observations_count_rg'] = total_obs
            df.at[taxon_id, 'leaf_taxa_count'] = leaf_count
            df.at[taxon_id, 'child_ids'] = child_ids_str
            df.at[taxon_id, 'iconic_taxon_id'] = iconic_taxon_id

    return df


def _process_taxa_chunk(args: tuple) -> list:
    """Worker function to process a chunk of taxa in parallel.

    Must be at module level for pickling.

    Args:
        args: Tuple of (chunk_data, iconic_taxa_set, task_name, progress_queue) where:
              - chunk_data is a list of (taxon_id, child_ids, own_obs, child_obs_sum,
                child_leaf_sum, ancestor_str)
              - iconic_taxa_set is the set of iconic taxon IDs
              - task_name is the name of the task for progress reporting (e.g., "Level 6")
              - progress_queue is optional queue for progress updates

    Returns:
        List of (taxon_id, total_obs, leaf_count, child_ids_str, iconic_taxon_id)
    """
    chunk_data, iconic_taxa_set, task_name, progress_queue = args
    results = []
    total = len(chunk_data)
    report_interval = max(1, total // 10)  # Report ~10 times per chunk

    for i, (taxon_id, child_ids, own_obs, child_obs_sum, child_leaf_sum, ancestor_str) in enumerate(
        chunk_data
    ):
        # Compute aggregates
        if child_ids:
            child_ids_str = ','.join(map(str, child_ids))
            total_obs = own_obs + child_obs_sum
            leaf_count = child_leaf_sum
        else:
            child_ids_str = None
            total_obs = own_obs
            leaf_count = 1

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
        if progress_queue is not None and (i + 1) % report_interval == 0:
            progress_queue.put((task_name, report_interval))

    # Report any remaining progress
    if progress_queue is not None:
        remaining = total % report_interval
        if remaining > 0:
            progress_queue.put((task_name, remaining))

    return results


def _get_iconic_taxon_id(taxon_id: int, ancestor_ids_str: Optional[str]) -> Optional[int]:
    """Get the most specific iconic taxon for a given taxon"""
    # Check ancestors + self in reverse order (most specific first)
    if isinstance(ancestor_ids_str, str):
        ancestor_ids = [int(x) for x in ancestor_ids_str.split(',')] + [taxon_id]
        for ancestor_id in reversed(ancestor_ids):
            if ancestor_id in ICONIC_TAXA:
                return ancestor_id
    return None


class ProgressQueues:
    """Container for multiprocessing queues used for progress reporting."""

    def __init__(self):
        manager = Manager()
        self.progress_queue = manager.Queue()
        self.task_queue = manager.Queue()
        self.log_queue = manager.Queue()
        self._progress_proc: Optional[Process] = None

    def start(self, total: int):
        """Start the progress display process."""
        self._progress_proc = Process(
            target=self._update_progress,
            args=(total,),
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

    def _update_progress(self, total: int):
        """Pull from multiprocessing queues and update progress display.

        Runs in a separate process to handle progress updates from parallel workers.

        Args:
            progress_queue: Queue for (task_name, n_completed) progress updates
            task_queue: Queue for (task_name, task_desc, total) new tasks, or None to stop
            log_queue: Queue for log messages to display
            total: Total for the overall progress bar
        """
        progress = ParallelMultiProgress(total=total)
        pending: List[Tuple[str, int]] = []
        refresh_rate = 10  # ticks per second

        with progress:
            while True:
                # Show any log messages
                while not self.log_queue.empty():
                    try:
                        msg = self.log_queue.get_nowait()
                        progress.log(msg)
                    except Exception:
                        break

                # Check for new tasks or stop signal
                while not self.task_queue.empty():
                    try:
                        item = self.task_queue.get_nowait()
                        if item is None:
                            # Stop signal received
                            return
                        task_name, task_desc, task_total = item
                        progress.start_job(task_name, task_total, task_desc)
                    except Exception:
                        break

                # Collect progress updates
                while not self.progress_queue.empty():
                    try:
                        pending.append(self.progress_queue.get_nowait())
                    except Exception:
                        break

                # Apply progress updates
                completed = pending.copy()
                pending = []
                for task_name, n_completed in completed:
                    if task_name in progress.job_names:
                        progress.advance(task_name, n_completed)
                    else:
                        # Task not registered yet; retry next iteration
                        pending.append((task_name, n_completed))

                sleep(1 / refresh_rate)
