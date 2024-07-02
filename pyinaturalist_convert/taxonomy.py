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
from concurrent.futures import ProcessPoolExecutor, as_completed
from logging import getLogger
from multiprocessing import Manager, Process
from pathlib import Path
from queue import Queue
from time import sleep, time
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

from pyinaturalist import Taxon
from pyinaturalist.constants import ICONIC_TAXA, ROOT_TAXON_ID

from .constants import DB_PATH, DWCA_TAXON_CSV_DIR, TAXON_AGGREGATES_PATH, PathOrStr
from .download import ParallelMultiProgress

if TYPE_CHECKING:
    from pandas import DataFrame

DEFAULT_LANG_CSV = DWCA_TAXON_CSV_DIR / 'VernacularNames-english.csv'
# Bacteria, viruses, etc.
EXCLUDE_IDS = [67333, 131236, 151817, 1228707, 1285874]
# Most populous phylum IDs to start processing first
LOAD_FIRST_IDS = [47120, 211194, 2]
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
        progress_bars: Show detailed progress bars in addition to log output
    """
    import pandas as pd

    # Get taxon counts from observations table
    start = time()
    df = _get_taxon_df(db_path)
    taxon_counts_dict = get_observation_taxon_counts(db_path)
    taxon_counts = pd.DataFrame(taxon_counts_dict.items(), columns=['id', 'observations_count_rg'])
    df = _join_taxon_agg(df, taxon_counts)
    n_phyla = len(df[(df['rank'] == 'phylum') & ~df['parent_id'].isin(EXCLUDE_IDS)])
    n_kingdoms = len(df[(df['rank'] == 'kingdom')])

    # Optionally run without fancy progress bars
    if not progress_bars:
        df = _aggregate_taxon_db(df, db_path, backup_path, common_names_path)
        return df

    # Set up a separate process to manage progress updates via queues
    manager = Manager()
    progress_queue = manager.Queue()
    task_queue = manager.Queue()
    log_queue = manager.Queue()
    progress_total = len(df) + n_phyla + n_kingdoms + 1
    progress_proc = Process(
        target=_update_progress, args=(progress_queue, task_queue, log_queue, progress_total)
    )
    progress_proc.start()

    try:
        _aggregate_taxon_db(
            df, db_path, backup_path, common_names_path, progress_queue, task_queue, log_queue
        )
        logger.info(f'Completed in {time()-start:.2f}s')
    except Exception as e:
        logger.exception(e)
    finally:
        progress_proc.terminate()
    return df


def _aggregate_taxon_db(
    df: 'DataFrame',
    db_path: PathOrStr = DB_PATH,
    backup_path: PathOrStr = TAXON_AGGREGATES_PATH,
    common_names_path: PathOrStr = DEFAULT_LANG_CSV,
    progress_queue: Optional[Queue] = None,
    task_queue: Optional[Queue] = None,
    log_queue: Optional[Queue] = None,
) -> 'DataFrame':
    import pandas as pd

    # Write a log message to either a stdlib logger, or rich's progress logger
    # (for better formatting that doesn't mangle progress bars)
    log_func: Callable[[str], None] = log_queue.put if log_queue else logger.info  # type: ignore
    q_kwargs = {'progress_queue': progress_queue, 'task_queue': task_queue}

    # Get common names from CSV
    common_names = _get_common_names(common_names_path, **q_kwargs)

    # Parallelize by phylum; split up entire dataframe to minimize memory usage per process
    combined_df = df[(df['id'] == ROOT_TAXON_ID) | (df['rank'] == 'kingdom')]
    phyla = [
        Taxon.from_json(taxon)
        for taxon in df[df['id'].isin(LOAD_FIRST_IDS)].to_dict(orient='records')
    ]
    phyla.extend(
        [
            Taxon.from_json(taxon)
            for taxon in df[df['rank'] == 'phylum'].to_dict(orient='records')
            if taxon['parent_id'] not in (EXCLUDE_IDS)
            and taxon['id'] not in (EXCLUDE_IDS + LOAD_FIRST_IDS)
        ]
    )

    with ProcessPoolExecutor() as executor_1, ProcessPoolExecutor() as executor_2:
        log_func('Partitioning tasks by phylum')
        futures_to_taxon = {
            executor_1.submit(
                _get_descendant_ids,
                taxon.id,
                taxon_name=taxon.name,
                df=df[['id', 'parent_id']],
                **q_kwargs,  # type: ignore
            ): taxon
            for taxon in phyla
        }

        # Process each phylum subtree
        stage_2_futures = []
        for future in as_completed(futures_to_taxon):
            taxon = futures_to_taxon[future]
            descenant_ids = future.result()
            stage_2_futures.append(
                executor_2.submit(
                    _aggregate_branch,
                    df[df['id'].isin(descenant_ids)].copy(),
                    taxon_id=taxon.id,
                    taxon_name=taxon.name,
                    ancestor_ids=[ROOT_TAXON_ID, taxon.parent_id],
                    common_names=common_names,
                    **q_kwargs,
                )
            )

        # As each subtree is completed, recombine into a single dataframe
        log_func('Combining results')
        for future in as_completed(stage_2_futures):
            sub_df = future.result()
            combined_df = pd.concat([combined_df, sub_df], ignore_index=True)

    # Process kingdoms
    df = _aggregate_kingdoms(combined_df, common_names, **q_kwargs)

    # Save all results back to the database, plus a minimal backup
    _save_taxon_agg(df, backup_path)
    _save_taxon_df(df, db_path)
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


def _get_descendant_ids(
    taxon_id: int,
    taxon_name: Optional[str] = None,
    db_path: PathOrStr = DB_PATH,
    df: Optional['DataFrame'] = None,
    progress_queue: Optional[Queue] = None,
    task_queue: Optional[Queue] = None,
) -> List[int]:
    """Recursively get all descendant taxon IDs (down to leaf taxa) for the given taxon"""
    import pandas as pd

    task_name = f'phylum {taxon_name}'
    if task_queue:
        task_queue.put((task_name, 'Finding descendants of', 1))

    if df is None:
        df = _get_taxon_df(db_path)

    def _get_descendants_rec(parent_id):
        child_ids = df[(df['parent_id'] == parent_id)]['id']
        combined = pd.concat([child_ids] + [_get_descendants_rec(c) for c in child_ids])
        return combined

    descendant_ids = [taxon_id] + list(_get_descendants_rec(taxon_id))
    if progress_queue:
        progress_queue.put((task_name, 1))
    return descendant_ids


def _aggregate_branch(
    df: 'DataFrame',
    taxon_id: int,
    taxon_name: Optional[str] = None,
    ancestor_ids: Optional[List[int]] = None,
    common_names: Optional[Dict[int, str]] = None,
    progress_queue: Optional[Queue] = None,
    task_queue: Optional[Queue] = None,
) -> 'DataFrame':
    """Add aggregate values to all descendants of a given taxon"""
    common_names = common_names or {}
    task_name = f'phylum {taxon_name}'
    if task_queue:
        task_queue.put((task_name, 'Loading', len(df) - 1))

    def aggregate_rec(taxon_id, ancestor_ids: List[int]):
        # Process children first, to update counts
        child_ids = list(df[df['parent_id'] == taxon_id]['id'])
        for child_id in child_ids:
            aggregate_rec(child_id, ancestor_ids + [taxon_id])
        if progress_queue:
            progress_queue.put((task_name, len(child_ids)))

        # Get combined child counts
        children = df[df['parent_id'] == taxon_id]
        obs_count = children['observations_count_rg'].sum()
        leaf_count = children['leaf_taxa_count'].sum()
        common_name = common_names.get(taxon_id)  # type: ignore
        if len(children) == 0:  # Current taxon is a leaf
            leaf_count = 1

        # Process current taxon
        mask = df['id'] == taxon_id
        df.loc[mask] = df.loc[mask].apply(
            lambda row: _update_taxon(
                row, ancestor_ids, child_ids, obs_count, leaf_count, common_name
            ),
            axis=1,
        )

    aggregate_rec(taxon_id, ancestor_ids or [ROOT_TAXON_ID])
    return df


def _aggregate_kingdoms(
    df: 'DataFrame',
    common_names: Optional[Dict[int, str]] = None,
    progress_queue: Optional[Queue] = None,
    task_queue: Optional[Queue] = None,
) -> 'DataFrame':
    """Process kingdoms + root taxon (in main thread) after all phyla have been processed"""
    common_names = common_names or {}
    kingdom_ids = list(df[df['rank'] == 'kingdom']['id'])

    if task_queue:
        task_queue.put(('kingdoms', 'Loading', len(kingdom_ids) + 1))

    for taxon_id in kingdom_ids + [ROOT_TAXON_ID]:
        children = df[df['parent_id'] == taxon_id]
        ancestor_ids = [] if taxon_id == ROOT_TAXON_ID else [ROOT_TAXON_ID]
        mask = df['id'] == taxon_id
        df.loc[mask] = df.loc[mask].apply(
            lambda row: _update_taxon(
                row,
                ancestor_ids,
                list(children['id']),
                children['observations_count_rg'].sum(),
                children['leaf_taxa_count'].sum(),
                common_names.get(taxon_id),
            ),
            axis=1,
        )

        if progress_queue:
            progress_queue.put(('kingdoms', 1))

    return df


def _update_taxon(
    row,
    ancestor_ids: List[int],
    child_ids: List[int],
    agg_count: int = 0,
    leaf_count: int = 0,
    common_name: Optional[str] = None,
):
    """Update aggregate values for a single taxon"""

    def _join_ids(ids: List[int]) -> Optional[str]:
        return ','.join(map(str, ids)) if ids else None

    iconic_taxon_id = next((i for i in ancestor_ids if i in ICONIC_TAXA), None)
    row['ancestor_ids'] = _join_ids(ancestor_ids) if ancestor_ids else None
    row['child_ids'] = _join_ids(child_ids)
    row['iconic_taxon_id'] = iconic_taxon_id
    row['observations_count_rg'] += agg_count
    row['leaf_taxa_count'] += leaf_count
    row['preferred_common_name'] = common_name
    return row


def _get_common_names(
    common_names_path: PathOrStr = DEFAULT_LANG_CSV,
    progress_queue: Optional[Queue] = None,
    task_queue: Optional[Queue] = None,
) -> Dict[int, str]:
    """Get common names for the specified language from DwC-A taxonomy files"""
    import pandas as pd

    # Arbitrary value to advance progress bar
    if task_queue:
        task_queue.put(('common names', 'Loading', 1))

    csv_path = Path(common_names_path).expanduser()
    if not csv_path.is_file():
        logger.warning(f'File not found: {csv_path}; common names will not be loaded')
        return {}

    logger.info(f'Loading common names from {common_names_path}')
    df = pd.read_csv(csv_path)

    # Get the first match for each taxon ID; appears to be already sorted by relevance
    df = df.drop_duplicates(subset='id', keep='first')
    df = df.set_index('id')
    df = df['vernacularName'].to_dict()

    if progress_queue:
        progress_queue.put(('common names', 1))
    return df


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


def _update_progress(
    progress_queue: Queue, task_queue: Queue, log_queue: Queue, total: int
):  # pragma: no cover
    """Pull from a multiprocessing queue and update progress"""
    progress = ParallelMultiProgress(total=total)
    pending: List[Tuple[str, int]] = []
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
