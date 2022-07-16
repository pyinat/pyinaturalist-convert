"""Utilities for working with taxonomy data"""
import sqlite3
from concurrent.futures import ProcessPoolExecutor, as_completed
from logging import getLogger
from multiprocessing import Manager, Process
from pathlib import Path
from queue import Queue
from time import sleep, time
from typing import TYPE_CHECKING, Dict, List, Optional

from pyinaturalist import Taxon
from pyinaturalist.constants import ICONIC_TAXA, ROOT_TAXON_ID

from .constants import DB_PATH, TAXON_COUNTS, PathOrStr
from .download import get_progress

# TODO: Could also add the total number of descendants to the taxon table (useful for display)
#      Or total leaf taxa

if TYPE_CHECKING:
    from pandas import DataFrame

# Bacteria, viruses, etc.
EXCLUDE_IDS = [67333, 131236, 151817, 1228707, 1285874]


logger = getLogger(__name__)


def aggregate_taxon_db(
    db_path: PathOrStr = DB_PATH, counts_path: PathOrStr = TAXON_COUNTS
) -> 'DataFrame':
    """Add aggregate values to the taxon database:

    * Ancestor IDs
    * Child IDs
    * Iconic taxon ID
    * Aggregated observation taxon counts
    """
    import pandas as pd

    # Get taxon counts from observations table
    start = time()
    df = get_taxon_df(db_path)
    taxon_counts_dict = get_observation_taxon_counts(db_path)
    taxon_counts = pd.DataFrame(taxon_counts_dict.items(), columns=['id', 'count'])
    df = _join_counts(df, taxon_counts)

    # A queue for completed items; a separate process pulls from this to update progress bar
    manager = Manager()
    q = manager.Queue()
    progress_proc = Process(target=update_progress, args=(q, len(df)))
    progress_proc.start()

    # Parallelize by phylum; split up entire dataframe to minimize memory usage per process
    combined_df = df[df['id'] == ROOT_TAXON_ID | df['rank'] == 'kingdom']
    phyla = [
        Taxon.from_json(t)
        for t in df[df['rank'] == 'phylum'].to_dict(orient='records')
        if t['parent_id'] not in EXCLUDE_IDS
    ]

    logger.info('Partitioning taxon dataframe by phylum')
    with ProcessPoolExecutor() as executor_1, ProcessPoolExecutor() as executor_2:
        futures_to_taxon = {
            executor_1.submit(
                get_descendant_ids,
                taxon_id=taxon.id,
                df=df[['id', 'parent_id']],
                # q=q,
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
                    aggregate_branch,
                    df[df['id'].isin(descenant_ids)].copy(),
                    # df[df['id'].isin(descenants)][
                    #     ['id', 'parent_id', 'ancestor_ids', 'child_ids']
                    # ].copy(),
                    taxon_id=taxon.id,
                    taxon_name=taxon.name,
                    ancestor_ids=[ROOT_TAXON_ID, taxon.parent_id],
                    q=q,
                )
            )

        # As each subtree is completed, recombine into a single dataframe
        for future in as_completed(stage_2_futures):
            sub_df = future.result()
            combined_df = pd.concat([combined_df, sub_df], ignore_index=True)

    # Process kingdoms
    df = _aggregate_kingdoms(combined_df)

    # Save taxon counts for future use
    if counts_path:
        _save_taxon_counts(df, counts_path)
    save_taxon_df(df, db_path)

    progress_proc.terminate()
    logger.debug(f'Elapsed: {time()-start:.2f}s')
    return df


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
            'WHERE taxon_id IS NOT NULL '
            'GROUP BY taxon_id;'
        ).fetchall()

        return {
            int(row['taxon_id']): int(row['count'])
            for row in sorted(rows, key=lambda r: r['count'], reverse=True)
        }


def get_descendant_ids(
    taxon_id: int,
    db_path: PathOrStr = DB_PATH,
    df: 'DataFrame' = None,
    q: Queue = None,
) -> List[int]:
    """Recursively get all descendant taxon IDs (down to leaf taxa) for the given taxon"""
    import pandas as pd

    logger.debug(f'Finding descendants of {taxon_id}')

    if df is None:
        df = get_taxon_df(db_path)

    def _get_descendants_rec(parent_id):
        child_ids = df[(df['parent_id'] == parent_id)]['id']
        combined = pd.concat([child_ids] + [_get_descendants_rec(c) for c in child_ids])
        if q:
            q.put(len(child_ids))
        return combined

    return [taxon_id] + list(_get_descendants_rec(taxon_id))


def aggregate_branch(
    df: 'DataFrame',
    taxon_id: int,
    taxon_name: str = None,
    ancestor_ids: List[int] = None,
    q: Queue = None,
) -> 'DataFrame':
    """Add aggregate values to all descendants of a given taxon"""
    logger.debug(f'Processing phylum {taxon_name} ({len(df)} taxa)')

    def aggregate_rec(taxon_id, ancestor_ids: List[int]):
        # Process children first, to update counts
        child_ids = list(df[df['parent_id'] == taxon_id]['id'])
        for child_id in child_ids:
            aggregate_rec(child_id, ancestor_ids + [taxon_id])
        if q:
            q.put(len(child_ids))

        # Get combined child counts
        children = df[df['parent_id'] == taxon_id]
        obs_count = children['count'].sum()
        leaf_count = children['leaf_taxon_count'].sum()
        if len(children) == 0:  # Current taxon is a leaf
            leaf_count = 1

        # Process current taxon
        mask = df['id'] == taxon_id
        df.loc[mask] = df.loc[mask].apply(
            lambda row: _update_row(row, ancestor_ids, child_ids, obs_count, leaf_count),
            axis=1,
        )

    aggregate_rec(taxon_id, ancestor_ids or [ROOT_TAXON_ID])
    logger.debug(f'Completed {taxon_name}')
    return df


def _aggregate_kingdoms(df: 'DataFrame') -> 'DataFrame':
    """Process kingdoms (in main thread) after all phyla have been processed"""
    for taxon_id in df[df['rank'] == 'kingdom']['id']:
        children = df[df['parent_id'] == taxon_id]
        mask = df['id'] == taxon_id
        df.loc[mask] = df.loc[mask].apply(
            lambda row: _update_row(
                row,
                [ROOT_TAXON_ID],
                list(children['id']),
                children['count'].sum(),
                children['leaf_taxon_count'].sum(),
            ),
            axis=1,
        )
    return df


def _update_row(
    row, ancestor_ids: List[int], child_ids: List[int], agg_count: int = 0, leaf_count: int = 0
):
    """Update aggregate values for a single taxon"""

    def _join_ids(ids: List[int]) -> Optional[str]:
        return ','.join(map(str, ids)) if ids else None

    iconic_taxon_id = next((i for i in ancestor_ids if i in ICONIC_TAXA), None)
    row['ancestor_ids'] = _join_ids(ancestor_ids)
    row['child_ids'] = _join_ids(child_ids)
    row['iconic_taxon_id'] = str(iconic_taxon_id)
    row['count'] += agg_count
    row['leaf_taxon_count'] += leaf_count
    return row


def get_taxon_df(db_path: PathOrStr = DB_PATH) -> 'DataFrame':
    """Load taxon table into a dataframe"""
    import pandas as pd

    logger.info(f'Loading taxa from {db_path}')
    df = pd.read_sql_query('SELECT * FROM taxon', sqlite3.connect(db_path))
    df['parent_id'] = df['parent_id'].astype(pd.Int64Dtype())
    return df


def save_taxon_df(df: 'DataFrame', db_path: PathOrStr = DB_PATH):
    """Save taxon dataframe back to SQLite; clear and reuse existing table to keep indexes"""
    logger.info('Saving taxon counts to database')
    with sqlite3.connect(db_path) as conn:
        conn.execute('DELETE FROM taxon')
        df.to_sql('taxon', conn, if_exists='append', index=False)
        conn.execute('VACUUM')


def update_progress(q: Queue, total: int):
    """Pull from a multiprocessing queue and update progress"""
    progress = get_progress()
    task = progress.add_task('[cyan]Processing...', total=total)

    with progress:
        while True:
            while not q.empty():
                n_completed = q.get()
                progress.advance(task, n_completed)
            sleep(0.1)


def update_taxon_counts(
    db_path: PathOrStr = DB_PATH, counts_path: PathOrStr = TAXON_COUNTS
) -> 'DataFrame':
    """Load previously saved taxon counts into the local taxon database"""
    import pandas as pd

    taxon_counts = pd.read_parquet(counts_path)
    df = get_taxon_df(db_path)
    df = _join_counts(df, taxon_counts)
    save_taxon_df(df, db_path)
    return df


def _join_counts(df: 'DataFrame', taxon_counts: 'DataFrame') -> 'DataFrame':
    """Join taxon dataframe with updated taxon counts"""
    from numpy import int64

    df = df.set_index('id')
    df = df.drop('count', axis=1)
    taxon_counts = taxon_counts.set_index('id')
    df = df.join(taxon_counts)
    df['count'] = df['count'].fillna(0).astype(int64)
    df['leaf_taxon_count'] = 0

    return df.rename_axis('id').reset_index()


def _save_taxon_counts(df: 'DataFrame', counts_path: PathOrStr = TAXON_COUNTS):
    """Save a minimal copy of taxon observation counts + leaf taxon counts"""
    counts_path = Path(counts_path)
    counts_path.parent.mkdir(parents=True, exist_ok=True)
    df2 = df.set_index('id')
    df2 = df2[['count', 'leaf_taxon_count']]
    df2 = df2.sort_values('count', ascending=False)
    df2.to_parquet(counts_path)
