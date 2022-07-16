from concurrent.futures import ProcessPoolExecutor, as_completed
from logging import getLogger
from multiprocessing import Manager, Process
from queue import Queue
from time import sleep, time
from typing import TYPE_CHECKING, List, Optional

from pyinaturalist import Taxon
from pyinaturalist.constants import ICONIC_TAXA, ROOT_TAXON_ID

from .constants import DB_PATH, PathOrStr
from .download import get_progress
from .dwca import _get_taxon_df, _save_taxon_df

# TODO: Combine with aggregate_taxon_counts
# TODO: Could also add the total number of descendants to the taxon table (useful for display)
#      Or total leaf taxa

if TYPE_CHECKING:
    from pandas import DataFrame

ICONIC_TAXON_IDS = list(ICONIC_TAXA.keys())[::-1]
ICONIC_TAXON_IDS.remove(0)

# Bacteria, viruses, etc.
EXCLUDE_IDS = [67333, 131236, 151817, 1228707, 1285874]


logger = getLogger(__name__)


def add_ancestry(db_path: PathOrStr = DB_PATH) -> 'DataFrame':
    import pandas as pd

    start = time()
    df = _get_taxon_df(db_path)
    logger.info('Start')

    # A queue for completed items; a separate process pulls from this to update progress bar
    manager = Manager()
    q = manager.Queue()
    progress_proc = Process(target=update_progress, args=(q, len(df)))
    progress_proc.start()

    # Start by processing all kingdoms
    combined_df = df[df['rank'] == 'kingdom']
    for taxon_id in combined_df['id']:
        child_ids = list(df[df['parent_id'] == taxon_id]['id'])
        mask = df['id'] == taxon_id
        df.loc[mask] = df.loc[mask].apply(
            lambda row: _update_ids(row, [ROOT_TAXON_ID], child_ids), axis=1
        )
    combined_df = pd.concat([df[df['id'] == ROOT_TAXON_ID], combined_df], ignore_index=True)

    # Parallelize by phylum; split up entire dataframe to minimize memory usage per process
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
                    add_ancestry_branch,
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

    logger.debug(f'Elapsed: {time()-start:.2f}s')
    progress_proc.terminate()
    # _save_taxon_df(df, db_path)
    return combined_df


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
        df = _get_taxon_df(db_path)

    def _get_descendants_rec(parent_id):
        child_ids = df[(df['parent_id'] == parent_id)]['id']
        combined = pd.concat([child_ids] + [_get_descendants_rec(c) for c in child_ids])
        if q:
            q.put(len(child_ids))
        return combined

    return [taxon_id] + list(_get_descendants_rec(taxon_id))


def add_ancestry_branch(
    df: 'DataFrame',
    taxon_id: int,
    taxon_name: str = None,
    ancestor_ids: List[int] = None,
    q: Queue = None,
) -> 'DataFrame':
    """Add ancestry to all descendants of a given taxon"""
    logger.debug(f'Processing phylum {taxon_name} ({len(df)} taxa)')

    def add_ancestry_rec(taxon_id, ancestor_ids: List[int]):
        child_ids = list(df[df['parent_id'] == taxon_id]['id'])
        mask = df['id'] == taxon_id
        df.loc[mask] = df.loc[mask].apply(
            lambda row: _update_ids(row, ancestor_ids, child_ids), axis=1
        )

        for child_id in child_ids:
            add_ancestry_rec(child_id, ancestor_ids + [taxon_id])
        if q:
            q.put(len(child_ids))

    add_ancestry_rec(taxon_id, ancestor_ids or [ROOT_TAXON_ID])
    logger.debug(f'Completed {taxon_name}')
    return df


def _update_ids(row, ancestor_ids, child_ids):
    """Update ancestor, child, and iconic taxon IDs for a single taxon"""

    def _join_ids(ids: List[int]) -> Optional[str]:
        return ','.join(map(str, ids)) if ids else None

    iconic_taxon_id = next((i for i in ancestor_ids if i in ICONIC_TAXON_IDS), None)
    row['ancestor_ids'] = _join_ids(ancestor_ids)
    row['child_ids'] = _join_ids(child_ids)
    row['iconic_taxon_id'] = str(iconic_taxon_id)
    return row


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


if __name__ == "__main__":
    from pyinaturalist import enable_logging

    enable_logging('DEBUG')
    df = add_ancestry()
