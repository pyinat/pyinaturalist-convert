from concurrent.futures import ProcessPoolExecutor, as_completed
from logging import getLogger
from multiprocessing import Manager, Process
from queue import Queue
from time import sleep, time
from typing import TYPE_CHECKING, List, Optional

from pyinaturalist.constants import ICONIC_TAXA, ROOT_TAXON_ID

from .constants import DB_PATH, PathOrStr
from .download import get_progress
from .dwca import _get_taxon_df, _save_taxon_df

# TODO: Combine with aggregate_taxon_counts
# TODO: Could also add the total number of descendants to the taxon table (useful for display)
#      Or total leaf taxa
# TODO: Maybe parallelize by phylum instead?
# TODO: Handle taxa in between root and iconic taxa

if TYPE_CHECKING:
    from pandas import DataFrame

ICONIC_TAXON_IDS = list(ICONIC_TAXA.keys())[::-1]
ICONIC_TAXON_IDS.remove(0)

logger = getLogger(__name__)


def add_ancestry(db_path: PathOrStr = DB_PATH) -> 'DataFrame':
    import pandas as pd

    df = _get_taxon_df(db_path)

    # A queue for completed items; a separate process pulls from this to update progress bar
    manager = Manager()
    q = manager.Queue()
    progress_proc = Process(target=update_progress, args=(q, len(df)))
    progress_proc.start()

    # Parallelize by iconic taxa; split up entire dataframe to minimize memory usage per process
    # Note: This excludes bacteria, viruses, and archaea
    start = time()
    combined_df = df[df['id'] == ROOT_TAXON_ID]
    logger.info('Partitioning taxon dataframe')
    with ProcessPoolExecutor() as executor:
        futures_to_id = {
            executor.submit(
                get_descendants,
                taxon_id=taxon_id,
                df=df[['id', 'parent_id']],
                # For Animalia, skip descendants of other iconic taxa
                exclude_branches=ICONIC_TAXON_IDS if taxon_id == 1 else [],
                # q=q,
            ): taxon_id
            for taxon_id in ICONIC_TAXON_IDS
        }

        # DEBUG: Test progress bar
        # def _dummy(branch_id, q):
        #     for i in range(100):
        #         q.put(10)
        #         sleep(0.1)
        #     return df[df['id'] == branch_id]
        # futures = [executor.submit(_dummy, df, branch_id, q) for branch_id in ICONIC_TAXON_IDS]

        # Process each iconic taxon branch
        stage_2_futures = []
        for future in as_completed(futures_to_id):
            iconic_taxon_id = futures_to_id[future]
            descenants = future.result()
            stage_2_futures.append(
                executor.submit(
                    add_ancestry_branch,
                    df[df['id'].isin(descenants)].copy(),
                    iconic_taxon_id,
                    q,
                )
            )

        # As each branch is completed, recombine into a single dataframe
        logger.info('Adding ancestry')
        for future in as_completed(stage_2_futures):
            sub_df = future.result()
            combined_df = pd.concat([combined_df, sub_df], ignore_index=True)

    logger.debug(f'Elapsed: {time()-start:.2f}s')
    progress_proc.terminate()
    # _save_taxon_df(df, db_path)
    return combined_df


def add_ancestry_branch(df: 'DataFrame', iconic_taxon_id: int, q: Queue) -> 'DataFrame':
    """Add ancestry to all descendants of an iconic taxon"""
    taxon_name = ICONIC_TAXA.get(iconic_taxon_id)
    logger.debug(f'Processing {iconic_taxon_id} ({taxon_name})')
    df['iconic_taxon_id'] = str(iconic_taxon_id)

    def add_ancestry_rec(taxon_id, ancestor_ids: List[int]):
        child_ids = list(df[df['parent_id'] == taxon_id]['id'])
        mask = df['id'] == taxon_id
        df.loc[mask] = df.loc[mask].apply(
            lambda row: _update_ids(row, ancestor_ids, child_ids), axis=1
        )

        for child_id in child_ids:
            add_ancestry_rec(child_id, ancestor_ids + [taxon_id])
        q.put(len(child_ids))

    add_ancestry_rec(iconic_taxon_id, [ROOT_TAXON_ID])
    logger.debug(f'Completed {iconic_taxon_id} ({taxon_name})')
    return df


def _update_ids(row, ancestor_ids, child_ids):
    """Update ancestor, child, and iconic taxon IDs for a single taxon"""

    def _join_ids(ids: List[int]) -> Optional[str]:
        return ','.join(map(str, ids)) if ids else None

    row['ancestor_ids'] = _join_ids(ancestor_ids)
    row['child_ids'] = _join_ids(child_ids)
    return row


def get_descendants(
    taxon_id: int,
    db_path: PathOrStr = DB_PATH,
    df: 'DataFrame' = None,
    exclude_branches: List[int] = None,
    q: Queue = None,
) -> List[int]:
    """Recursively get all descendant taxon IDs (down to leaf taxa) for the given taxon"""
    import pandas as pd

    taxon_name = ICONIC_TAXA.get(taxon_id)
    logger.debug(f'Finding descendants of {taxon_id} ({taxon_name})')

    if df is None:
        df = _get_taxon_df(db_path)

    def _get_descendants_rec(parent_id):
        if exclude_branches:
            child_ids = df[(df['parent_id'] == parent_id) & ~df['id'].isin(exclude_branches)]['id']
        else:
            child_ids = df[(df['parent_id'] == parent_id)]['id']
        combined = pd.concat([child_ids] + [_get_descendants_rec(c) for c in child_ids])
        if q:
            q.put(len(child_ids))
        return combined

    return [taxon_id] + list(_get_descendants_rec(taxon_id))


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
