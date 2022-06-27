from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager, Process, Queue
from time import sleep
from typing import TYPE_CHECKING, List, Optional

from .constants import DB_PATH, ICONIC_TAXA, PathOrStr
from .download import get_progress
from .dwca import _get_taxon_df, _save_taxon_df

# TODO: Could also add the total number of descendants to the taxon table (useful for display)

if TYPE_CHECKING:
    from pandas import DataFrame

ICONIC_TAXON_IDS = list(ICONIC_TAXA.keys())[::-1]
ICONIC_TAXON_IDS.remove(0)
ROOT_TAXON_ID = 48460


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


def add_ancestry(db_path: PathOrStr = DB_PATH) -> 'DataFrame':
    import pandas as pd

    df = _get_taxon_df(db_path)
    manager = Manager()
    q = manager.Queue()

    progress_proc = Process(target=update_progress, args=(q, len(df)))
    progress_proc.start()

    # Parallelize by direct descendants of the root taxon
    branch_ids = df[df['parent_id'] == ROOT_TAXON_ID]['id']
    combined_df = df[df['id'] == ROOT_TAXON_ID]

    with ProcessPoolExecutor(max_workers=1) as executor:
        futures = [
            executor.submit(add_ancestry_branch, df.copy(), branch_id, q)
            for branch_id in branch_ids
        ]
        # futures = [executor.submit(dummy, df, branch_id, q) for branch_id in branch_ids]
        for future in as_completed(futures):
            sub_df = future.result()
            combined_df = pd.concat([combined_df, sub_df], ignore_index=True)

    # progress_proc.join()
    progress_proc.terminate()
    # _save_taxa_df(df, db_path)
    return combined_df


def dummy(df: 'DataFrame', branch_id: int, q: Queue) -> 'DataFrame':
    for i in range(100):
        q.put(10)
        sleep(0.1)
    return df[df['id'] == branch_id]


def add_ancestry_branch(df: 'DataFrame', branch_id: int, q: Queue) -> 'DataFrame':
    print(f'Processing {branch_id}')

    def add_ancestry_rec(taxon_id, ancestor_ids: List[int]):
        child_ids = list(df[df['parent_id'] == taxon_id]['id'])
        print(f'{taxon_id}: {child_ids}')
        mask = df['id'] == taxon_id
        df.loc[mask] = df.loc[mask].apply(
            lambda row: _update_ids(row, ancestor_ids, child_ids), axis=1
        )

        for child_id in child_ids:
            add_ancestry_rec(child_id, ancestor_ids + [taxon_id])
        q.put(len(child_ids))

    add_ancestry_rec(branch_id, [ROOT_TAXON_ID])
    return df[df['ancestor_ids'].notnull()]


def _update_ids(row, ancestor_ids, child_ids):
    """Update ancestor, child, and iconic taxon IDs for a single taxon"""
    iconic_taxon_id = next((i for i in ancestor_ids if i in ICONIC_TAXON_IDS), None)
    row['ancestor_ids'] = _join_ids(ancestor_ids)
    row['child_ids'] = _join_ids(child_ids)
    row['iconic_taxon_id'] = str(iconic_taxon_id)
    return row


def _join_ids(ids: List[int] = None) -> Optional[str]:
    return ','.join(map(str, ids)) if ids else None


def _get_descendants(taxon_id: int, db_path: PathOrStr = DB_PATH) -> List[int]:
    """Recursively get all descendant taxon IDs (down to leaf taxa) for the given taxon"""
    import pandas as pd

    df = _get_taxon_df(db_path)

    def _get_descendants_rec(parent_id):
        child_ids = df[df['parent_id'] == parent_id]['id']
        return pd.concat([child_ids] + [_get_descendants_rec(c) for c in child_ids])

    return list(_get_descendants_rec(taxon_id))
