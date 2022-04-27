"""Utilities to help load date into a SQLite database"""
import sqlite3
from csv import reader as csv_reader
from logging import getLogger
from pathlib import Path
from time import time
from typing import Dict, List

from .constants import PathOrStr
from .download import MultiProgress

logger = getLogger(__name__)


class ChunkReader:
    """A CSV reader that yields chunks of rows

    Args:
        chunk_size: Number of rows to yield at a time
        fields: List of fields to include in each chunk
    """

    def __init__(self, f, chunk_size: int = 2000, fields: List[str] = None, **kwargs):
        self.reader = csv_reader(f, **kwargs)
        self._chunk_size = chunk_size

        # Determine which fields to include (by index)
        field_names = next(self.reader)
        self._include_idx = [field_names.index(k) for k in fields] if fields else None

    def __iter__(self):
        return self

    def __next__(self):
        chunk = []
        try:
            for _ in range(self._chunk_size):
                row = next(self.reader)
                chunk.append([row[i] for i in self._include_idx] if self._include_idx else row)
        except StopIteration:
            # Ignore first StopIteration to return final chunk
            if not chunk:
                raise
        return chunk


# TODO: Indexes
# TODO: Load all columns with original names if a column map isn't provided
def load_table(
    csv_path: PathOrStr,
    db_path: PathOrStr,
    table_name: str = None,
    column_map: Dict = None,
    pk: str = 'id',
    fts5: bool = False,
    progress: MultiProgress = None,
):
    """Load a CSV file into a sqlite3 table.
    This is less efficient than the sqlite3 shell `.import` command, but easier to use.

    Args:
        csv_path: Path to CSV file
        db_path: Path to SQLite database
        table_name: Name of table to load into
        column_map: Dictionary mapping CSV column names to SQLite column names. And columns not
            listed will be ignored.
        pk: Primary key column name
        fts5: Create a full-text search table instead of a regular table
        progress: Progress bar, if tracking loading from multiple files
    """
    if column_map is None:
        raise NotImplementedError

    csv_path = Path(csv_path).expanduser()
    db_path = Path(db_path).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f'Loading {csv_path} into {db_path}')

    table_name = table_name or db_path.stem
    non_pk_cols = [k for k in column_map.values() if k != pk]
    csv_cols = list(column_map.keys())
    placeholders = ','.join(['?'] * len(column_map))
    start = time()

    if progress:
        progress.start_job(csv_path)

    with sqlite3.connect(db_path) as conn, open(csv_path) as f:
        _create_table(conn, table_name, non_pk_cols, pk, fts5)
        for chunk in ChunkReader(f, fields=csv_cols):
            conn.executemany(f'INSERT OR REPLACE INTO {table_name} VALUES ({placeholders})', chunk)
            if progress:
                progress.advance(len(chunk))
        conn.commit()

    logger.info(f'Completed in {time() - start:.2f}s')


def _create_table(conn, table_name, non_pk_cols, pk, fts5):
    # For text search, the "pk" will be the indexed text column; all others are unindexed
    if fts5:
        table_cols = [pk] + [f'{k} UNINDEXED' for k in non_pk_cols]
        stmt = (
            f'CREATE VIRTUAL TABLE IF NOT EXISTS {table_name} USING fts5({", ".join(table_cols)});'
        )
    # For regular tables, assume an integer pk and text columns for the rest
    else:
        table_cols = ', '.join([f'{k} TEXT' for k in non_pk_cols])
        table_cols = [f'{pk} INTEGER PRIMARY KEY'] + [f'{k} TEXT' for k in non_pk_cols]
        stmt = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(table_cols)});'
    conn.execute(stmt)