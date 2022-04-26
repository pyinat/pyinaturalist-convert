"""Utilities to help load date into a SQLite database"""
# TODO: Indexes
# TODO: Progress bar!
import sqlite3
from csv import reader as csv_reader
from pathlib import Path
from time import time
from typing import Dict, List

from .constants import PathOrStr


def load_table(
    csv_path: PathOrStr,
    db_path: PathOrStr,
    column_map: Dict,
    pk: str = 'id',
    table_name: str = None,
):
    """Load a CSV file into a sqlite3 table.
    This is less efficient than the sqlite3 shell `.import` command, but easier to use.

    Args:
        csv_path: Path to CSV file
        db_path: Path to SQLite database
        column_map: Dictionary mapping CSV column names to SQLite column names. And columns not
            listed will be ignored.
        pk: Primary key column name
        table_name: Name of table to load into
    """
    csv_path = Path(csv_path).expanduser()
    db_path = Path(db_path).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    print(f'Loading {csv_path} into {db_path}')

    table_name = table_name or db_path.stem
    table_cols = ', '.join([f'{k} TEXT' for k in column_map.values() if k != pk])
    csv_cols = list(column_map.keys())
    placeholders = ','.join(['?'] * len(column_map))
    start = time()

    with sqlite3.connect(db_path) as conn, open(csv_path) as f:
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS {table_name} ({pk} INTEGER PRIMARY KEY, {table_cols})'
        )
        reader = ChunkReader(f, fields=csv_cols)
        for chunk in reader:
            conn.executemany(f'INSERT OR REPLACE INTO {table_name} VALUES ({placeholders})', chunk)
        conn.commit()

    print(f'Completed in {time() - start:.2f}s')


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
