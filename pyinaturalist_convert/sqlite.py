"""Helper utilities to load data directly from CSV into a SQLite database"""

import sqlite3
from contextlib import nullcontext
from csv import reader as csv_reader
from logging import getLogger
from pathlib import Path
from time import time
from typing import Callable, Dict, List, Optional

from .constants import DB_PATH, PathOrStr
from .download import MultiProgress, get_progress_spinner

logger = getLogger(__name__)


class ChunkReader:
    """A CSV reader that yields chunks of rows, with optional per-row transforms.

    Args:
        chunk_size: Number of rows to yield at a time
        fields: List of fields to include in each chunk
        transform: Optional callback ``(row: list, field_index: dict[str, int]) -> list``
            that modifies each row in place. ``field_index`` maps CSV column names to list
            positions. For extra fields listed in *fields* but absent from the CSV header,
            the transform should append values in the order they appear.
    """

    def __init__(
        self,
        f,
        chunk_size: int = 2000,
        fields: Optional[List[str]] = None,
        transform: Optional[Callable] = None,
        **kwargs,
    ):
        self.reader = csv_reader(f, **kwargs)
        self._chunk_size = chunk_size
        self.transform = transform

        # Determine which fields to include (by index)
        field_names = next(self.reader)

        if transform and fields:
            # Build field name -> index mapping for transforms
            self._field_index: Optional[Dict[str, int]] = {
                name: i for i, name in enumerate(field_names)
            }
            # Extra fields that transforms will append (not in CSV header)
            n = len(field_names)
            for name in fields:
                if name not in self._field_index:
                    self._field_index[name] = n
                    n += 1
            self._include_idx = [self._field_index[k] for k in fields]
        else:
            self._field_index = None
            self._include_idx = [field_names.index(k) for k in fields] if fields else None

    def __iter__(self):
        return self

    def __next__(self):
        chunk = []
        try:
            for _ in range(self._chunk_size):
                chunk.append(self._next_row())
        except StopIteration:
            # Ignore first StopIteration to return final chunk
            if not chunk:
                raise
        return chunk

    def _next_row(self):
        row = next(self.reader)
        if self.transform:
            row = self.transform(row, self._field_index)
            return [row[i] for i in self._include_idx] if self._include_idx else row
        return [row[i] or None for i in self._include_idx] if self._include_idx else row


def get_fields(csv_path: PathOrStr, delimiter: str = ',') -> List[str]:
    with open(csv_path, encoding='utf-8') as f:
        reader = csv_reader(f, delimiter=delimiter)
        return next(reader)


def load_table(
    csv_path: PathOrStr,
    db_path: PathOrStr,
    table_name: Optional[str] = None,
    column_map: Optional[Dict] = None,
    pk: str = 'id',
    progress: Optional[MultiProgress] = None,
    delimiter: str = ',',
    transform: Optional[Callable] = None,
):
    """Load a CSV file into a sqlite3 table.
    This is less efficient than the sqlite3 shell `.import` command, but easier to use.

    Example:
        # Minimal example to load data into a 'taxon' table in 'my_database.db'
        >>> from pyinaturalist_convert import load_table
        >>> load_table('taxon.csv', 'my_database.db')

    Args:
        csv_path: Path to CSV file
        db_path: Path to SQLite database
        table_name: Name of table to load into (defaults to csv_path basename)
        column_map: Dictionary mapping CSV column names to SQLite column names. And columns not
            listed will be ignored.
        pk: Primary key column name
        progress: Progress bar, if tracking loading from multiple files
        transform: Callback to transform a row before inserting into the database
    """
    csv_path = Path(csv_path).expanduser()
    db_path = Path(db_path).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f'Loading {csv_path} into {db_path}')

    # Use mapping from CSV to SQLite column names, if provided; otherwise use CSV names as-is
    if not column_map:
        csv_cols = db_cols = get_fields(csv_path, delimiter)
    else:
        csv_cols = list(column_map.keys())
        db_cols = list(column_map.values())

    table_name = table_name or csv_path.stem
    staging_name = f'_staging_{table_name}'
    non_pk_cols = [k for k in db_cols if k != pk]
    columns_str = ', '.join(db_cols)
    placeholders = ','.join(['?'] * len(csv_cols))
    start = time()

    if progress:
        progress.start_job(csv_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA synchronous = 0')
        conn.execute('PRAGMA journal_mode = WAL')
        conn.execute('PRAGMA cache_size = -64000')  # 64MB page cache
        conn.execute('PRAGMA mmap_size = 268435456')  # 256MB memory-mapped I/O for faster reads

        _create_table(conn, table_name, non_pk_cols, pk)
        _create_staging_table(conn, staging_name, db_cols)
        stmt = f'INSERT INTO {staging_name} ({columns_str}) VALUES ({placeholders})'

        with open(csv_path, encoding='utf-8') as f:
            reader = ChunkReader(
                f, chunk_size=50000, fields=csv_cols, delimiter=delimiter, transform=transform
            )

            for chunk in reader:
                conn.executemany(stmt, chunk)
                if progress:
                    progress.advance(len(chunk))

        conn.execute(
            f'INSERT OR REPLACE INTO {table_name} ({columns_str}) '
            f'SELECT {columns_str} FROM {staging_name}'
        )
        conn.execute(f'DROP TABLE {staging_name}')
        conn.commit()

    logger.info(f'Completed in {time() - start:.2f}s')


def vacuum_analyze(
    table_names: List[str], db_path: PathOrStr = DB_PATH, show_spinner: bool = False
):
    """Vacuum a SQLite database and analyze one or more tables. If loading multiple tables, this
    should be done once after loading all of them.
    """
    spinner = get_progress_spinner('Final cleanup') if show_spinner else nullcontext()
    with spinner, sqlite3.connect(db_path) as conn:
        conn.execute('VACUUM')
        for table_name in table_names:
            conn.execute(f'ANALYZE {table_name}')


def _create_table(conn, table_name, non_pk_cols, pk):
    # Assume an integer primary key and text columns for the rest
    table_cols = [f'{pk} INTEGER PRIMARY KEY'] + [f'{k} TEXT' for k in non_pk_cols]
    conn.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(table_cols)});')


def _create_staging_table(conn, table_name, columns):
    """Create an unindexed staging table for fast bulk inserts"""
    conn.execute(f'DROP TABLE IF EXISTS {table_name}')
    table_cols = ', '.join(f'{k} TEXT' for k in columns)
    conn.execute(f'CREATE TABLE {table_name} ({table_cols});')
