"""Helper classes and functions to load data directly from CSV into a SQLite database


.. automodsumm:: pyinaturalist_convert.sqlite
   :classes-only:
   :nosignatures:

.. automodsumm:: pyinaturalist_convert.sqlite
   :functions-only:
   :nosignatures:
"""
import sqlite3
from csv import DictReader
from csv import reader as csv_reader
from logging import getLogger
from pathlib import Path
from time import time
from typing import Callable, Dict, List

from .constants import DB_PATH, PathOrStr
from .download import MultiProgress, get_progress_spinner

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
                chunk.append(self._next_row())
        except StopIteration:
            # Ignore first StopIteration to return final chunk
            if not chunk:
                raise
        return chunk

    def _next_row(self):
        row = next(self.reader)
        return [row[i] or None for i in self._include_idx] if self._include_idx else row


class XFormChunkReader(ChunkReader):
    """A CSV reader that yields chunks of rows, and applies a transform callback to each row

    Args:
        chunk_size: Number of rows to yield at a time
        fields: List of fields to include in each chunk
        transform: Callback to transform a row before inserting into the database
    """

    def __init__(
        self,
        f,
        chunk_size: int = 2000,
        fields: List[str] = None,
        transform: Callable = None,
        **kwargs,
    ):
        self.reader = DictReader(f, **kwargs)  # type: ignore
        self._chunk_size = chunk_size
        self.include_fields = fields
        self.transform = transform or (lambda x: x)

    def __iter__(self):
        return self

    def _next_row(self) -> list:
        row = self.transform(next(self.reader))
        return [row[f] for f in self.include_fields] if self.include_fields else row


def get_fields(csv_path: PathOrStr, delimiter: str = ',') -> list[str]:
    with open(csv_path) as f:
        reader = csv_reader(f, delimiter=delimiter)
        return next(reader)


# TODO: Load all columns with original names if a column map isn't provided
def load_table(
    csv_path: PathOrStr,
    db_path: PathOrStr,
    table_name: str = None,
    column_map: Dict = None,
    pk: str = 'id',
    progress: MultiProgress = None,
    delimiter: str = ',',
    transform: Callable = None,
):
    """Load a CSV file into a sqlite3 table.
    This is less efficient than the sqlite3 shell `.import` command, but easier to use.

    Args:
        csv_path: Path to CSV file
        db_path: Path to SQLite database
        table_name: Name of table to load into (defaults to db_path basename)
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

    table_name = table_name or db_path.stem
    non_pk_cols = [k for k in db_cols if k != pk]
    columns_str = ', '.join(db_cols)
    placeholders = ','.join(['?'] * len(csv_cols))
    start = time()

    if progress:
        progress.start_job(csv_path)

    with sqlite3.connect(db_path) as conn, open(csv_path) as f:
        conn.execute('PRAGMA synchronous = 0')
        conn.execute('PRAGMA journal_mode = MEMORY')
        _create_table(conn, table_name, non_pk_cols, pk)
        stmt = f'INSERT OR REPLACE INTO {table_name} ({columns_str}) VALUES ({placeholders})'

        if not transform:
            reader = ChunkReader(f, fields=csv_cols, delimiter=delimiter)
        else:
            reader = XFormChunkReader(f, fields=csv_cols, delimiter=delimiter, transform=transform)

        for chunk in reader:
            conn.executemany(stmt, chunk)
            if progress:
                progress.advance(len(chunk))
        conn.commit()

    logger.info(f'Completed in {time() - start:.2f}s')


def vacuum_analyze(table_names: List[str], db_path: PathOrStr = DB_PATH):
    """Vacuum a SQLite database and analzy one or more tables. If loading multiple tables, this
    should be done once after loading all of them.
    """
    spinner = get_progress_spinner('Final cleanup')
    with spinner, sqlite3.connect(db_path) as conn:
        conn.execute('VACUUM')
        for table_name in table_names:
            conn.execute(f'ANALYZE {table_name}')


def _create_table(conn, table_name, non_pk_cols, pk):
    # Assume an integer primary key and text columns for the rest
    table_cols = [f'{pk} INTEGER PRIMARY KEY'] + [f'{k} TEXT' for k in non_pk_cols]
    conn.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(table_cols)});')
