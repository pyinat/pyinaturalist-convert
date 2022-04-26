"""Utilities for working with the iNat GBIF DwC archive"""
# TODO: Some more helper functions (or at least examples) for loading into a database.
# * Rename columns, ignore some redundant ones, add indexes, etc.
# * Create and load table with python sqlite3 instead of sqlite3 shell?
import sqlite3
from csv import reader as csv_reader
from os.path import basename, splitext
from pathlib import Path
from time import time
from typing import Dict, List

from .constants import DATA_DIR, DWCA_DIR, DWCA_TAXA_URL, DWCA_URL, PathOrStr
from .download import check_download, download_file, unzip_progress

TAXON_COLUMN_MAP = {
    # 'id': 'id',
    # 'kingdom': 'kingdom',
    # 'phylum': 'phylum',
    # 'class': 'class',
    # 'order': 'order',
    # 'family': 'family',
    # 'genus': 'genus',
    # 'specificEpithet': 'species',
    # 'infraspecificEpithet': 'infraspecies',
    'scientificName': 'name',
    'taxonRank': 'rank',
    # 'taxonID',
    # 'identifier',
    # 'parentNameUsageID',
    # 'modified',
    # 'references',
}


def download_dwca(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the GBIF DwC-A export. Reuses local data if it already exists and is
    up to date.

    Example to load into a SQLite database (using the `sqlite3` shell, from bash):

    .. highlight:: bash

        export DATA_DIR="$HOME/.local/share/pyinaturalist"
        sqlite3 -csv $DATA_DIR/observations.db ".import $DATA_DIR/gbif-observations-dwca/observations.csv observations"

    Args:
        download_dir: Alternative download directory
    """
    _download_archive(DWCA_URL, dest_dir)


def download_taxa(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the DwC-A taxonomy export. Reuses local data if it already exists and is
    up to date.

    Example to load into a SQLite database (using the `sqlite3` shell, from bash):

    .. highlight:: bash

        export DATA_DIR="$HOME/.local/share/pyinaturalist"
        sqlite3 -csv $DATA_DIR/taxa.db ".import $DATA_DIR/inaturalist-taxonomy.dwca/taxa.csv taxa"

    Args:
        download_dir: Alternative download directory
    """
    _download_archive(DWCA_TAXA_URL, dest_dir)


def test_load_table():
    load_table(
        DATA_DIR / 'inaturalist-taxonomy.dwca' / 'taxa.csv',
        DATA_DIR / 'taxa.db',
        column_map=TAXON_COLUMN_MAP,
    )


def load_table(csv_path: PathOrStr, db_path: PathOrStr, column_map: Dict, table_name: str = None):
    """Load a CSV file into a sqlite3 table.
    This is less efficient than the sqlite3 shell `.import` command, but easier to use.

    Args:
        csv_path: Path to CSV file
        db_path: Path to SQLite database
        column_map: Dictionary mapping CSV column names to SQLite column names. And columns not
            listed will be ignored.
        table_name: Name of table to load into
    """
    csv_path = Path(csv_path).expanduser()
    db_path = Path(db_path).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    table_name = table_name or db_path.stem
    table_cols = ', '.join([f'{k} TEST' for k in column_map.values()])
    csv_cols = list(column_map.keys())
    start = time()

    with sqlite3.connect(db_path) as conn, open(csv_path) as f:
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY, {table_cols})'
        )
        reader = ChunkReader(f, fields=['id'] + csv_cols)
        for chunk in reader:
            conn.executemany(f'INSERT OR REPLACE INTO {table_name} VALUES (?,?,?)', chunk)
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


def _download_archive(url: str, dest_dir: PathOrStr = DATA_DIR):
    dest_dir = Path(dest_dir).expanduser()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / basename(url)

    # Skip download if we're already up to date
    if check_download(dest_file, url=url, release_interval=7):
        return

    # Otherwise, download and extract files
    download_file(url, dest_file)
    unzip_progress(dest_file, dest_dir / splitext(basename(url))[0])


def get_dwca_reader(dest_path: PathOrStr = DWCA_DIR):
    """Get a :py:class:`~dwca.DwCAReader` for the GBIF DwC archive.

    Args:
        dwca_dir: Alternative archive file path (zipped) or directory (extracted)
    """
    from dwca.read import DwCAReader

    # Extract the archive, if it hasn't already been done
    dest_path = Path(dest_path).expanduser()
    if dest_path.is_file():
        subdir = splitext(basename(dest_path))[0]
        unzip_progress(dest_path, dest_path / subdir)
        dest_path = dest_path / subdir

    return DwCAReader(dest_path)
