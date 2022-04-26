"""Utilities for working with the iNat GBIF DwC archive"""
# TODO: Some more helper functions (or at least examples) for loading into a database.
# * Rename columns, ignore some redundant ones, add indexes, etc.
# * Create and load table with python sqlite3 instead of sqlite3 shell?
import sqlite3
from csv import DictReader
from csv import reader as csv_reader
from os.path import basename, splitext
from pathlib import Path
from time import time
from typing import List

from pyparsing import ParseSyntaxException

from .constants import DATA_DIR, DWCA_DIR, DWCA_TAXA_URL, DWCA_URL, PathOrStr
from .download import check_download, download_file, unzip_progress

TAXON_COLUMN_MAP = {
    'id': 'id',
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
    load_table(DATA_DIR / 'inaturalist-taxonomy.dwca' / 'taxa.csv', DATA_DIR / 'taxa.db')


# WIP
def load_table(csv_path: PathOrStr, db_path: PathOrStr, table_name: str = None):
    """Load a CSV file into a sqlite3 table.
    This is less efficient than the sqlite3 shell `.import` command, but easier to use.
    """
    csv_path = Path(csv_path).expanduser()
    db_path = Path(db_path).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    table_name = table_name or db_path.stem
    start = time()

    with sqlite3.connect(db_path) as conn, open(csv_path) as f:
        try:
            conn.execute(
                f'CREATE TABLE IF NOT EXISTS {table_name} '
                '(id INTEGER PRIMARY KEY, name TEXT, rank TEXT)'
            )
        except sqlite3.OperationalError:
            pass

        reader = DictReader(f)
        chunk = []
        chunksize = 2000
        for i, line in enumerate(reader):
            chunk.append((line['id'], line['scientificName'], line['taxonRank']))
            # Write chunk
            if (i + 1) % chunksize == 0:
                # print(f'Writing chunk: {chunksize}')
                conn.executemany(f'INSERT INTO {table_name} VALUES (?,?,?)', chunk)
                chunk = []
        # Final chunk
        if chunk:
            # print(f'Writing chunk: {len(chunk)}')
            conn.executemany(f'INSERT INTO {table_name} VALUES (?,?,?)', chunk)

        conn.commit()

    print(f'Completed in {time() - start:.2f}s')


class ChunkReader(DictReader):
    """A CSV reader that yields chunks of rows"""

    def __init__(self, fp, chunk_size: int = 2000, include_fields: List[str] = None, **kwargs):
        super().__init__(fp, **kwargs)
        self._chunk_size = chunk_size
        self._include_fields = include_fields

    def __next__(self):
        chunk = []
        try:
            for _ in range(self._chunk_size):
                line = super().__next__()
                if self._include_fields:
                    line = {k: v for k, v in line.items() if k in self._include_fields}
                self.chunk.append(line)
        except StopIteration:
            pass
        return list(chunk.values())


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
