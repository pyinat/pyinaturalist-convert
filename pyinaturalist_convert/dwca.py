"""Utilities for working with the iNat GBIF DwC archive"""
import sqlite3
from logging import getLogger
from os.path import basename, splitext
from pathlib import Path
from time import time
from typing import Iterable, List

from pyinaturalist.models import Taxon

from .constants import DATA_DIR, DWCA_DIR, DWCA_TAXA_URL, DWCA_URL, PathOrStr
from .download import check_download, download_file, unzip_progress
from .sqlite import load_table

logger = getLogger(__name__)


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


TAXON_COLUMN_MAP = {
    'id': 'id',
    'scientificName': 'name',
    'taxonRank': 'rank',
    # 'kingdom': 'kingdom',
    # 'phylum': 'phylum',
    # 'class': 'class',
    # 'order': 'order',
    # 'family': 'family',
    # 'genus': 'genus',
    # 'specificEpithet': 'species',
    # 'infraspecificEpithet': 'infraspecies',
    # 'taxonID',
    # 'identifier',
    # 'parentNameUsageID',
    # 'modified',
    # 'references',
}

TAXON_NAME_MAP = {'scientificName': 'name', 'id': 'taxon_id'}
COMMON_TAXON_NAME_MAP = {
    'vernacularName': 'common_name',
    'id': 'taxon_id',
    # 'language': 'language',
    # 'locality': 'locality',
    # 'countryCode': 'country_code',
    # 'source': 'source',
    # 'lexicon': 'lexicon',
    # 'contributor': 'contributor',
    # 'created': 'created',
}


def load_taxonomy_table():
    load_table(
        DATA_DIR / 'inaturalist-taxonomy.dwca' / 'taxa.csv',
        DATA_DIR / 'taxa.db',
        table_name='taxa',
        column_map=TAXON_COLUMN_MAP,
    )


def load_taxonomy_text_search_tables(
    csv_dir: PathOrStr = DATA_DIR / 'inaturalist-taxonomy.dwca',
    db_path: PathOrStr = DATA_DIR / 'taxa.db',
    base_table_name: str = 'taxon_names',
    languages: Iterable[str] = ('english',),
):
    """Create full text search tables from the iNat taxonomy DwC-A archive.
    Requires SQLite FTS5 extension.

    Args:
        csv_dir: Directory containing extracted CSV files
        db_path: Path to SQLite database; use `:memory:` to create an in-memory database
        base_table_name: Base table name for the text search table(s)
        lanugages: List of languages for which common names will be loaded
    """
    csv_dir = Path(csv_dir).expanduser()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f'CREATE VIRTUAL TABLE IF NOT EXISTS {base_table_name} USING fts5(name, taxon_id UNINDEXED);'
        )

    # Load scientific names
    load_table(
        csv_dir / 'taxa.csv',
        db_path,
        table_name=base_table_name,
        column_map=TAXON_NAME_MAP,
    )

    # Load common names, with a separate table per locale
    if languages:
        common_name_csvs = {lang: csv_dir / f'VernacularNames-{lang}.csv' for lang in languages}
        common_name_csvs = {
            locale: csv_path for locale, csv_path in common_name_csvs.items() if csv_path.exists()
        }
    else:
        common_name_csvs = {
            path.stem.replace('VernacularNames-', ''): path
            for path in csv_dir.glob('VernacularNames-*.csv')
        }

    logger.info(
        f'Loading common names for {len(common_name_csvs)} languages:'
        ', '.join(common_name_csvs.keys())
    )
    for lang, csv_file in common_name_csvs.items():
        table_name = f'{base_table_name}_{lang}'.replace('-', '_')
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                f'CREATE VIRTUAL TABLE IF NOT EXISTS {table_name} USING fts5(name, taxon_id UNINDEXED);'
            )
        load_table(
            csv_file,
            db_path,
            table_name=table_name,
            column_map=COMMON_TAXON_NAME_MAP,
        )


class TaxonAutocompleter:
    def __init__(
        self,
        db_path: PathOrStr = DATA_DIR / 'taxa.db',
        base_table_name: str = 'taxon_names',
        limit: int = 10,
    ):
        self.base_table_name = base_table_name
        self.limit = limit
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row

    def search(self, q: str, language: str = 'english') -> List[Taxon]:
        """Search for taxa by scientific and/or common name.

        Returns:
            ``{taxon_id: name}``
        """
        query = f"SELECT * FROM {self.base_table_name} WHERE name MATCH '{q}*' "
        if language:
            query += (
                f"UNION SELECT * FROM {self.base_table_name}_{language} WHERE name MATCH '{q}*' "
            )
        query += f' LIMIT {self.limit}'

        with self.connection as conn:
            return [Taxon(id=int(row['taxon_id']), name=row['name']) for row in conn.execute(query)]


def benchmark():
    iterations = 10000
    autocompleter = TaxonAutocompleter()
    start = time()

    for _ in range(iterations):
        autocompleter.search('berry', language=None)
    elapsed = time() - start

    logger.info(f'Total: {elapsed:.2f}s')
    logger.info(f'Avg per query: {(elapsed/iterations)*1000:2f}ms')
