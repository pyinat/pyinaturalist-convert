"""Utilities for working with the iNat GBIF DwC archive"""
import sqlite3
from logging import getLogger
from os.path import basename, splitext
from pathlib import Path
from typing import Dict, Iterable, List

from pyinaturalist.models import Taxon

from .constants import DATA_DIR, DWCA_DIR, DWCA_TAXA_URL, DWCA_URL, PathOrStr
from .download import CSVProgress, check_download, download_file, unzip_progress
from .sqlite import load_table

TAXON_COLUMN_MAP = {'id': 'id', 'scientificName': 'name', 'taxonRank': 'rank'}
# Other available fields:
# 'kingdom',
# 'phylum',
# 'class',
# 'order',
# 'family',
# 'genus',
# 'specificEpithet'
# 'infraspecificEpithet'
# 'taxonID',
# 'identifier',
# 'parentNameUsageID',
# 'modified',
# 'references',

TAXON_NAME_MAP = {'scientificName': 'name', 'id': 'taxon_id'}
COMMON_TAXON_NAME_MAP = {'vernacularName': 'name', 'id': 'taxon_id'}
# Other available fields:
# 'language',
# 'locality',
# 'countryCode',
# 'source',
# 'lexicon',
# 'contributor',
# 'created',

logger = getLogger(__name__)


class TaxonAutocompleter:
    """Taxon autocomplete search.

    Example:
        >>> from pyinaturalist_convert import (
        ...     TaxonAutocompleter,
        ...     download_taxa,
        ...     load_taxonomy_text_search_tables,
        ... )
        >>>
        >>> download_taxa()
        >>> load_taxonomy_text_search_tables()
        >>> ta = TaxonAutocompleter()
        >>> ta.search('aves')
        [
            Taxon(id=3, name='Aves'),
            Taxon(id=1043988, name='Avesicaria'),
            ...,
        ]
        >>> ta.search('frill')
        [
            Taxon(id=56447, name='Acid Frillwort'),
            Taxon(id=614339, name='Antilles Frillfin'),
            ...,
        ]

    Args:
        db_path: Path to SQLite database
        base_table_name: Base table name for the text search table(s)
        limit: Maximum number of results to return per query
    """

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
            Taxon objects (with ID and name only)
        """
        # Start with scientific name text search
        base_query = f"SELECT *, rank FROM {{}} WHERE name MATCH '{q}*' "
        query = base_query.format(self.base_table_name)

        # Union with common name results for specified language
        if language:
            lang_table = f'{self.base_table_name}_{language}'.replace('-', '_')
            query += 'UNION ' + base_query.format(lang_table)

        # Order by hidden FTS5 column 'rank'
        query += f' ORDER BY rank LIMIT {self.limit}'
        with self.connection as conn:
            return [Taxon(id=int(row['taxon_id']), name=row['name']) for row in conn.execute(query)]


def download_dwca(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the GBIF DwC-A export. Reuses local data if it already exists and is
    up to date.

    Example to load into a SQLite database (using the `sqlite3` shell, from bash):

    .. highlight:: bash

        export DATA_DIR="$HOME/.local/share/pyinaturalist"
        sqlite3 -csv $DATA_DIR/observations.db ".import $DATA_DIR/gbif-observations-dwca/observations.csv observations"

    Args:
        dest_dir: Alternative download directory
    """
    _download_archive(DWCA_URL, dest_dir)


def download_taxa(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the DwC-A taxonomy export. Reuses local data if it already exists and is
    up to date.

    Args:
        dest_dir: Alternative download directory
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


def load_taxonomy_table(
    csv_path: PathOrStr = DATA_DIR / 'inaturalist-taxonomy.dwca' / 'taxa.csv',
    db_path: PathOrStr = DATA_DIR / 'taxa.db',
    table_name: str = 'taxa',
    column_map: Dict = TAXON_COLUMN_MAP,
):
    """Create a taxonomy table from the GBIF DwC-A archive"""
    load_table(csv_path, db_path, table_name, column_map)


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
        db_path: Path to SQLite database
        base_table_name: Base table name for the text search table(s)
        lanugages: List of common name languages to load, or 'all' to load everything
    """
    csv_dir = Path(csv_dir).expanduser()
    main_csv = csv_dir / 'taxa.csv'
    common_name_csvs = _get_common_name_csvs(csv_dir, languages)
    progress = CSVProgress(main_csv, *common_name_csvs.values())

    def _load_fts5_table(csv_path, table_name, column_map):
        load_table(
            csv_path, db_path, table_name, column_map, pk='name', fts5=True, progress=progress
        )

    with progress:
        logger.info(
            f'Loading taxon scientific names + common names for {len(common_name_csvs)} languages:'
            ', '.join(common_name_csvs.keys())
        )
        for lang, csv_file in common_name_csvs.items():
            table_name = f'{base_table_name}_{lang}'.replace('-', '_')
            _load_fts5_table(csv_file, table_name, COMMON_TAXON_NAME_MAP)
        _load_fts5_table(main_csv, base_table_name, TAXON_NAME_MAP)


def _get_common_name_csvs(csv_dir: Path, languages: Iterable[str] = None) -> Dict[str, Path]:
    """Get common name CSVs, for either all or some languages, with a separate table per language"""
    if languages and languages != 'all':
        common_name_csvs = {lang: csv_dir / f'VernacularNames-{lang}.csv' for lang in languages}
        return {
            locale: csv_path for locale, csv_path in common_name_csvs.items() if csv_path.exists()
        }
    else:
        return {
            path.stem.replace('VernacularNames-', ''): path
            for path in csv_dir.glob('VernacularNames-*.csv')
        }
