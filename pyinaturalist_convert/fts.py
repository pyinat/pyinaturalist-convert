"""Taxon full text search utilities"""
import sqlite3
from logging import getLogger
from pathlib import Path
from typing import Dict, Iterable, List

from pyinaturalist.models import Taxon

from .constants import DATA_DIR, PathOrStr
from .download import CSVProgress
from .sqlite import load_table

# Add extra text search prefix indexes to speed up searches for these prefix lengths
PREFIX_INDEXES = [2, 3, 4, 5]

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
            language = language.lower().replace('-', '_')
            query += 'UNION ' + base_query.format(f'{self.base_table_name}_{language}')

        # Order by hidden FTS5 column 'rank'
        query += f' ORDER BY rank LIMIT {self.limit}'
        with self.connection as conn:
            return [Taxon(id=int(row['taxon_id']), name=row['name']) for row in conn.execute(query)]


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

    def load_fts5_table(csv_path, table_name, column_map):
        create_fts5_table(db_path, table_name, column_map)
        load_table(csv_path, db_path, table_name, column_map, progress=progress)

    with progress:
        logger.info(
            f'Loading taxon scientific names + common names for {len(common_name_csvs)} languages:'
            ', '.join(common_name_csvs.keys())
        )
        for lang, csv_file in common_name_csvs.items():
            lang = lang.lower().replace('-', '_')
            load_fts5_table(csv_file, f'{base_table_name}_{lang}', COMMON_TAXON_NAME_MAP)
        load_fts5_table(main_csv, base_table_name, TAXON_NAME_MAP)


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


def create_fts5_table(db_path, table_name, column_map, pk='name'):
    # For text search, the "pk" will be the indexed text column; all others are unindexed
    non_pk_cols = [k for k in column_map.values() if k != pk]
    table_cols = [pk] + [f'{k} UNINDEXED' for k in non_pk_cols]
    prefix_idxs = [f'prefix={i}' for i in PREFIX_INDEXES]

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f'CREATE VIRTUAL TABLE IF NOT EXISTS {table_name} '
            f'USING fts5({", ".join(table_cols + prefix_idxs)});'
        )
