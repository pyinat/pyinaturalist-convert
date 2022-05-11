"""Taxon full text search utilities"""
import sqlite3
from logging import getLogger
from pathlib import Path
from typing import Dict, Iterable, List

from pyinaturalist.models import Taxon
from rich.progress import track

from .constants import DATA_DIR, PathOrStr
from .download import CSVProgress, get_progress_spinner
from .sqlite import load_table

# Add extra text search prefix indexes to speed up searches for these prefix lengths
PREFIX_INDEXES = [2, 3, 4]

# Columns to use for text search table, and which should be indexes
TAXON_FTS_COLUMNS = {
    'name': True,
    'taxon_id': False,
    'taxon_rank': False,
    'count_rank': False,
    'language': False,
}

TAXON_COUNT_RANK_FACTOR = 5
TAXON_NAME_MAP = {'scientificName': 'name', 'id': 'taxon_id', 'taxonRank': 'taxon_rank'}
COMMON_TAXON_NAME_MAP = {'vernacularName': 'name', 'id': 'taxon_id', 'language': 'language_code'}
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

    def search(self, q: str, language: str = 'en') -> List[Taxon]:
        """Search for taxa by scientific and/or common name.

        Args:
            q: Search query
            language: Language code for common names

        Returns:
            Taxon objects (with ID and name only)
        """
        language = (language or '').lower().replace('-', '_')
        query = 'SELECT name, taxon_id, taxon_rank, count_rank, rank FROM taxon_names '
        query += f"WHERE name MATCH '{q}*' AND (language_code IS NULL "
        query += f"OR language_code = '{language}') " if language else ' '
        query += f'ORDER BY rank LIMIT {self.limit}'

        with self.connection as conn:
            return [
                Taxon(id=int(row['taxon_id']), name=row['name'], rank=row['taxon_rank'])
                for row in conn.execute(query)
            ]


def load_taxonomy_text_search_tables(
    csv_dir: PathOrStr = DATA_DIR / 'inaturalist-taxonomy.dwca',
    db_path: PathOrStr = DATA_DIR / 'taxa.db',
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

    def load_fts5_table(csv_path, column_map):
        load_table(csv_path, db_path, 'taxon_names', column_map, progress=progress)

    with progress:
        logger.info(
            f'Loading taxon scientific names + common names for {len(common_name_csvs)} languages:'
            ', '.join(common_name_csvs.keys())
        )
        create_fts5_table(db_path)

        for lang, csv_file in common_name_csvs.items():
            lang = lang.lower().replace('-', '_')
            load_fts5_table(csv_file, COMMON_TAXON_NAME_MAP)
        load_fts5_table(main_csv, TAXON_NAME_MAP)

    load_taxon_counts()


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


def create_fts5_table(db_path: PathOrStr):
    prefix_idxs = ', '.join([f'prefix={i}' for i in PREFIX_INDEXES])

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f'CREATE VIRTUAL TABLE IF NOT EXISTS taxon_names USING fts5( '
            '   name, taxon_id UNINDEXED, taxon_rank UNINDEXED, count_rank UNINDEXED, language_code,'
            f'  {prefix_idxs})'
        )


def load_taxon_counts(
    taxon_db_path: PathOrStr = DATA_DIR / 'taxa.db',
    obs_db_path: PathOrStr = DATA_DIR / 'observations.db',
):
    """Calculate normalized taxon counts, and add to taxon text search table"""
    taxon_counts = _read_taxon_counts()
    if not taxon_counts:
        taxon_counts = _get_taxon_counts(obs_db_path)
        taxon_counts = _normalize_taxon_counts(taxon_counts)

    _load_taxon_counts(taxon_db_path, taxon_counts)
    _optimize_fts_table(taxon_db_path)


def _get_taxon_counts(db_path: PathOrStr) -> Dict[int, int]:
    """Get taxon counts based on GBIF export"""
    if not Path(db_path).is_file():
        logger.warning(f'Observation database {db_path} not found')
        return {}

    logger.info(f'Getting taxon counts from {db_path}')
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT taxonID, COUNT(*) AS count FROM observations GROUP BY taxonID;"
        ).fetchall()

        return {
            int(row['taxonID']): int(row['count'])
            for row in sorted(rows, key=lambda r: r['count'], reverse=True)
        }


def _read_taxon_counts(csv_file: Path = DATA_DIR / 'taxon_counts.csv') -> Dict[int, int]:
    """Read previously calculated taxon counts from a CSV file"""
    import pandas as pd

    if csv_file.is_file():
        logger.info(f'Reading taxon counts from {csv_file}')
        df = pd.read_csv(csv_file)
        return df.set_index('taxon_id')['count_rank'].to_dict()
    return {}


def _normalize_taxon_counts(taxon_counts: Dict[int, int]) -> Dict[int, int]:
    """Normalize taxon counts to a distribution between -10 and 10"""
    import numpy as np
    import pandas as pd

    # Alternative to get_taxon_counts():
    # sqlite3 "SELECT..." > taxon_counts_raw.csv
    # df = pd.read_csv('taxon_counts_raw.csv', delimiter='|')
    # df = df[df['count'] > 1]
    # df = df.sort_values(by=['count'], ascending=False)

    def normalize(series):
        with np.errstate(divide='ignore'):
            series = np.log(series.copy())
        series[np.isneginf(series)] = 0
        return (series - series.mean()) / series.std()

    logger.info('Normalizing taxon counts')
    df = pd.DataFrame(taxon_counts.items(), columns=['taxon_id', 'count'])
    df['count_rank'] = normalize(df['count'])
    df['count_rank'].fillna(0)
    df['count_rank'] = df['count_rank'] * TAXON_COUNT_RANK_FACTOR

    df = df.sort_values(by='count_rank', ascending=False)
    df.to_csv(DATA_DIR / 'taxon_counts.csv', index=False)
    return df.set_index('taxon_id')['count_rank'].to_dict()


def _load_taxon_counts(db_path: PathOrStr, taxon_counts: Dict[int, int]):
    """Update text search table with normalized taxon counts"""
    logger.info(f'Loading taxon counts into {db_path}')
    with sqlite3.connect(db_path) as conn:
        for taxon_id, count_rank in track(taxon_counts.items(), description='Loading taxon counts'):
            conn.execute(
                'UPDATE taxon_names SET count_rank=? WHERE taxon_id=?', (count_rank, taxon_id)
            )
        conn.commit()


def _optimize_fts_table(db_path: PathOrStr):
    logger.info('Optimizing FTS table')
    progress = get_progress_spinner('Optimizing table')
    with progress, sqlite3.connect(db_path) as conn:
        conn.execute('UPDATE taxon_names SET count_rank=-10 WHERE count_rank IS NULL')
        conn.execute('UPDATE taxon_names SET language_code="zh_cn" WHERE language_code="zh-CN"')
        conn.execute("INSERT INTO taxon_names(taxon_names) VALUES('optimize')")
        conn.commit()
        conn.execute('VACUUM')
        conn.execute('ANALYZE taxon_names')
