"""Tools to build and search a taxon full text search database.

**Extra dependencies**: ``sqlalchemy`` (for inital load only, not searching)

**Example**::

    >>> from pyinaturalist_convert import aggregate_taxon_counts, load_dwca_tables, load_taxon_fts_table
    >>>
    >>> load_dwca_tables()
    >>> aggregate_taxon_counts()  # Optional, but recommended
    >>> load_fts_taxa()

Note: This process will take several hours.

.. automodsumm:: pyinaturalist_convert.fts
   :classes-only:
   :nosignatures:

.. automodsumm:: pyinaturalist_convert.fts
   :functions-only:
   :nosignatures:
"""
# TODO: Add scientific names to common name records (unindexed, just for display purposes)
import sqlite3
from functools import partial
from logging import getLogger
from pathlib import Path
from typing import Dict, Iterable, List, Union

import numpy as np
import pandas as pd
from pyinaturalist.models import Taxon

from .constants import DB_PATH, DWCA_TAXON_CSV_DIR, TAXON_COUNTS, PathOrStr
from .download import CSVProgress, get_progress_spinner
from .sqlite import load_table, vacuum_analyze

# Add extra text search prefix indexes to speed up searches for these prefix lengths
PREFIX_INDEXES = [2, 3, 4]

# Columns to use for text search table, and which should be indexed
TAXON_FTS_TABLE = 'taxon_fts'
TAXON_FTS_COLUMNS = {
    'name': True,
    'taxon_id': False,
    'taxon_rank': False,
    'count_rank': False,
    'language': False,
}

TAXON_COUNT_RANK_FACTOR = 2.5
TAXON_NAME_MAP = {
    'scientificName': 'name',
    'id': 'taxon_id',
    'taxonRank': 'taxon_rank',
    'count_rank': 'count_rank',
}
COMMON_TAXON_NAME_MAP = {
    'vernacularName': 'name',
    'id': 'taxon_id',
    'language': 'language_code',
    'count_rank': 'count_rank',
}
# Other available fields:
# 'language',
# 'locality',
# 'countryCode',
# 'source',
# 'lexicon',
# 'contributor',
# 'created',

logger = getLogger(__name__)


# TODO: Deduplicate results (if both common and scientific names are present)
class TaxonAutocompleter:
    """Taxon autocomplete search.

    Example:
        >>> from pyinaturalist_convert import TaxonAutocompleter
        >>>
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
        limit: Maximum number of results to return per query
    """

    def __init__(self, db_path: PathOrStr = DB_PATH, limit: int = 10):
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
        if not q:
            return []

        language = (language or '').lower().replace('-', '_')
        query = f'SELECT *, rank, (rank - count_rank) AS combined_rank FROM {TAXON_FTS_TABLE} '
        query += f"WHERE name MATCH '{q}*' AND (language_code IS NULL "
        query += f"OR language_code = '{language}') " if language else ' '
        query += f'ORDER BY combined_rank LIMIT {self.limit}'

        with self.connection as conn:
            results = sorted(conn.execute(query).fetchall(), key=lambda row: row['combined_rank'])
            return [
                Taxon(id=int(row['taxon_id']), name=row['name'], rank=row['taxon_rank'])
                for row in results
            ]


def load_fts_taxa(
    csv_dir: PathOrStr = DWCA_TAXON_CSV_DIR,
    db_path: PathOrStr = DB_PATH,
    counts_path: PathOrStr = TAXON_COUNTS,
    languages: Iterable[str] = ('english',),
):
    """Create full text search tables for taxonomic names.
    Requires SQLite FTS5 extension and the iNat taxonomy DwC-A archive.

    Args:
        csv_dir: Directory containing extracted CSV files
        db_path: Path to SQLite database
        counts_path: Path to previously calculated taxon counts
            (from :py:func:`.aggregate_taxon_counts`)
        lanugages: List of common name languages to load, or 'all' to load everything
    """
    csv_dir = Path(csv_dir).expanduser()
    main_csv = csv_dir / 'taxa.csv'
    common_name_csvs = get_common_name_csvs(csv_dir, languages)
    progress = CSVProgress(main_csv, *common_name_csvs.values())

    taxon_counts = normalize_taxon_counts(counts_path)
    transform = partial(add_taxon_counts, taxon_counts=taxon_counts)

    def load_fts_table(csv_path, column_map):
        load_table(
            csv_path,
            db_path,
            TAXON_FTS_TABLE,
            column_map,
            progress=progress,
            transform=transform,
        )

    with progress:
        logger.info(
            f'Loading taxon scientific names + common names for {len(common_name_csvs)} languages:'
            + ', '.join(common_name_csvs.keys())
        )
        create_fts5_table(db_path)

        for lang, csv_file in common_name_csvs.items():
            lang = lang.lower().replace('-', '_')
            load_fts_table(csv_file, COMMON_TAXON_NAME_MAP)
        load_fts_table(main_csv, TAXON_NAME_MAP)

    optimize_fts_table(db_path)


def get_common_name_csvs(csv_dir: Path, languages: Iterable[str] = None) -> Dict[str, Path]:
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


def create_fts5_table(db_path: PathOrStr = DB_PATH):
    prefix_idxs = ', '.join([f'prefix={i}' for i in PREFIX_INDEXES])

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f'CREATE VIRTUAL TABLE IF NOT EXISTS {TAXON_FTS_TABLE} USING fts5( '
            '   name, taxon_id, taxon_rank UNINDEXED, count_rank UNINDEXED, language_code,'
            f'  {prefix_idxs})'
        )


def optimize_fts_table(db_path: PathOrStr = DB_PATH):
    """Some final cleanup after loading text search tables"""
    logger.info('Optimizing FTS table')
    progress = get_progress_spinner('Optimizing table')
    with progress, sqlite3.connect(db_path) as conn:
        _load_taxon_ranks(conn)
        conn.execute(f"INSERT INTO {TAXON_FTS_TABLE}({TAXON_FTS_TABLE}) VALUES('optimize')")
        conn.commit()
    vacuum_analyze([TAXON_FTS_TABLE], db_path)


def _load_taxon_ranks(conn):
    """Set taxon ranks for common name results. Attempt to get from full taxa table, which
    will be much faster than using text search table.
    """
    try:
        conn.execute(
            f'UPDATE {TAXON_FTS_TABLE} SET taxon_rank = '
            f'(SELECT t2.rank from taxon t2 WHERE t2.id = {TAXON_FTS_TABLE}.taxon_id) '
            'WHERE taxon_rank IS NULL'
        )
    except sqlite3.OperationalError:
        logger.warning('Full taxon table not found; ranks not loaded for common names')


def add_taxon_counts(row: Dict[str, Union[int, str]], taxon_counts: Dict[int, int]):
    """Add taxon counts to a chunk of taxon records read from CSV"""
    taxon_id = int(row['id'])
    row['count_rank'] = taxon_counts.get(taxon_id, -1)
    if row.get('language_code'):
        row['language_code'] = str(row['language_code']).lower().replace('-', '_')
    return row


def normalize_taxon_counts(counts_path: PathOrStr = TAXON_COUNTS) -> Dict[int, int]:
    """Read previously calculated taxon counts, and normalize to a logarithmic distribution"""
    if not Path(counts_path).is_file():
        logger.warning(f'Taxon counts file not found: {counts_path}')
        return {}

    logger.info(f'Reading taxon counts from {counts_path}')
    df = pd.read_parquet(counts_path)

    def normalize(series):
        with np.errstate(divide='ignore'):
            series = np.log(series.copy())
        series[np.isneginf(series)] = 0
        return (series - series.mean()) / series.std()

    logger.info('Normalizing taxon counts')
    df['count_rank'] = normalize(df['count']).fillna(-1)
    df['count_rank'] = df['count_rank'] * TAXON_COUNT_RANK_FACTOR
    df = df.sort_values(by='count_rank', ascending=False)
    return df['count_rank'].to_dict()
