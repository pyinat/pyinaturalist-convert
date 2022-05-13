"""Tools to build and search a Taxon full text search database"""
import sqlite3
from functools import partial
from logging import getLogger
from pathlib import Path
from typing import Dict, Iterable, List, Union

from pyinaturalist.models import Taxon

from .constants import DATA_DIR, OBS_DB, TAXON_COUNTS, TAXON_DB, PathOrStr
from .download import CSVProgress, get_progress_spinner
from .dwca import get_observation_taxon_counts
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
        db_path: PathOrStr = TAXON_DB,
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
        query = 'SELECT name, taxon_id, taxon_rank, count_rank, rank, (rank - count_rank) AS combined_rank FROM taxon_names '
        query += f"WHERE name MATCH '{q}*' AND (language_code IS NULL "
        query += f"OR language_code = '{language}') " if language else ' '
        query += f'ORDER BY combined_rank LIMIT {self.limit}'

        with self.connection as conn:
            results = sorted(conn.execute(query).fetchall(), key=lambda row: row['combined_rank'])
            # logger.debug('\n'.join([
            #     f"[{t['taxon_id']}] {t['name']}: {t['rank']}, {t['count_rank']}, {t['combined_rank']}"
            #         for t in results]))
            return [
                Taxon(id=int(row['taxon_id']), name=row['name'], rank=row['taxon_rank'])
                for row in results[: self.limit]
                # for row in conn.execute(query)
            ]


def load_taxonomy_text_search_tables(
    csv_dir: PathOrStr = DATA_DIR / 'inaturalist-taxonomy.dwca',
    db_path: PathOrStr = TAXON_DB,
    obs_db_path: PathOrStr = OBS_DB,
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
    common_name_csvs = get_common_name_csvs(csv_dir, languages)
    progress = CSVProgress(main_csv, *common_name_csvs.values())

    taxon_counts = load_taxon_counts(obs_db_path)
    transform = partial(add_taxon_counts, taxon_counts=taxon_counts)

    def load_fts5_table(csv_path, column_map):
        load_table(
            csv_path,
            db_path,
            'taxon_names',
            column_map,
            progress=progress,
            transform=transform,
        )

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


def create_fts5_table(db_path: PathOrStr = TAXON_DB):
    prefix_idxs = ', '.join([f'prefix={i}' for i in PREFIX_INDEXES])

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f'CREATE VIRTUAL TABLE IF NOT EXISTS taxon_names USING fts5( '
            '   name, taxon_id, taxon_rank UNINDEXED, count_rank UNINDEXED, language_code,'
            f'  {prefix_idxs})'
        )


def optimize_fts_table(db_path: PathOrStr = TAXON_DB):
    logger.info('Optimizing FTS table')
    progress = get_progress_spinner('Optimizing table')
    with progress, sqlite3.connect(db_path) as conn:
        conn.execute('UPDATE taxon_names SET language_code="zh_cn" WHERE language_code="zh-CN"')
        conn.execute("INSERT INTO taxon_names(taxon_names) VALUES('optimize')")
        conn.commit()
        conn.execute('VACUUM')
        conn.execute('ANALYZE taxon_names')


# TODO: Save to and read from taxa SQLite table instead of CSV
def load_taxon_counts(
    obs_db_path: PathOrStr = OBS_DB,
) -> Dict[int, int]:
    """Calculate normalized taxon counts (or load previously saved counts)"""
    taxon_counts = read_saved_taxon_counts()
    if not taxon_counts:
        taxon_counts = get_observation_taxon_counts(obs_db_path)
        taxon_counts = normalize_taxon_counts(taxon_counts)
    return taxon_counts


def add_taxon_counts(row: Dict[str, Union[int, str]], taxon_counts: Dict[int, int]):
    """Add taxon counts to a chunk of taxon records read from CSV"""
    taxon_id = int(row['id'])
    row['count_rank'] = taxon_counts.get(taxon_id, -1)
    return row


def read_saved_taxon_counts(csv_file: PathOrStr = TAXON_COUNTS) -> Dict[int, int]:
    """Read previously calculated taxon count ranks from a CSV file"""
    import pandas as pd

    if Path(csv_file).is_file():
        logger.info(f'Reading taxon count ranks from {csv_file}')
        df = pd.read_csv(csv_file, index_col='id')
        return df.set_index('taxon_id')['count_rank'].to_dict()
    return {}


def normalize_taxon_counts(
    taxon_counts: Dict[int, int], csv_file: PathOrStr = TAXON_COUNTS
) -> Dict[int, int]:
    """Normalize taxon counts to a distribution between -10 and 10"""
    import numpy as np
    import pandas as pd

    if not taxon_counts:
        return {}

    def normalize(series):
        with np.errstate(divide='ignore'):
            series = np.log(series.copy())
        series[np.isneginf(series)] = 0
        return (series - series.mean()) / series.std()

    logger.info('Normalizing taxon counts')
    df = pd.DataFrame(taxon_counts.items(), columns=['id', 'count'])
    df['count_rank'] = normalize(df['count'])
    df['count_rank'].fillna(0)
    df['count_rank'] = df['count_rank'] * TAXON_COUNT_RANK_FACTOR

    df = df.sort_values(by='count_rank', ascending=False)
    df.to_csv(csv_file, index=False)
    return df.set_index('taxon_id')['count_rank'].to_dict()
