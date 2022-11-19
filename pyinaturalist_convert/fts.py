"""Build and search a taxonomy full text search database using
`FTS5 <https://www.sqlite.org/fts5.html>`_. This works similarly to the API endpoint
:py:func:`~pyinaturalist.v1.taxa.get_taxa_autocomplete`, which powers the taxon autocomplete feature
on inaturalist.org:

.. image::
    ../images/inat-taxon-autocomplete.png

**Extra dependencies**: ``sqlalchemy`` (only for building the database; not required for searches)

**Build Example**::

    >>> from pyinaturalist_convert import (
    ...     aggregate_taxon_db, enable_logging, load_dwca_tables, load_fts_taxa
    ... )

    >>> # Optional, but recommended:
    >>> enable_logging()
    >>> load_dwca_tables()
    >>> aggregate_taxon_db()

    >>> # Load FTS table for all languages (Defaults to English names only):
    >>> load_fts_taxa(language='all')

.. note::
    Running :py:func:`.aggregate_taxon_db` will result in more accurate search rankings based
    on taxon counts, but will take a couple hours to complete.

**Search example**::

    >>> from pyinaturalist_convert import TaxonAutocompleter

    >>> ta = TaxonAutocompleter()

    >>> # Search by scientific name
    >>> ta.search('aves')
    [
        Taxon(id=3, name='Aves'),
        Taxon(id=1043988, name='Avesicaria'),
        ...,
    ]

    >>> # Or by common name
    >>> ta.search('frill')
    [
        Taxon(id=56447, name='Acid Frillwort'),
        Taxon(id=614339, name='Antilles Frillfin'),
        ...,
    ]

    >>> # Or by common name in a specific language
    >>> ta.search('flughund', language='german')

**Main classes & functions:**

.. autosummary::
    :nosignatures:

    TaxonAutocompleter
    load_fts_taxa

"""
import sqlite3
from functools import partial
from logging import getLogger
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Union

from pyinaturalist import Observation
from pyinaturalist.models import Taxon

from .constants import DB_PATH, DWCA_TAXON_CSV_DIR, TAXON_COUNTS, PathOrStr
from .download import CSVProgress, get_progress_spinner
from .sqlite import load_table, vacuum_analyze

# Add extra text search prefix indexes to speed up searches for these prefix lengths
PREFIX_INDEXES = [2, 3, 4]

OBS_FTS_TABLE = 'observation_fts'
TAXON_FTS_TABLE = 'taxon_fts'

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

    Args:
        db_path: Path to SQLite database; uses platform-specific data directory by default
        limit: Maximum number of results to return per query
    """

    def __init__(self, db_path: PathOrStr = DB_PATH, limit: int = 10):
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row
        self.limit = limit

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
        query += "WHERE name MATCH ? || '*' AND (language_code IS NULL "
        query += "OR language_code = ?) " if language else ' '
        query += 'ORDER BY combined_rank LIMIT ?'

        with self.connection as conn:
            cursor = conn.execute(query, (q, language, self.limit))
            results = sorted(
                cursor.fetchall(),
                key=lambda row: row['combined_rank'],
            )
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
            (from :py:func:`.aggregate_taxon_db`)
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
        create_taxon_fts_table(db_path)

        for lang, csv_file in common_name_csvs.items():
            lang = lang.lower().replace('-', '_')
            load_fts_table(csv_file, COMMON_TAXON_NAME_MAP)
        load_fts_table(main_csv, TAXON_NAME_MAP)

    _load_taxon_ranks(db_path)
    optimize_fts_table(TAXON_FTS_TABLE, db_path)


def get_common_name_csvs(csv_dir: Path, languages: Iterable[str] = None) -> Dict[str, Path]:
    """Get common name CSVs, for either all or some languages, with a separate table per language"""
    if languages and languages != 'all':
        common_name_csvs = {lang: csv_dir / f'VernacularNames-{lang}.csv' for lang in languages}
        return {lang: csv_path for lang, csv_path in common_name_csvs.items() if csv_path.exists()}
    else:
        return {
            path.stem.replace('VernacularNames-', ''): path
            for path in csv_dir.glob('VernacularNames-*.csv')
        }


def create_taxon_fts_table(db_path: PathOrStr = DB_PATH):
    prefix_idxs = ', '.join([f'prefix={i}' for i in PREFIX_INDEXES])

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f'CREATE VIRTUAL TABLE IF NOT EXISTS {TAXON_FTS_TABLE} USING fts5( '
            '   name, taxon_id, taxon_rank UNINDEXED, count_rank UNINDEXED, language_code,'
            f'  {prefix_idxs})'
        )


def optimize_fts_table(table: str, db_path: PathOrStr = DB_PATH):
    """Some final cleanup after loading a text search table"""
    logger.info(f'Optimizing FTS table {table}')
    progress = get_progress_spinner('Optimizing table')
    with progress, sqlite3.connect(db_path) as conn:
        conn.execute(f"INSERT INTO {table}({table}) VALUES('optimize')")
        conn.commit()
    vacuum_analyze([table], db_path, show_spinner=True)


def _load_taxon_ranks(db_path: PathOrStr = DB_PATH):
    """Set taxon ranks for common name results. Attempt to get from full taxa table, which
    will be much faster than using text search table.
    """
    logger.info('Loading taxon ranks')
    progress = get_progress_spinner('Loading taxon ranks for common names')
    with progress, sqlite3.connect(db_path) as conn:
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


# TODO: Read from taxon table instead
def normalize_taxon_counts(counts_path: PathOrStr = TAXON_COUNTS) -> Dict[int, int]:
    """Read previously calculated taxon counts, and normalize to a logarithmic distribution"""
    import numpy as np
    import pandas as pd

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
    df['count_rank'] = normalize(df['observations_count']).fillna(-1)
    df['count_rank'] = df['count_rank'] * TAXON_COUNT_RANK_FACTOR
    df = df.sort_values(by='count_rank', ascending=False)
    return df['count_rank'].to_dict()


# TODO: Pre-populate with any existing observations in db?
# TODO: Should rows be added via trigger on observation table, or in save_observations()?
def load_fts_observations(db_path: PathOrStr = DB_PATH):
    """Create full text search table for observations (descriptions, comments, and identifications).
    Requires SQLite FTS5 extension.

    Args:
        db_path: Path to SQLite database
    """
    create_observation_fts_table(db_path)
    optimize_fts_table(OBS_FTS_TABLE, db_path)


def create_observation_fts_table(db_path: PathOrStr = DB_PATH):
    prefix_idxs = ', '.join([f'prefix={i}' for i in PREFIX_INDEXES])

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f'CREATE VIRTUAL TABLE IF NOT EXISTS {OBS_FTS_TABLE} USING fts5( '
            '   text, observation_id,'
            f'  {prefix_idxs})'
        )


def index_obs_text(obs: Observation, db_path: PathOrStr = DB_PATH):
    """Index observation text (descriptions, comments, and identification comments) in FTS table"""
    if not obs:
        return

    texts = [obs.description]
    texts.extend([c.body for c in obs.comments])
    texts.extend([i.body for i in obs.identifications])
    texts = [t for t in texts if t]

    with sqlite3.connect(db_path) as conn:
        for text in texts:
            conn.execute(
                f'INSERT INTO {OBS_FTS_TABLE} (text, observation_id) VALUES (?, ?)',
                (text, obs.id),
            )
        conn.commit()


# TODO: Add observation short description (what/where/when) to FTS table?
# TODO: Add iconic taxon ID to display emjoi in search results?
# TODO: Filter by (description or comment)?
class ObservationAutocompleter:
    """Observation autocomplete search.

    Args:
        db_path: Path to SQLite database; uses platform-specific data directory by default
        limit: Maximum number of results to return per query
        truncate_match_chars: Truncate matched text to this many characters. Set to -1 to disable.
    """

    def __init__(
        self, db_path: PathOrStr = DB_PATH, limit: int = 10, truncate_match_chars: int = 50
    ):
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row
        self.limit = limit
        self.truncate_match_chars = truncate_match_chars

    def search(self, q: str) -> List[Tuple[int, str]]:
        """Search for taxa by scientific and/or common name.

        Args:
            q: Search query

        Returns:
            Tuples of ``(observation_id, truncated_text)``
        """
        if not q:
            return []

        query = f'SELECT *, rank FROM {OBS_FTS_TABLE} '
        query += "WHERE text MATCH ? "
        query += 'ORDER BY rank LIMIT ?'

        with self.connection as conn:
            cursor = conn.execute(query, (q, self.limit))
            results = sorted(cursor.fetchall(), key=lambda row: row['rank'])
            return [(row['observation_id'], self._truncate(row['text'], q)) for row in results]

    def _truncate(self, text: str, q: str) -> str:
        """Truncate matched text to a maximum number of characters"""
        if self.truncate_match_chars == -1 or len(text) <= self.truncate_match_chars:
            return text

        # Determine if match is at beginning, middle, or end of text
        idx = text.lower().find(q.lower())
        truncated_text = ''
        truncate_chars = self.truncate_match_chars
        if idx > 0:
            truncated_text += '...'
            truncate_chars -= 3

        truncated_text += text[idx : idx + truncate_chars]
        if idx + truncate_chars < len(text):
            truncated_text = truncated_text[:-3] + '...'
        return truncated_text
