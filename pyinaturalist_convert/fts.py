"""
Build and search a full text search tables for taxa and observations using
`FTS5 <https://www.sqlite.org/fts5.html>`_.

**Extra dependencies**: ``sqlalchemy`` (only for building the database; not required for searches)

Taxon Autocomplete
------------------

:py:class:`.TaxonAutocompleter` works similarly to the API endpoint
:py:func:`~pyinaturalist.v1.taxa.get_taxa_autocomplete`, which powers the taxon autocomplete
feature on inaturalist.org:

.. image::
    ../images/inat-taxon-autocomplete.png


Build database with all taxa from GBIF archive::

    >>> from pyinaturalist_convert import (
    ...     aggregate_taxon_db, enable_logging, load_dwca_tables, load_fts_taxa, vacuum_analyze
    ... )

    >>> # Optional, but recommended:
    >>> enable_logging()
    >>> load_dwca_tables()
    >>> aggregate_taxon_db()

    >>> # Load FTS table for all languages (Defaults to English names only):
    >>> load_fts_taxa(languages='all')

    >>> # Optional: VACUUM and analyze all new tables
    >>> vacuum_analyze(['observation', 'taxon', 'taxon_fts'], show_spinner=True, fast=True)

.. note::
    Running :py:func:`.aggregate_taxon_db` will result in more accurate search rankings based
    on taxon counts, but will take a couple hours to complete.

Search taxa::

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

    >>> # Or by common name in a specific language (by short code)
    >>> ta.search('flughund', language='de')

    >>> # Or with strict language filtering enabled
    >>> ta.search('flughund', language='de', strict_language=True)

    >>> # Filter by taxonomic rank
    >>> ta.search('aves', rank='species')

    >>> # Filter by taxon ID or ancestry (e.g., descendants of Aves, taxon_id=3)
    >>> ta.search('robin', taxon_id=3)

Observation Autocomplete
------------------------

:py:class:`.ObservationAutocompleter` adds additional observation search features not available in
the web UI.

Query all of your own observations::

    >>> from pyinaturalist import iNatClient

    >>> client = iNatClient()
    >>> observations = client.observations.search(user_id='my_username').all()

Create table and index observations::

    >>> from pyinaturalist_convert import (
    ...     create_observation_fts_table, create_observation_fts_triggers, index_observation_text
    ... )

    >>> create_observation_fts_table()
    >>> # Optionally create triggers to keep FTS table in sync automatically:
    >>> create_observation_fts_triggers()
    >>> index_observation_text(observations)

Search observations::

    >>> from pyinaturalist_convert import ObservationAutocompleter

    >>> obs_autocompleter = ObservationAutocompleter()
    >>> obs_autocompleter.search('test')
    [
        (12345, 'test description text'),
        (67890, 'test comment text'),
    ]

**Main classes and functions:**

.. autosummary::
    :nosignatures:

    ObservationAutocompleter
    TaxonAutocompleter
    TextField
    create_taxon_fts_table
    create_taxon_fts_triggers
    create_observation_fts_table
    create_observation_fts_triggers
    index_observation_text
    load_fts_taxa
"""

import re
import sqlite3
import unicodedata
from collections.abc import Iterable, Sequence
from enum import Enum
from functools import partial
from itertools import chain
from logging import Logger, getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from pyinaturalist import Observation
from pyinaturalist.models import Taxon

from .constants import DB_PATH, DWCA_TAXON_CSV_DIR, TAXON_AGGREGATES_PATH, ParamList, PathOrStr
from .download import CSVProgress, get_progress_spinner
from .sqlite import load_table

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

FTS5_SEPARATOR_CHARS = re.compile(r'[^\w\s]|_')

# Add extra text search prefix indexes to speed up searches for these prefix lengths
TAXON_PREFIX_INDEXES = [2, 3, 4]
OBS_PREFIX_INDEXES = [3, 4]

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


class TextField(Enum):
    DESCRIPTION = 1
    COMMENT = 2
    IDENTIFICATION = 3
    PLACE = 4


class BaseSearchStrategy:
    """Base interface for taxon search generation strategies."""

    def setup_connection(self, connection: sqlite3.Connection) -> None:
        """Perform any connection-level setup (e.g. registering custom SQL functions).

        Args:
            connection: SQLite database connection
        """
        pass

    def build_query(
        self,
        q: str,
        language: str,
        taxon_rank: str | None,
        taxon_id: int | None,
        limit: int,
        strict_language: bool = False,
    ) -> tuple[str, list[Any]]:
        """Build and return the SQL query string and parameter list.

        Args:
            q: Sanitized search query string
            language: Preferred language code for common names
            taxon_rank: Optional taxonomic rank filter (e.g., 'species')
            taxon_id: Optional ancestor taxon ID filter
            limit: Maximum number of results to return
            strict_language: Whether to enforce strict language filtering

        Returns:
            A tuple of ``(sql_query, parameters)``
        """
        raise NotImplementedError

    def parse_results(self, rows: list[sqlite3.Row]) -> list[Taxon]:
        """Parse raw SQL rows into Taxon model instances.

        Args:
            rows: Raw database rows from query execution

        Returns:
            List of parsed :py:class:`~pyinaturalist.models.Taxon` instances
        """
        raise NotImplementedError

    def _build_base_cte(
        self,
        q: str,
        language: str,
        taxon_rank: str | None = None,
        taxon_id: int | None = None,
        strict_language: bool = False,
        strip_diacritics_enabled: bool = False,
    ) -> tuple[str, list[Any]]:
        """Consolidated filtering and base CTE construction shared by all strategies."""
        tokens = q.split()
        fts_q = f'{"* ".join(tokens)}*'
        preferred_lang = language.lower().replace('-', '_')[:2] if language else 'en'

        where_clauses = ['fts.name MATCH ?']
        params: list[Any] = [fts_q]

        if language and strict_language:
            where_clauses.append("(fts.language_code IS NULL OR fts.language_code LIKE ? || '%')")
            params.append(preferred_lang)

        if taxon_rank:
            where_clauses.append('(taxon_rank = ?)')
            params += [taxon_rank.lower()]

        join_sql = ''
        if taxon_id is not None:
            join_sql = 'JOIN taxon t ON fts.taxon_id = t.id'
            where_clauses.append(
                "(t.id = ? OR t.ancestor_ids = ? OR t.ancestor_ids LIKE ? || ',%' "
                "OR t.ancestor_ids LIKE '%,' || ? OR t.ancestor_ids LIKE '%,' || ? || ',%')"
            )
            str_ancestor_id = str(taxon_id)
            params.extend(
                [taxon_id, str_ancestor_id, str_ancestor_id, str_ancestor_id, str_ancestor_id]
            )

        where_sql = ' AND '.join(where_clauses)

        name_expr = (
            "REPLACE(LOWER(strip_diacritics(fts.name)), '-', ' ')"
            if strip_diacritics_enabled
            else "REPLACE(LOWER(fts.name), '-', ' ')"
        )

        cte_sql = f"""
            SELECT
                fts.rowid AS rowid,
                fts.taxon_id,
                fts.name,
                fts.taxon_rank,
                fts.count_rank,
                fts.language_code,
                fts.`rank`,
                {name_expr} AS clean_name
            FROM {TAXON_FTS_TABLE} fts
            {join_sql}
            WHERE {where_sql}
        """
        return cte_sql, params


class TaxonSimpleSearchStrategy(BaseSearchStrategy):
    """Default prefix, rank and count-based search strategy.

    Better performing but only crudely approximates the Elasticsearch-based
    iNaturalist API search results from /v?/taxa/autocomplete

    For a slower but closer to the API approximation, use TaxonAdvancedSearchStrategy
    """

    def build_query(
        self,
        q: str,
        language: str,
        taxon_rank: str | None,
        taxon_id: int | None,
        limit: int,
        strict_language: bool = True,
    ) -> tuple[str, list[Any]]:
        """Build and return the SQL query string and parameter list for the simple strategy.

        Args:
            q: Sanitized search query string
            language: Preferred language code for common names
            taxon_rank: Optional taxonomic rank filter (e.g., 'species')
            taxon_id: Optional ancestor taxon ID filter
            limit: Maximum number of results to return
            strict_language: Whether to enforce strict language filtering

        Returns:
            A tuple of ``(sql_query, parameters)``
        """
        base_cte_sql, params = self._build_base_cte(
            q=q,
            language=language,
            taxon_rank=taxon_rank,
            taxon_id=taxon_id,
            strict_language=strict_language,
            strip_diacritics_enabled=False,
        )

        query = f"""
            WITH base AS ({base_cte_sql}),
            ranked_matches AS (
                SELECT *,
                    rank,
                    (rank - COALESCE(count_rank, -1)) AS combined_rank,
                    ROW_NUMBER() OVER (
                        PARTITION BY taxon_id
                        ORDER BY (rank - COALESCE(count_rank, -1)) ASC, rowid ASC
                    ) AS rn
                FROM base
            )
            SELECT taxon_id, name, taxon_rank, combined_rank
            FROM ranked_matches
            WHERE rn = 1
        """

        if limit > 0:
            query += ' ORDER BY combined_rank ASC LIMIT ?'
            params.append(limit)

        return query, params

    def parse_results(self, rows: list[sqlite3.Row]) -> list[Taxon]:
        """Parse raw SQL rows into Taxon model instances.

        Args:
            rows: Raw database rows from query execution

        Returns:
            List of parsed :py:class:`~pyinaturalist.models.Taxon` instances
        """
        sorted_rows = sorted(rows, key=lambda row: row['combined_rank'])
        return [
            Taxon(id=int(row['taxon_id']), name=row['name'], rank=row['taxon_rank'])
            for row in sorted_rows
        ]


class TaxonAdvancedSearchStrategy(BaseSearchStrategy):
    """Advanced CTE-based search strategy with diacritic stripping and multi-word matching.

    A closer approximation of the Elasticsearch-based iNaturalist API results
    from /v?/taxa/autocomplete, but slower than TaxonSimpleSearchStrategy.

    Args:
        exact_match_boost: Score boost for exact name matches
        sci_phrase_match_boost: Score boost for scientific name phrase matches
        sci_loose_match_boost: Score boost for scientific name loose matches
        loc_phrase_match_boost: Score boost for localized name phrase matches
        loc_loose_match_boost: Score boost for localized name loose matches
        phrase_match_boost: Score boost for general phrase matches
        loose_match_boost: Score boost for general loose matches
        popularity_rank_factor: Weight factor for observation count popularity rank
    """

    def __init__(
        self,
        exact_match_boost: float = -10000.0,
        sci_phrase_match_boost: float = -300.0,
        sci_loose_match_boost: float = -240.0,
        loc_phrase_match_boost: float = -280.0,
        loc_loose_match_boost: float = -270.0,
        phrase_match_boost: float = -20.0,
        loose_match_boost: float = -10.0,
        popularity_rank_factor: float = -15.0,
    ):
        self.exact_match_boost = exact_match_boost
        self.sci_phrase_match_boost = sci_phrase_match_boost
        self.sci_loose_match_boost = sci_loose_match_boost
        self.loc_phrase_match_boost = loc_phrase_match_boost
        self.loc_loose_match_boost = loc_loose_match_boost
        self.phrase_match_boost = phrase_match_boost
        self.loose_match_boost = loose_match_boost
        self.popularity_rank_factor = popularity_rank_factor

    def setup_connection(self, connection: sqlite3.Connection) -> None:
        """Perform connection-level setup including custom SQL functions.

        Args:
            connection: SQLite database connection
        """
        connection.create_function('strip_diacritics', 1, _strip_diacritics)

    def _build_scoring_sql(
        self, q_lower: str, tokens: list[str], language: str
    ) -> tuple[str, list[Any]]:
        def build_phrase_match_sql(tokens: list[str]):
            phrase_parts = [
                "(clean_name LIKE ? || ' ' || '%' OR clean_name LIKE ? || '-%' OR clean_name = ?)"
            ]
            phrase_params = [tokens[0], tokens[0], tokens[0]]
            for token in tokens[1:]:
                phrase_parts.append(
                    "(clean_name LIKE '% ' || ? || ' ' || '%' OR clean_name LIKE '% ' || ? || '-%')"
                )
                phrase_params.extend([token, token])
            return ' AND '.join(phrase_parts), phrase_params

        def build_loose_match_sql(tokens: list[str]):
            loose_parts = ["clean_name LIKE ? || '%'"]
            loose_params = [tokens[0]]
            for token in tokens[1:]:
                loose_parts.append(
                    "(clean_name LIKE '% ' || ? || '%' OR clean_name LIKE '%-' || ? || '%')"
                )
                loose_params.extend([token, token])
            return ' AND '.join(loose_parts), loose_params

        if len(tokens) > 1:
            phrase_sql, phrase_params = build_phrase_match_sql(tokens)
            loose_sql, loose_params = build_loose_match_sql(tokens)

            sql = f"""
                (
                    CASE
                        WHEN clean_name = ? THEN {self.exact_match_boost}
                        WHEN {phrase_sql} THEN
                            CASE
                                WHEN language_code IS NULL THEN {self.sci_phrase_match_boost}
                                WHEN language_code LIKE ? THEN {self.loc_phrase_match_boost}
                                ELSE {self.phrase_match_boost}
                            END
                        WHEN {loose_sql} THEN
                            CASE
                                WHEN language_code IS NULL THEN {self.sci_loose_match_boost}
                                WHEN language_code LIKE ? THEN {self.loc_loose_match_boost}
                                ELSE {self.loose_match_boost}
                            END
                        ELSE 0.0
                    END
                    + (COALESCE(count_rank, 0.0) * {self.popularity_rank_factor})
                    + `rank`
                )
            """
            params = [q_lower, *phrase_params, language, *loose_params, language]
        else:
            sql = f"""
                (
                    CASE
                        WHEN clean_name = ? THEN {self.exact_match_boost}
                        WHEN clean_name LIKE ? || '%' THEN
                            CASE
                                WHEN language_code IS NULL THEN {self.sci_loose_match_boost}
                                WHEN language_code LIKE ? THEN {self.loc_loose_match_boost}
                                ELSE {self.loose_match_boost}
                            END
                        ELSE 0.0
                    END
                    + (COALESCE(count_rank, 0.0) * {self.popularity_rank_factor})
                    + `rank`
                )
            """
            params = [q_lower, q_lower, language]

        return sql, params

    def build_query(
        self,
        q: str,
        language: str,
        taxon_rank: str | None,
        taxon_id: int | None,
        limit: int,
        strict_language: bool = False,
    ) -> tuple[str, list[Any]]:
        """Build and return the SQL query string and parameter list for the advanced strategy.

        Args:
            q: Sanitized search query string
            language: Preferred language code for common names
            taxon_rank: Optional taxonomic rank filter (e.g., 'species')
            taxon_id: Optional ancestor taxon ID filter
            limit: Maximum number of results to return
            strict_language: Whether to enforce strict language filtering

        Returns:
            A tuple of ``(sql_query, parameters)``
        """
        q_lower = _strip_diacritics(q.lower())
        preferred_lang = language.lower().replace('-', '_')[:2] if language else 'en'

        base_cte_sql, base_params = self._build_base_cte(
            q=q,
            language=language,
            taxon_rank=taxon_rank,
            taxon_id=taxon_id,
            strict_language=strict_language,
            strip_diacritics_enabled=True,
        )

        score_sql, strategy_params = self._build_scoring_sql(
            q_lower=q_lower,
            tokens=q.split(),
            language=preferred_lang,
        )

        params = base_params + strategy_params

        query = f"""
            WITH base AS ({base_cte_sql}),
            scored AS (
                SELECT
                    rowid,
                    taxon_id,
                    name,
                    taxon_rank,
                    count_rank,
                    language_code,
                    {score_sql} AS combined_rank
                FROM base
            ),
            ranked AS (
                SELECT
                    taxon_id,
                    name,
                    taxon_rank,
                    combined_rank,
                    ROW_NUMBER() OVER (
                        PARTITION BY taxon_id
                        ORDER BY combined_rank ASC, rowid ASC
                    ) AS rn
                FROM scored
            )
            SELECT
                taxon_id,
                name,
                taxon_rank,
                combined_rank
            FROM ranked
            WHERE rn = 1
            ORDER BY combined_rank ASC
        """

        if limit > 0:
            query += ' LIMIT ?'
            params.append(limit)

        return query, params

    def parse_results(self, rows: list[sqlite3.Row]) -> list[Taxon]:
        """Parse raw SQL rows into Taxon model instances.

        Args:
            rows: Raw database rows from query execution

        Returns:
            List of parsed :py:class:`~pyinaturalist.models.Taxon` instances
        """
        return [
            Taxon(
                id=int(row['taxon_id']),
                name=row['name'],
                rank=row['taxon_rank'],
            )
            for row in rows
        ]


class TaxonAutocompleter:
    """Taxon autocomplete search. Runs full text search on taxon scientific and common names.

    Args:
        db_path: Path to SQLite database; uses platform-specific data directory by default
        limit: Maximum number of results to return per query. Set to -1 to disable.
        strategy: Optional custom search strategy
    """

    def __init__(
        self,
        db_path: PathOrStr = DB_PATH,
        limit: int = 10,
        strategy: BaseSearchStrategy | None = None,
    ):
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.limit = limit
        self.strategy = strategy or TaxonSimpleSearchStrategy()
        self.strategy.setup_connection(self.connection)

    def search(
        self,
        q: str,
        language: str = 'en',
        rank: str | None = None,
        taxon_id: int | None = None,
        strict_language: bool = False,
    ) -> list[Taxon]:
        """Search for taxa by scientific and/or common name.

        Args:
            q: Search query string
            language: Language code for common names (default: 'en')
            rank: Optional taxonomic rank filter (e.g., 'species', 'genus')
            taxon_id: Optional ancestor taxon ID to restrict results to descendants of this taxon
            strict_language: If True, strictly filter results to the specified language

        Returns:
            List of matching :py:class:`~pyinaturalist.models.Taxon` instances
        """
        q = _sanitize_fts_query(q)
        if not q:
            return []

        query, params = self.strategy.build_query(
            q,
            language=language,
            taxon_rank=rank,
            taxon_id=taxon_id,
            limit=self.limit,
            strict_language=strict_language,
        )

        with self.connection as conn:
            cursor = conn.execute(query, params)
            return self.strategy.parse_results(cursor.fetchall())


# TODO: Add observation short description (what/where/when) to FTS table?
# TODO: Add iconic taxon ID to display emoji in search results?
class ObservationAutocompleter:
    """Observation autocomplete search. Runs full text search on observation descriptions, comments,
    identification comments, and place names.

    Args:
        db_path: Path to SQLite database; uses platform-specific data directory by default
        limit: Maximum number of results to return per query. Set to -1 to disable.
        truncate_match_chars: Truncate matched text to this many characters. Set to -1 to disable.
    """

    def __init__(
        self, db_path: PathOrStr = DB_PATH, limit: int = 10, truncate_match_chars: int = 50
    ):
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row
        self.limit = limit
        self.truncate_match_chars = truncate_match_chars

    def search(self, q: str, fields: Optional[list[TextField]] = None) -> list[tuple[int, str]]:
        """Search for observations by text.

        Args:
            q: Search query
            fields: Specific text fields to search (description, comment, identification, and/or
                place). If not specified, all fields will be searched.

        Returns:
            Tuples of ``(observation_id, truncated_matched_text)``
        """
        q = _sanitize_fts_query(q)
        if not q:
            return []

        query = f"SELECT *, rank FROM {OBS_FTS_TABLE} WHERE text MATCH ? || '*' "
        params: ParamList = [q]

        if fields:
            placeholders = ','.join('?' for _ in fields)
            query += f'AND field IN ({placeholders}) '
            params += [t.value for t in fields]
        if self.limit > 1:
            query += 'ORDER BY rank LIMIT ?'
            params += [self.limit]

        with self.connection as conn:
            cursor = conn.execute(query, params)
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


def load_fts_taxa(
    csv_dir: PathOrStr = DWCA_TAXON_CSV_DIR,
    db_path: PathOrStr = DB_PATH,
    agg_path: PathOrStr = TAXON_AGGREGATES_PATH,
    languages: Iterable[str] = ('english',),
):
    """Create full text search tables for taxonomic names.
    Requires SQLite FTS5 extension and the iNat taxonomy DwC-A archive.

    Args:
        csv_dir: Directory containing extracted CSV files
        db_path: Path to SQLite database
        agg_path: Path to previously calculated taxon counts (from :py:func:`.aggregate_taxon_db`)
        languages: List of common name languages to load, or 'all' to load everything
    """
    csv_dir = Path(csv_dir).expanduser()
    main_csv = csv_dir / 'taxa.csv'
    common_name_csvs = _get_common_name_csvs(csv_dir, languages)
    progress = CSVProgress(main_csv, *common_name_csvs.values())

    taxon_counts = _normalize_taxon_counts(agg_path)
    transform = partial(_add_taxon_counts, taxon_counts=taxon_counts)

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


def create_taxon_fts_table(db_path: PathOrStr = DB_PATH):
    """Create a SQLite FTS5 table for taxonomic names

    Args:
        db_path: Path to SQLite database
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute(f'DROP TABLE IF EXISTS {TAXON_FTS_TABLE}')
        conn.execute(_create_taxon_fts_table_sql())


def create_observation_fts_table(db_path: PathOrStr = DB_PATH):
    """Create a SQLite FTS5 table for observation text.

    There are two options for indexing observations:

    * Automatic (sync with ``observation`` table): :py:func:`create_observation_fts_triggers`
    * Manual (with ``Observation`` objects): :py:func:`index_observation_text`

    Args:
        db_path: Path to SQLite database
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute(_create_observation_fts_table_sql())


def create_observation_fts_triggers(db_path: PathOrStr = DB_PATH):
    """Create SQLite triggers to keep observation_fts in sync with the observation table.
    Requires both the observation table and observation_fts table to already exist.

    Args:
        db_path: Path to SQLite database
    """
    with sqlite3.connect(db_path) as conn:
        for sql in _create_observation_fts_trigger_sql().values():
            conn.execute(sql)


def create_taxon_fts_triggers(db_path: PathOrStr = DB_PATH):
    """Create SQLite triggers to keep taxon_fts in sync with the taxon table.
    Requires both the taxon table and taxon_fts table to already exist.

    Args:
        db_path: Path to SQLite database
    """
    with sqlite3.connect(db_path) as conn:
        for sql in _create_taxon_fts_trigger_sql().values():
            conn.execute(sql)


def _upgrade_fts_sync(
    bind: 'Connection',
    fts_table: str,
    table_sql: str,
    triggers: dict[str, str],
    logger: Logger,
):
    """Create an FTS table (if it doesn't already exist) and its sync triggers.
    Used by alembic migrations; only supports SQLite.
    """
    import sqlalchemy as sa

    existing_tables = set(sa.inspect(bind).get_table_names())
    if fts_table in existing_tables:
        logger.info("'%s' already exists; ensuring triggers exist", fts_table)
    else:
        bind.exec_driver_sql(table_sql)
        logger.info("Created '%s' table", fts_table)

    for name, sql in triggers.items():
        bind.exec_driver_sql(sql)
        logger.info("Created trigger '%s'", name)


def _downgrade_fts_sync(bind: 'Connection', triggers: dict[str, str]):
    """Drop sync triggers. Used by alembic migrations; only supports SQLite.

    Does not drop the FTS table itself, since this migration may not be the one
    that created it (it may have already existed before this migration was applied).
    """
    for name in triggers:
        bind.exec_driver_sql(f'DROP TRIGGER IF EXISTS {name}')


def _create_taxon_fts_table_sql() -> str:
    prefix_idxs = ', '.join([f'prefix={i}' for i in TAXON_PREFIX_INDEXES])
    return (
        f'CREATE VIRTUAL TABLE {TAXON_FTS_TABLE} USING fts5( '
        '   name, taxon_id, taxon_rank UNINDEXED, count_rank UNINDEXED, language_code,'
        f'  {prefix_idxs})'
    )


def _create_observation_fts_table_sql() -> str:
    prefix_idxs = ', '.join([f'prefix={i}' for i in OBS_PREFIX_INDEXES])
    return (
        f'CREATE VIRTUAL TABLE IF NOT EXISTS {OBS_FTS_TABLE} USING fts5( '
        '   text, observation_id, field,'
        f'  {prefix_idxs})'
    )


def _create_observation_fts_trigger_sql() -> dict[str, str]:
    return {
        'observation_ai': (
            'CREATE TRIGGER IF NOT EXISTS observation_ai AFTER INSERT ON observation '
            + f'BEGIN{_observation_fts_insert_statements("new")}END'
        ),
        'observation_au': (
            'CREATE TRIGGER IF NOT EXISTS observation_au AFTER UPDATE ON observation '
            'WHEN old.description IS NOT new.description '
            'OR old.place_guess IS NOT new.place_guess '
            'OR old.comments IS NOT new.comments '
            'OR old.identifications IS NOT new.identifications '
            + f'BEGIN\n  DELETE FROM {OBS_FTS_TABLE} WHERE observation_id = old.id;'
            + f'{_observation_fts_insert_statements("new")}END'
        ),
        'observation_ad': (
            'CREATE TRIGGER IF NOT EXISTS observation_ad AFTER DELETE ON observation '
            + f'BEGIN\n  DELETE FROM {OBS_FTS_TABLE} WHERE observation_id = old.id;\nEND'
        ),
    }


def _taxon_fts_insert_statements(row: str) -> str:
    return (
        f'\n  INSERT INTO {TAXON_FTS_TABLE} (taxon_id, name, taxon_rank, count_rank, language_code)\n'
        f"    SELECT {row}.id, {row}.name, {row}.rank, -1, NULL WHERE {row}.name IS NOT NULL AND {row}.name != '';"
        f'\n  INSERT INTO {TAXON_FTS_TABLE} (taxon_id, name, taxon_rank, count_rank, language_code)\n'
        f"    SELECT {row}.id, {row}.preferred_common_name, {row}.rank, -1, 'en' "
        f"    WHERE {row}.preferred_common_name IS NOT NULL AND {row}.preferred_common_name != '';\n"
    )


def _create_taxon_fts_trigger_sql() -> dict[str, str]:
    return {
        'taxon_ai': (
            'CREATE TRIGGER IF NOT EXISTS taxon_ai AFTER INSERT ON taxon '
            + f'BEGIN{_taxon_fts_insert_statements("new")}END'
        ),
        'taxon_au': (
            'CREATE TRIGGER IF NOT EXISTS taxon_au AFTER UPDATE ON taxon '
            'WHEN old.name IS NOT new.name '
            'OR old.preferred_common_name IS NOT new.preferred_common_name '
            'OR old.rank IS NOT new.rank '
            + f'BEGIN\n  DELETE FROM {TAXON_FTS_TABLE} WHERE taxon_id = old.id;'
            + f'{_taxon_fts_insert_statements("new")}END'
        ),
        'taxon_ad': (
            'CREATE TRIGGER IF NOT EXISTS taxon_ad AFTER DELETE ON taxon '
            + f'BEGIN\n  DELETE FROM {TAXON_FTS_TABLE} WHERE taxon_id = old.id;\nEND'
        ),
    }


def _observation_fts_insert_statements(row: str) -> str:
    return (
        f'\n  INSERT INTO {OBS_FTS_TABLE} (observation_id, text, field)\n'
        f"    SELECT {row}.id, {row}.description, 1 WHERE {row}.description IS NOT NULL AND {row}.description != '';"
        f'\n  INSERT INTO {OBS_FTS_TABLE} (observation_id, text, field)\n'
        f"    SELECT {row}.id, {row}.place_guess, 4 WHERE {row}.place_guess IS NOT NULL AND {row}.place_guess != '';"
        f'\n  INSERT INTO {OBS_FTS_TABLE} (observation_id, text, field)\n'
        f"    SELECT {row}.id, json_extract(value, '$.body'), 2\n"
        f"    FROM json_each(CASE WHEN json_valid({row}.comments) THEN {row}.comments ELSE '[]' END)\n"
        "    WHERE json_extract(value, '$.body') IS NOT NULL AND json_extract(value, '$.body') != '';"
        f'\n  INSERT INTO {OBS_FTS_TABLE} (observation_id, text, field)\n'
        f"    SELECT {row}.id, json_extract(value, '$.body'), 3\n"
        f"    FROM json_each(CASE WHEN json_valid({row}.identifications) THEN {row}.identifications ELSE '[]' END)\n"
        "    WHERE json_extract(value, '$.body') IS NOT NULL AND json_extract(value, '$.body') != '';\n"
    )


def index_observation_text(observations: Sequence[Observation], db_path: PathOrStr = DB_PATH):
    """Index observation text in FTS table: descriptions, places, comments, and identification
    comments. Replaces any previously indexed text associated with these observations.

    Args:
        observations: observations to index
        db_path: Path to SQLite database
    """
    all_obs_strs = [_get_obs_strs(obs) for obs in observations]

    with sqlite3.connect(db_path) as conn:
        placeholders = ','.join(['?'] * len(observations))
        conn.execute(
            f'DELETE FROM {OBS_FTS_TABLE} WHERE observation_id IN ({placeholders})',
            [obs.id for obs in observations],
        )
        conn.executemany(
            f'INSERT INTO {OBS_FTS_TABLE} (observation_id, text, field) VALUES (?, ?, ?)',
            chain.from_iterable(all_obs_strs),
        )
        conn.commit()


def _get_obs_strs(obs: Observation) -> list[tuple[int, str, int]]:
    obs_strs = []
    if obs.description:
        obs_strs.append((obs.id, obs.description, TextField.DESCRIPTION.value))
    if obs.place_guess:
        obs_strs.append((obs.id, obs.place_guess, TextField.PLACE.value))
    obs_strs.extend([(obs.id, c.body, TextField.COMMENT.value) for c in obs.comments if c.body])
    obs_strs.extend(
        [(obs.id, i.body, TextField.IDENTIFICATION.value) for i in obs.identifications if i.body]
    )
    return obs_strs


def optimize_fts_table(table: str, db_path: PathOrStr = DB_PATH):
    """Some final cleanup after loading a text search table"""
    logger.info(f'Optimizing FTS table {table}')
    progress = get_progress_spinner('Optimizing table')
    with progress, sqlite3.connect(db_path) as conn:
        conn.execute(f"INSERT INTO {table}({table}) VALUES('optimize')")
        conn.commit()


def _get_common_name_csvs(
    csv_dir: Path, languages: Optional[Iterable[str]] = None
) -> dict[str, Path]:
    """Get common name CSVs, for either all or some languages, with a separate table per language"""
    if languages and languages != 'all':
        common_name_csvs = {lang: csv_dir / f'VernacularNames-{lang}.csv' for lang in languages}
        return {lang: csv_path for lang, csv_path in common_name_csvs.items() if csv_path.exists()}
    else:
        return {
            path.stem.replace('VernacularNames-', ''): path
            for path in csv_dir.glob('VernacularNames-*.csv')
        }


def _add_taxon_counts(
    row: list, field_index: dict[str, int], taxon_counts: dict[int, float]
) -> list:
    """Add taxon counts to a chunk of taxon records read from CSV"""
    taxon_id = int(row[field_index['id']])
    row.append(taxon_counts.get(taxon_id, -1.0))
    lang_idx = field_index.get('language')
    if lang_idx is not None and row[lang_idx]:
        row[lang_idx] = str(row[lang_idx]).lower().replace('-', '_')
    return row


# TODO: Read from taxon table instead
def _normalize_taxon_counts(
    agg_path: PathOrStr = TAXON_AGGREGATES_PATH,
) -> dict[int, float]:
    """Read previously calculated taxon counts and apply log1p scaling to match iNat ES scoring."""
    import numpy as np
    import pandas as pd

    if not Path(agg_path).is_file():
        logger.warning(f'Taxon counts file not found: {agg_path}')
        return {}

    logger.info(f'Reading taxon counts from {agg_path}')
    df = pd.read_parquet(agg_path).reset_index()

    logger.info('Normalizing taxon counts')
    counts = df['observations_count_rg'].to_numpy()
    popularity = np.zeros_like(counts, dtype=float)
    observed_mask = ~np.isnan(counts)

    popularity[observed_mask] = np.log1p(counts[observed_mask]) * TAXON_COUNT_RANK_FACTOR
    popularity[~observed_mask] = -1.0

    df['count_rank'] = popularity
    return df.set_index('id')['count_rank'].to_dict()


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


def _strip_diacritics(text: str) -> str:
    """Strip diacritics/accents from text using NFD normalization."""
    if not text:
        return ''
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')


def _sanitize_fts_query(query: str) -> str:
    query = FTS5_SEPARATOR_CHARS.sub(' ', query)
    return ' '.join(query.split())
