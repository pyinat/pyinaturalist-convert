import sqlite3
from logging import getLogger
from time import time

import pytest
from pyinaturalist import Comment, Identification, Observation, Taxon

from pyinaturalist_convert.db import create_tables, save_observations, save_taxa
from pyinaturalist_convert.fts import (
    ObservationAutocompleter,
    TaxonAutocompleter,
    TextField,
    create_observation_fts_table,
    create_observation_fts_triggers,
    create_taxon_fts_table,
    create_taxon_fts_triggers,
    index_observation_text,
    load_fts_taxa,
)
from test.conftest import SAMPLE_DATA_DIR

CSV_DIR = SAMPLE_DATA_DIR / 'inaturalist-taxonomy.dwca'
COUNTS_PATH = SAMPLE_DATA_DIR / 'taxon_counts_fts.parquet'
logger = getLogger(__name__)


def test_taxon_text_search(tmp_path):
    db_path = tmp_path / 'taxa.db'
    load_fts_taxa(csv_dir=CSV_DIR, db_path=db_path, agg_path=COUNTS_PATH)
    ta = TaxonAutocompleter(db_path=db_path)

    results = ta.search('ave')
    assert results[0].id == 3 and results[0].name == 'Aves'

    results = ta.search('franco')
    assert len(results) == 3
    assert results[0].id == 649 and results[0].name == 'Black Francolin'

    assert len(ta.search('')) == 0
    assert len(ta.search('franco', language=None)) == 3


def test_taxon_text_search__limit(tmp_path):
    db_path = tmp_path / 'taxa.db'
    load_fts_taxa(csv_dir=CSV_DIR, db_path=db_path, agg_path=COUNTS_PATH)
    ta = TaxonAutocompleter(db_path=db_path)

    ta.limit = 2
    assert len(ta.search('franco')) == 2
    ta.limit = -1
    assert len(ta.search('franco')) == 3


obs_1 = Observation(
    id=1,
    description='This is a test observation with a description',
    comments=[Comment(body='This is a test comment')],
    identifications=[Identification(body='This is a test identification comment')],
    place_guess='Riverdale, Maryland, United States',
)
obs_2 = Observation(
    id=2,
    description='description 2',
    comments=[Comment(body='comment')],
    identifications=[Identification(body='identification comment')],
    place_guess='Winnipeg, Manitoba, Canada',
)


def test_observation_text_search(tmp_path):
    db_path = tmp_path / 'obs.db'
    create_observation_fts_table(db_path)
    index_observation_text([obs_1, obs_2], db_path=db_path)

    oa = ObservationAutocompleter(db_path=db_path, truncate_match_chars=25)
    assert len(oa.search('test')) == 3
    assert len(oa.search('description')) == 2
    assert len(oa.search('comment')) == 4
    assert len(oa.search('identification')) == 2
    assert len(oa.search('')) == 0

    # Test results with matching text truncated to 25 characters
    results = oa.search('test observation')
    assert results[0] == (1, '...test observation wi...')
    results = oa.search('test com')
    assert results[0] == (1, 'This is a test comment')
    results = oa.search('test ident')
    assert results[0] == (1, '...test identification...')
    results = oa.search('description 2')
    assert results[0] == (2, 'description 2')

    oa.truncate_match_chars = 30
    results = oa.search('maryland')
    assert results[0] == (1, '...Maryland, United States')
    results = oa.search('manitoba')
    assert results[0] == (2, 'Winnipeg, Manitoba, Canada')


def test_observation_text_search__limit(tmp_path):
    db_path = tmp_path / 'obs.db'
    create_observation_fts_table(db_path)
    index_observation_text([obs_1, obs_2], db_path=db_path)

    oa = ObservationAutocompleter(db_path=db_path, limit=2)
    assert len(oa.search('test')) == 2
    oa.limit = -1
    assert len(oa.search('test')) == 3


def test_observation_text_search__by_field(tmp_path):
    db_path = tmp_path / 'obs.db'
    create_observation_fts_table(db_path)
    index_observation_text([obs_1, obs_2], db_path=db_path)

    oa = ObservationAutocompleter(db_path=db_path)
    fields_except_place = [TextField.DESCRIPTION, TextField.COMMENT, TextField.IDENTIFICATION]
    assert len(oa.search('test', fields=[TextField.DESCRIPTION])) == 1
    assert len(oa.search('test', fields=[TextField.DESCRIPTION, TextField.COMMENT])) == 2
    assert len(oa.search('test', fields=fields_except_place)) == 3
    assert len(oa.search('description', fields=[TextField.DESCRIPTION])) == 2
    assert len(oa.search('description', fields=[TextField.COMMENT])) == 0
    assert len(oa.search('comment', fields=[TextField.COMMENT])) == 2
    assert len(oa.search('comment', fields=[TextField.COMMENT, TextField.IDENTIFICATION])) == 4
    assert len(oa.search('comment', fields=[TextField.DESCRIPTION])) == 0
    assert len(oa.search('identification', fields=[TextField.IDENTIFICATION])) == 2
    assert len(oa.search('identification', fields=[TextField.DESCRIPTION])) == 0
    assert len(oa.search('maryland', fields=fields_except_place)) == 0
    assert len(oa.search('maryland', fields=[TextField.PLACE])) == 1


def test_observation_text_search__reindex(tmp_path):
    db_path = tmp_path / 'obs.db'
    create_observation_fts_table(db_path)
    index_observation_text([obs_1, obs_2], db_path=db_path)

    oa = ObservationAutocompleter(db_path=db_path)

    # Update an observation and re-index
    obs_1b = Observation(
        id=1,
        description='replaced description',
        comments=[Comment(body='replaced comment')],
        identifications=[Identification(body='replaced identification comment')],
        place_guess='replaced place',
    )
    index_observation_text([obs_1b], db_path=db_path)

    # Expect previously indexed results to be replaced
    assert len(oa.search('test')) == 0
    assert len(oa.search('maryland')) == 0
    assert len(oa.search('replaced')) == 4
    assert len(oa.search('description')) == 2
    assert len(oa.search('comment')) == 4
    assert len(oa.search('identification')) == 2
    assert len(oa.search('place')) == 1


def test_observation_fts_triggers(tmp_path):
    db_path = tmp_path / 'obs.db'
    create_tables(db_path)
    create_observation_fts_table(db_path)
    create_observation_fts_triggers(db_path)

    # INSERT trigger: saving an observation should populate FTS table
    save_observations([obs_1], db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute('SELECT observation_id, field FROM observation_fts').fetchall()
    assert len(rows) == 4  # description, place, comment, identification
    assert all(row[0] == 1 for row in rows)

    # UPDATE trigger: updating observation should replace old FTS entries
    obs_1_updated = Observation(
        id=1,
        description='updated description',
        place_guess='updated place',
        comments=[Comment(body='updated comment')],
        identifications=[Identification(body='updated identification')],
    )
    save_observations([obs_1_updated], db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute('SELECT text FROM observation_fts WHERE observation_id = 1').fetchall()
    texts = {row[0] for row in rows}
    assert 'updated description' in texts
    assert 'updated place' in texts
    assert 'updated comment' in texts
    assert 'updated identification' in texts
    # Old values should be gone
    assert 'This is a test observation with a description' not in texts

    # DELETE trigger: deleting the observation should remove its FTS entries
    with sqlite3.connect(db_path) as conn:
        conn.execute('DELETE FROM observation WHERE id = 1')
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute('SELECT * FROM observation_fts WHERE observation_id = 1').fetchall()
    assert len(rows) == 0

    # Empty strings should not be indexed (same behavior as manual indexing)
    obs_empty = Observation(
        id=2,
        description='',
        place_guess='',
        comments=[Comment(body='')],
        identifications=[Identification(body='')],
    )
    save_observations([obs_empty], db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute('SELECT * FROM observation_fts WHERE observation_id = 2').fetchall()
    assert len(rows) == 0

    # Malformed JSON in denormalized fields should not break writes or trigger indexing
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO observation (id, description, place_guess, comments, identifications)
            VALUES (?, ?, ?, ?, ?)
            """,
            (3, 'ok description', 'ok place', 'not-json', '{bad'),
        )
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            'SELECT text, field FROM observation_fts WHERE observation_id = 3 ORDER BY field'
        ).fetchall()
    assert rows == [
        ('ok description', TextField.DESCRIPTION.value),
        ('ok place', TextField.PLACE.value),
    ]


def _get_taxon_fts_rows(db_path, taxon_id):
    """Helper to fetch FTS rows for a taxon"""
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            'SELECT name, language_code FROM taxon_fts WHERE taxon_id = ? ORDER BY language_code',
            (taxon_id,),
        ).fetchall()


@pytest.mark.parametrize(
    'taxon_id, name, common_name, expected_names',
    [
        (1, 'Aves', 'Birds', {'Aves', 'Birds'}),
        (2, 'Carnivora', None, {'Carnivora'}),
        (3, 'Felidae', '', {'Felidae'}),  # Empty string not indexed
    ],
)
def test_taxon_fts_triggers__insert(tmp_path, taxon_id, name, common_name, expected_names):
    """Test INSERT triggers create correct FTS rows"""
    db_path = tmp_path / 'taxa.db'
    create_tables(db_path)
    create_taxon_fts_table(db_path)
    create_taxon_fts_triggers(db_path)

    taxon = Taxon(id=taxon_id, name=name, rank='class', preferred_common_name=common_name)
    save_taxa([taxon], db_path=db_path)

    rows = _get_taxon_fts_rows(db_path, taxon_id)
    actual_names = {row[0] for row in rows}
    assert actual_names == expected_names


def test_taxon_fts_triggers__update(tmp_path):
    """Test UPDATE trigger replaces old FTS rows"""
    db_path = tmp_path / 'taxa.db'
    create_tables(db_path)
    create_taxon_fts_table(db_path)
    create_taxon_fts_triggers(db_path)

    # Insert initial taxon
    taxon = Taxon(id=1, name='Aves', rank='class', preferred_common_name='Birds')
    save_taxa([taxon], db_path=db_path)
    assert {row[0] for row in _get_taxon_fts_rows(db_path, 1)} == {'Aves', 'Birds'}

    # Update scientific name
    updated = Taxon(id=1, name='Aves updated', rank='class', preferred_common_name='Birds')
    save_taxa([updated], db_path=db_path)
    rows = _get_taxon_fts_rows(db_path, 1)
    actual_names = {row[0] for row in rows}

    # Old scientific name should be gone, new one present, common name unchanged
    assert 'Aves' not in actual_names
    assert 'Aves updated' in actual_names
    assert 'Birds' in actual_names


def test_taxon_fts_triggers__delete(tmp_path):
    """Test DELETE trigger removes FTS rows"""
    db_path = tmp_path / 'taxa.db'
    create_tables(db_path)
    create_taxon_fts_table(db_path)
    create_taxon_fts_triggers(db_path)

    # Insert and verify
    taxon = Taxon(id=1, name='Aves', rank='class', preferred_common_name='Birds')
    save_taxa([taxon], db_path=db_path)
    assert len(_get_taxon_fts_rows(db_path, 1)) == 2

    # Delete and verify FTS rows removed
    with sqlite3.connect(db_path) as conn:
        conn.execute('DELETE FROM taxon WHERE id = 1')
        conn.commit()

    assert len(_get_taxon_fts_rows(db_path, 1)) == 0


def benchmark():
    iterations = 10000
    ta = TaxonAutocompleter()
    start = time()

    for _ in range(iterations):
        ta.search('berry', language=None)
    elapsed = time() - start

    logger.info(f'Total: {elapsed:.2f}s')
    logger.info(f'Avg per query: {(elapsed / iterations) * 1000:2f}ms')
