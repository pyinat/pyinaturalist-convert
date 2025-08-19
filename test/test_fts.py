from logging import getLogger
from time import time

from pyinaturalist import Comment, Identification, Observation

from pyinaturalist_convert.fts import (
    ObservationAutocompleter,
    TaxonAutocompleter,
    TextField,
    create_observation_fts_table,
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


def benchmark():
    iterations = 10000
    ta = TaxonAutocompleter()
    start = time()

    for _ in range(iterations):
        ta.search('berry', language=None)
    elapsed = time() - start

    logger.info(f'Total: {elapsed:.2f}s')
    logger.info(f'Avg per query: {(elapsed / iterations) * 1000:2f}ms')
