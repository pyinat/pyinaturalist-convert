# flake8: noqa: F401
from logging import getLogger
from pathlib import Path
from tempfile import gettempdir
from time import time

from pyinaturalist_convert.dwca import (
    TaxonAutocompleter,
    download_dwca,
    download_taxa,
    load_taxonomy_table,
    load_taxonomy_text_search_tables,
)
from test.conftest import SAMPLE_DATA_DIR

CSV_DIR = SAMPLE_DATA_DIR / 'inaturalist-taxonomy.dwca'
TEMP = Path(gettempdir())

logger = getLogger(__name__)


def test_text_search():
    db_path = TEMP / 'taxa.db'
    load_taxonomy_text_search_tables(csv_dir=CSV_DIR, db_path=db_path)
    ta = TaxonAutocompleter(db_path=db_path)

    results = ta.search('ave')
    assert results[0].id == 3 and results[0].name == 'Aves'

    results = ta.search('franco')
    assert len(results) == 3
    assert results[0].id == 649 and results[0].name == 'Black Francolin'

    db_path.unlink()


def benchmark():
    iterations = 10000
    ta = TaxonAutocompleter()
    start = time()

    for _ in range(iterations):
        ta.search('berry', language=None)
    elapsed = time() - start

    logger.info(f'Total: {elapsed:.2f}s')
    logger.info(f'Avg per query: {(elapsed/iterations)*1000:2f}ms')
