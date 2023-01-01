# flake8: noqa: F401
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from csv import DictReader
from logging import getLogger
from unittest.mock import patch

from pytest_cov.embed import cleanup_on_sigterm

from pyinaturalist_convert.dwca import load_dwca_taxa
from pyinaturalist_convert.sqlite import load_table
from pyinaturalist_convert.taxonomy import aggregate_taxon_db
from test.conftest import SAMPLE_DATA_DIR

CSV_DIR = SAMPLE_DATA_DIR / 'inaturalist-taxonomy.dwca'

logger = getLogger(__name__)
cleanup_on_sigterm()


@patch('pyinaturalist_convert.taxonomy.sleep')
@patch(
    'pyinaturalist_convert.taxonomy.ProcessPoolExecutor', ThreadPoolExecutor
)  # Can't get coverage to work with multiprocessing
def test_aggregate_taxon_db(mock_sleep, tmp_path):
    taxon_db_path = SAMPLE_DATA_DIR / 'taxon_counts.csv'
    common_names_path = SAMPLE_DATA_DIR / 'taxon_common_names.csv'
    db_path = tmp_path / 'observations.db'
    counts_path = tmp_path / 'taxon_counts.parquet'

    with open(taxon_db_path) as f:
        rows = list(DictReader(f))
    expected_counts = {int(row['id']): int(row['expected_count']) for row in rows}
    expected_leaf_taxa = {int(row['id']): int(row['expected_leaf_taxa']) for row in rows}

    load_dwca_taxa(
        csv_path=taxon_db_path,
        db_path=db_path,
        column_map={
            'id': 'id',
            'parent_id': 'parent_id',
            'name': 'name',
            'rank': 'rank',
            'count': 'observations_count_rg',
        },
    )

    # Observation table with just taxon IDs to count
    load_table(
        csv_path=taxon_db_path,
        db_path=db_path,
        table_name='observation',
        column_map={'id': 'taxon_id'},
    )

    # Aggregate counts
    aggregate_taxon_db(
        db_path,
        counts_path=counts_path,
        common_names_path=common_names_path,
    )

    # Get and compare results
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        actual_observation_counts = {
            row['id']: row['observations_count_rg']
            for row in conn.execute('SELECT * FROM taxon').fetchall()
        }
        actual_leaf_taxon_counts = {
            row['id']: row['leaf_taxa_count']
            for row in conn.execute('SELECT * FROM taxon').fetchall()
        }
        test_common_name = conn.execute(
            'SELECT preferred_common_name FROM taxon WHERE id=101'
        ).fetchone()[0]

    assert actual_observation_counts == expected_counts
    assert actual_leaf_taxon_counts == expected_leaf_taxa
    assert counts_path.is_file()
    assert test_common_name == 'test_kingdom_1'
