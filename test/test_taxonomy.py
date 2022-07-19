# flake8: noqa: F401
import sqlite3
from csv import DictReader
from logging import getLogger
from unittest.mock import patch

from pyinaturalist_convert.dwca import load_dwca_taxa
from pyinaturalist_convert.sqlite import load_table
from pyinaturalist_convert.taxonomy import aggregate_taxon_db
from test.conftest import SAMPLE_DATA_DIR

CSV_DIR = SAMPLE_DATA_DIR / 'inaturalist-taxonomy.dwca'

logger = getLogger(__name__)


@patch('pyinaturalist_convert.taxonomy.sleep')
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
            'count': 'count',
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
        actual_counts = {
            row['id']: row['count'] for row in conn.execute('SELECT * FROM taxon').fetchall()
        }
        actual_leaf_taxa = {
            row['id']: row['leaf_taxon_count']
            for row in conn.execute('SELECT * FROM taxon').fetchall()
        }
        test_common_name = conn.execute(
            'SELECT preferred_common_name FROM taxon WHERE id=101'
        ).fetchone()[0]

    assert actual_counts == expected_counts
    assert actual_leaf_taxa == expected_leaf_taxa
    assert counts_path.is_file()
    assert test_common_name == 'test_kingdom_1'
