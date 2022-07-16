# flake8: noqa: F401
import sqlite3
from csv import DictReader
from logging import getLogger

from pyinaturalist_convert.dwca import load_dwca_taxa
from pyinaturalist_convert.sqlite import load_table
from pyinaturalist_convert.taxonomy import aggregate_taxon_db
from test.conftest import SAMPLE_DATA_DIR

CSV_DIR = SAMPLE_DATA_DIR / 'inaturalist-taxonomy.dwca'

logger = getLogger(__name__)


def test_aggregate_taxon_db(tmp_path):
    csv_path = SAMPLE_DATA_DIR / 'taxon_counts.csv'
    db_path = tmp_path / 'observations.db'
    counts_path = tmp_path / 'taxon_counts.parquet'

    with open(csv_path) as f:
        expected_counts = {int(row['id']): int(row['expected']) for row in DictReader(f)}

    load_dwca_taxa(
        csv_path=csv_path,
        db_path=db_path,
        column_map={'id': 'id', 'parent_id': 'parent_id', 'name': 'name', 'count': 'count'},
    )

    # Observation table with just taxon IDs to count
    load_table(
        csv_path=csv_path,
        db_path=db_path,
        table_name='observation',
        column_map={'id': 'taxon_id'},
    )

    # Aggregate counts
    aggregate_taxon_db(db_path, counts_path=counts_path)

    # Get and compare results
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        actual_counts = {
            row['id']: row['count'] for row in conn.execute('SELECT * FROM taxon').fetchall()
        }
    assert actual_counts == expected_counts
    assert counts_path.is_file()
