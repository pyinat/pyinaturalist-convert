# flake8: noqa: F401
import sqlite3
from csv import DictReader
from logging import getLogger

from pyinaturalist_convert.dwca import (
    aggregate_taxon_counts,
    download_dwca_observations,
    download_dwca_taxa,
    load_dwca_observations,
    load_dwca_taxa,
)
from pyinaturalist_convert.sqlite import load_table
from test.conftest import SAMPLE_DATA_DIR

CSV_DIR = SAMPLE_DATA_DIR / 'inaturalist-taxonomy.dwca'

logger = getLogger(__name__)


def test_load_observation_table(tmp_path):
    csv_path = SAMPLE_DATA_DIR / 'observations_dwca.csv'
    db_path = tmp_path / 'observations.db'
    load_dwca_observations(csv_path, db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute('SELECT * FROM observation ORDER BY id').fetchall()

    assert len(rows) == 50
    assert rows[0]['id'] == 38
    assert rows[0]['taxon_id'] == 47993
    assert rows[0]['geoprivacy'] == 'obscured'
    assert rows[0]['longitude'] == -122.2834661155


def test_aggregate_taxon_counts(tmp_path):
    csv_path = SAMPLE_DATA_DIR / 'taxon_counts.csv'
    db_path = tmp_path / 'observations.db'

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
    aggregate_taxon_counts(db_path)

    # Get and compare results
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        actual_counts = {
            row['id']: row['count'] for row in conn.execute('SELECT * FROM taxon').fetchall()
        }
    assert actual_counts == expected_counts
