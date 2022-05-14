# TODO
# flake8: noqa: F401
import sqlite3
from csv import DictReader
from logging import getLogger
from pathlib import Path

from pyinaturalist_convert.dwca import (
    aggregate_taxon_counts,
    download_dwca,
    download_dwca_taxa,
    load_taxon_table,
)
from pyinaturalist_convert.sqlite import load_table
from test.conftest import SAMPLE_DATA_DIR

CSV_DIR = SAMPLE_DATA_DIR / 'inaturalist-taxonomy.dwca'

logger = getLogger(__name__)


def test_aggregate_taxon_counts(tmp_path):
    """Make a small"""
    csv_path = SAMPLE_DATA_DIR / 'taxon_counts.csv'
    taxon_db_path = tmp_path / 'taxa.db'
    obs_db_path = tmp_path / 'observations.db'

    with open(csv_path) as f:
        expected_counts = {int(row['id']): int(row['expected']) for row in DictReader(f)}

    load_taxon_table(
        csv_path=csv_path,
        db_path=taxon_db_path,
        column_map={'id': 'id', 'parent_id': 'parent_id', 'name': 'name', 'count': 'count'},
    )

    # Observation table with just taxon IDs to count
    load_table(
        csv_path=csv_path,
        db_path=obs_db_path,
        column_map={'id': 'taxonID'},
    )

    # Aggregate counts
    aggregate_taxon_counts(taxon_db_path, obs_db_path)

    # Get and compare results
    with sqlite3.connect(taxon_db_path) as conn:
        conn.row_factory = sqlite3.Row
        actual_counts = {
            row['id']: row['count'] for row in conn.execute('SELECT * FROM taxa').fetchall()
        }
    assert actual_counts == expected_counts
