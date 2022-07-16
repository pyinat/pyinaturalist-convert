# flake8: noqa: F401
import sqlite3
from logging import getLogger

from pyinaturalist_convert.dwca import (
    download_dwca_observations,
    download_dwca_taxa,
    load_dwca_observations,
    load_dwca_taxa,
)
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
