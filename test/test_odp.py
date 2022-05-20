import sqlite3
from unittest.mock import patch

from pyinaturalist_convert.odp import load_odp_tables
from test.conftest import SAMPLE_DATA_DIR


@patch('pyinaturalist_convert.odp.download_s3_file')
@patch('pyinaturalist_convert.odp.untar_progress')
def test_load_odp_tables(untar_progress, download_s3_file, tmp_path):
    db_path = tmp_path / 'observations.db'
    load_odp_tables(SAMPLE_DATA_DIR, db_path)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute('SELECT COUNT(*) FROM observation').fetchone()[0] == 50
        assert conn.execute('SELECT COUNT(*) FROM photo').fetchone()[0] == 50
        assert conn.execute('SELECT COUNT(*) FROM taxon').fetchone()[0] == 50
        assert conn.execute('SELECT COUNT(*) FROM user').fetchone()[0] == 50
