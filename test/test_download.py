from shutil import which
from subprocess import CompletedProcess
from unittest.mock import patch

import pytest

from pyinaturalist_convert.download import _count_lines, _count_lines_polars, _count_lines_wc
from test.conftest import SAMPLE_DATA_DIR

try:
    import polars  # noqa: F401

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

HAS_WC = which('wc') is not None

CSV_FILE = SAMPLE_DATA_DIR / 'observations.csv'


def test_count_lines_python():
    assert _count_lines(CSV_FILE) == 51


@pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
def test_count_lines_polars():
    assert _count_lines_polars(CSV_FILE) == 51


@pytest.mark.skipif(not HAS_WC, reason='wc not available')
def test_count_lines_wc():
    assert _count_lines_wc(CSV_FILE) == 51


@pytest.mark.skipif(not HAS_WC, reason='wc not available')
def test_count_lines_wc__empty_file(tmp_path):
    p = tmp_path / 'empty.csv'
    p.write_text('')
    assert _count_lines_wc(p) == 0


def test_count_lines_wc__error():
    mock_result = CompletedProcess(args=[], returncode=1, stdout='unparsable output', stderr='')
    with patch('pyinaturalist_convert.download.subprocess.run', return_value=mock_result):
        assert _count_lines_wc('fake_file.csv') == 0
