# TODO
# flake8: noqa: F401
from logging import getLogger
from pathlib import Path
from tempfile import gettempdir

from pyinaturalist_convert.dwca import download_dwca, download_dwca_taxa, load_taxon_table
from test.conftest import SAMPLE_DATA_DIR

CSV_DIR = SAMPLE_DATA_DIR / 'inaturalist-taxonomy.dwca'
TEMP = Path(gettempdir())

logger = getLogger(__name__)
