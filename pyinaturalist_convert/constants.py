# flake8: noqa: F401
from pathlib import Path
from typing import Union

from pyinaturalist.constants import DATA_DIR

DB_PATH = DATA_DIR / 'observations.db'
DEFAULT_DB_URI = f'sqlite:////{DB_PATH}'

DWCA_DIR = DATA_DIR / 'dwca'
DWCA_URL = 'https://static.inaturalist.org/observations/gbif-observations-dwca.zip'
DWCA_TAXA_URL = 'https://www.inaturalist.org/taxa/inaturalist-taxonomy.dwca.zip'

ODP_ARCHIVE_NAME = 'inaturalist-open-data-latest.tar.gz'
ODP_BUCKET_NAME = 'inaturalist-open-data'
ODP_METADATA_KEY = f'metadata/{ODP_ARCHIVE_NAME}'
ODP_CSV_DIR = DATA_DIR / 'inaturalist-open-data'
ODP_OBS_CSV = ODP_CSV_DIR / 'observations.csv'
ODP_TAXON_CSV = ODP_CSV_DIR / 'taxa.csv'
ODP_PHOTO_CSV = ODP_CSV_DIR / 'photos.csv'
ODP_USER_CSV = ODP_CSV_DIR / 'observers.csv'

DWCA_TAXON_CSV_DIR = DATA_DIR / 'inaturalist-taxonomy.dwca'
DWCA_TAXON_CSV = DWCA_TAXON_CSV_DIR / 'taxa.csv'
DWCA_OBS_CSV_DIR = DATA_DIR / 'gbif-observations-dwca'
DWCA_OBS_CSV = DWCA_OBS_CSV_DIR / 'observations.csv'
TAXON_COUNTS = DATA_DIR / 'taxon_counts.parquet'

PathOrStr = Union[Path, str]
