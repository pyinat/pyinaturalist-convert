from pathlib import Path
from typing import Union

from platformdirs import user_data_dir

DATA_DIR = Path(user_data_dir()) / 'pyinaturalist'
DB_PATH = DATA_DIR / 'observations.db'
DEFAULT_DB_URI = f'sqlite:////{DB_PATH}'

DWCA_DIR = DATA_DIR / 'dwca'
DWCA_URL = 'https://static.inaturalist.org/observations/gbif-observations-dwca.zip'
DWCA_TAXA_URL = 'https://www.inaturalist.org/taxa/inaturalist-taxonomy.dwca.zip'

ODP_ARCHIVE_NAME = 'inaturalist-open-data-latest.tar.gz'
ODP_BUCKET_NAME = 'inaturalist-open-data'
ODP_METADATA_KEY = f'metadata/{ODP_ARCHIVE_NAME}'
PHOTO_BASE_URL = 'https://inaturalist-open-data.s3.amazonaws.com/photos/'

TAXON_CSV_DIR = DATA_DIR / 'inaturalist-taxonomy.dwca'
TAXON_CSV = TAXON_CSV_DIR / 'taxa.csv'
TAXON_COUNTS = DATA_DIR / 'taxon_counts.parquet'

OBS_CSV_DIR = DATA_DIR / 'gbif-observations-dwca'
OBS_CSV = OBS_CSV_DIR / 'observations.csv'

PathOrStr = Union[Path, str]
