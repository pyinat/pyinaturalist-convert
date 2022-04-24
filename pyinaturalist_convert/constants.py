from pathlib import Path
from typing import Union

from platformdirs import user_data_dir

DATA_DIR = Path(user_data_dir()) / 'pyinaturalist'
DEFAULT_DB_PATH = DATA_DIR / 'inaturalist-open-data.db'
DEFAULT_DB_URI = f'sqlite:////{DEFAULT_DB_PATH}'

ODP_ARCHIVE_NAME = 'inaturalist-open-data-latest.tar.gz'
ODP_BUCKET_NAME = 'inaturalist-open-data'
METADATA_KEY = f'metadata/{ODP_ARCHIVE_NAME}'
PHOTO_BASE_URL = 'https://inaturalist-open-data.s3.amazonaws.com/photos/'

PathOrStr = Union[Path, str]
