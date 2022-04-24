from pathlib import Path
from typing import Union

from platformdirs import user_data_dir

DATA_DIR = Path(user_data_dir()) / 'inaturalist'
DEFAULT_DB_PATH = DATA_DIR / 'inaturalist-open-data.db'
DEFAULT_DB_URI = f'sqlite:////{DEFAULT_DB_PATH}'

ARCHIVE_NAME = 'inaturalist-open-data-latest.tar.gz'
BUCKET_NAME = 'inaturalist-open-data'
METADATA_KEY = f'metadata/{ARCHIVE_NAME}'
PHOTO_BASE_URL = 'https://inaturalist-open-data.s3.amazonaws.com/photos/'

PathOrStr = Union[Path, str]
