"""Utilities for working with the iNaturalist dataset hosted by Amazon Open Data Program.

**Extra dependencies:** ``boto3``
"""
# TODO: Maybe pick another acronym? ODP may not make sense.
from pathlib import Path

from pyinaturalist_convert.db import create_tables
from pyinaturalist_convert.sqlite import load_table

from .constants import (
    DATA_DIR,
    DB_PATH,
    ODP_ARCHIVE_NAME,
    ODP_BUCKET_NAME,
    ODP_CSV_DIR,
    ODP_METADATA_KEY,
    PathOrStr,
)
from .download import CSVProgress, check_download, download_s3_file, untar_progress

OBS_COLUMN_MAP = {
    'latitude': 'latitude',
    'longitude': 'longitude',
    'observation_uuid': 'uuid',
    'observed_on': 'observed_on',
    'observer_id': 'user_id',
    'positional_accuracy': 'positional_accuracy',
    'quality_grade': 'quality_grade',
    'taxon_id': 'taxon_id',
}


def download_odp_metadata(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the iNat Open Data metadata archive. Reuses local data if it already
    exists and is up to date.

    Args:
        dest_dir: Optional directory to download to
    """
    dest_dir = Path(dest_dir).expanduser()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / ODP_ARCHIVE_NAME

    # Skip download if we're already up to date
    if check_download(dest_file, bucket=ODP_BUCKET_NAME, key=ODP_METADATA_KEY, release_interval=31):
        return

    # Otherwise, download and extract files
    download_s3_file(ODP_BUCKET_NAME, ODP_METADATA_KEY, dest_file)
    untar_progress(dest_file, dest_dir / 'inaturalist-open-data')


def load_odp_observations(
    csv_path: PathOrStr = ODP_CSV_DIR / 'observations.csv',
    db_path: PathOrStr = DB_PATH,
):
    create_tables(db_path)
    column_map = OBS_COLUMN_MAP
    progress = CSVProgress(csv_path)
    with progress:
        load_table(csv_path, db_path, 'observation', column_map, delimiter='\t', progress=progress)
