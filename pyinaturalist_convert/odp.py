"""Utilities for working with iNaturalist Open Data"""
from pathlib import Path

from .constants import DATA_DIR, ODP_ARCHIVE_NAME, ODP_BUCKET_NAME, ODP_METADATA_KEY, PathOrStr
from .download import check_download, download_s3_file, untar_progress


def download_odp_metadata(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the iNat Open Data metadata archive. Reuses local data if it alread
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
    untar_progress(dest_file, dest_dir)
