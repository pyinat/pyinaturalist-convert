"""Utilities for working with iNaturalist Open Data"""
from datetime import datetime, timezone
from pathlib import Path

from .constants import DATA_DIR, METADATA_KEY, ODP_ARCHIVE_NAME, ODP_BUCKET_NAME, PathOrStr
from .download import FlatTarFile, download_s3_file, get_file_mtime, get_s3_mtime
from .progress import ProgressIO


def download_odp_metadata(download_dir: PathOrStr = DATA_DIR, verbose: int = 0):
    """Download and extract the iNat Open Data metadata archive. Reuses local data if it alread
    exists and is up to date.

    Args:
        download_dir: Optional directory to download to
    """
    download_dir = Path(download_dir).expanduser()
    download_dir.mkdir(parents=True, exist_ok=True)
    download_file = download_dir / ODP_ARCHIVE_NAME

    # Skip download if we're already up to date
    if check_odp_download(download_file):
        return

    # Otherwise, download and extract files
    print(f'Downloading to: {download_file}')
    download_s3_file(ODP_BUCKET_NAME, METADATA_KEY, download_file)
    progress_file = ProgressIO(download_file)
    with FlatTarFile.open(fileobj=progress_file) as archive, progress_file.progress:
        archive.extractall(path=download_dir)


def check_odp_download(download_file: Path) -> bool:
    """Check if the iNat Open Data file already exists locally, and if a newer version is not yet available"""
    if not download_file.exists():
        return False

    local_mtime = get_file_mtime(download_file)
    remote_mtime = get_s3_mtime(ODP_BUCKET_NAME, METADATA_KEY)
    if local_mtime >= remote_mtime:
        print(f'[cyan]File already exists and is up to date:[/cyan] {download_file}')
        estimate_next_release(remote_mtime)
        return True
    else:
        print(f'[cyan]File exists, but is out of date:[/cyan] {download_file}')
        return False


def estimate_next_release(s3_mtime: datetime):
    """Get an estimate of time until next update. Updates are roughly monthly, but not necessarily
    on the same day each month.
    """
    elapsed = datetime.now(timezone.utc) - s3_mtime
    est_release_days = 31 - elapsed.days
    print(f'[cyan]Possible new release in ~[magenta]{est_release_days} [cyan]days')
