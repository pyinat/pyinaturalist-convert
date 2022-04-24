"""Utilities for working with the iNat GBIF DwC archive"""
from os.path import basename
from pathlib import Path

from .constants import DATA_DIR, DWCA_DIR, DWCA_URL, PathOrStr
from .download import check_download, download_file, unzip_progress


def download_dwca(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the GBIF DwC archive. Reuses local data if it already exists and is
    up to date.

    Args:
        download_dir: Alternative download directory
    """
    dest_dir = Path(dest_dir).expanduser()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / basename(DWCA_URL)

    # Skip download if we're already up to date
    if check_download(dest_file, url=DWCA_URL, release_interval=7):
        return

    # Otherwise, download and extract files
    download_file(DWCA_URL, dest_file)
    unzip_progress(dest_file, dest_dir / 'dwca')


def get_dwca_reader(dest_path: PathOrStr = DWCA_DIR):
    """Get a :py:class:`~dwca.DwCAReader` for the GBIF DwC archive.

    Args:
        dwca_dir: Alternative archive file path (zipped) or directory (extracted)
    """
    from dwca.read import DwCAReader

    # Extract the archive, if it hasn't already been done
    dest_path = Path(dest_path).expanduser()
    if dest_path.is_file():
        unzip_progress(dest_path, dest_path / 'dwca')
        dest_path = dest_path / 'dwca'

    return DwCAReader(dest_path)
