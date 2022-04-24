"""Functions for downloading and extracting inaturalist open data"""
# TODO: Download separate archive files in parallel?
from datetime import datetime, timezone
from os.path import basename
from pathlib import Path
from tarfile import TarFile

from rich import print

# from .constants import ARCHIVE_NAME, BUCKET_NAME, DATA_DIR, METADATA_KEY, PathOrStr
from .progress import ProgressIO, get_download_progress


class FlatTarFile(TarFile):
    """Extracts all archive contents to a flat base directory, ignoring archive subdirectories"""

    def extract(self, member, path="", **kwargs):
        if member.isfile():
            member.name = basename(member.name)
            super().extract(member, path, **kwargs)


def download_metadata(download_dir: PathOrStr = DATA_DIR, verbose: int = 0):
    """Download and extract metadata archive. Reuses local data if already exists and is up to date.

    Args:
        download_path: Optional file path to download to
    """
    download_dir = Path(download_dir).expanduser()
    download_dir.mkdir(parents=True, exist_ok=True)
    download_file = download_dir / ARCHIVE_NAME

    # Skip download if we're already up to date
    if not check_download(download_file):
        return

    # Otherwise, download and extract files
    print(f'Downloading to: {download_file}')
    get_s3_file(BUCKET_NAME, METADATA_KEY, download_file)
    progress_file = ProgressIO(download_file)
    with FlatTarFile.open(fileobj=progress_file) as archive, progress_file.progress:
        archive.extractall(path=download_dir)


def check_download(download_file: Path) -> bool:
    """Check if the file already exists locally, and if a newer version is not yet available"""
    new_download = True
    if download_file.exists():
        s3_mtime = get_s3_mtime(BUCKET_NAME, METADATA_KEY)
        file_mtime = get_file_mtime(download_file)
        if s3_mtime > file_mtime:
            print(f'[cyan]File exists, but is out of date:[/cyan] {download_file}')
        else:
            print(f'[cyan]File already exists and is up to date:[/cyan] {download_file}')
            estimate_next_release(s3_mtime)
            new_download = False
    return new_download


def get_file_mtime(file: Path) -> datetime:
    """Get the modified time of a file, as a timezone-aware datetime"""
    file_mtime = file.stat().st_mtime
    return datetime.fromtimestamp(file_mtime).astimezone()


def get_s3_mtime(bucket_name: str, key: str) -> datetime:
    """Get the modified time of an S3 file"""
    s3 = _get_s3_client()
    head = s3.head_object(Bucket=bucket_name, Key=key)
    return head['LastModified']


def get_s3_file(bucket_name: str, key: str, download_file: PathOrStr):
    """Download a file from S3, with progress bar"""
    # Get file size for progress bar
    s3 = _get_s3_client()
    head = s3.head_object(Bucket=bucket_name, Key=key)
    file_size = head['ContentLength']

    # Download file with a callback to periodically update progress
    progress, task = get_download_progress(file_size)
    with progress:
        s3.download_file(
            Bucket=bucket_name,
            Key=key,
            Filename=str(download_file),
            Callback=lambda n_bytes: progress.update(task, advance=n_bytes),
        )


def _get_s3_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    return boto3.client('s3', config=Config(signature_version=UNSIGNED))


def estimate_next_release(s3_mtime: datetime):
    """Get an estimate of time until next update. Updates are roughly monthly, but not necessarily
    on the same day each month.
    """
    elapsed = datetime.now(timezone.utc) - s3_mtime
    est_release_days = 31 - elapsed.days
    print(f'[cyan]Possible new release in ~[magenta]{est_release_days} [cyan]days')


# if __name__ == '__main__':
#     start = time()
#     download_metadata()
#     print(f'Finished in {time() - start:.2f} seconds')
