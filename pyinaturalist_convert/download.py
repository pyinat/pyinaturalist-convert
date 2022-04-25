"""Functions for downloading and extracting archives, with progress bars"""
# TODO: Download separate archive files in parallel?
# TODO: Make progress bar optional
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from io import FileIO
from os.path import basename, getsize
from pathlib import Path
from shutil import copyfileobj
from tarfile import TarFile
from typing import Optional, Tuple
from zipfile import ZipFile

import requests
from rich import print
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from .constants import PathOrStr

ProgressTask = Tuple[Progress, TaskID]


class FlatTarFile(TarFile):
    """Extracts all archive contents to a flat base directory, ignoring archive subdirectories"""

    def extract(self, member, path="", **kwargs):
        if member.isfile():
            member.name = basename(member.name)
            super().extract(member, path, **kwargs)


class ProgressIO(FileIO):
    """File object wrapper that updates progress on read and write"""

    def __init__(self, path, *args, total_size: int = 0, description: str = 'Extracting', **kwargs):
        self._total_size = total_size or getsize(path)
        self.progress, self.task = _get_progress(self._total_size, description)
        super().__init__(path, *args, **kwargs)

    def read(self, size):
        self.progress.update(self.task, advance=size)
        return super().read(size)

    def write(self, b):
        self.progress.update(self.task, advance=len(b))
        return super().write(b)


def check_download(
    dest_file: Path,
    url: str = None,
    bucket: str = None,
    key: str = None,
    release_interval: int = None,
) -> bool:
    """Check if a locally downloaded file exists and is up to date"""
    if not dest_file.exists():
        return False

    # Get remote timestamp from either URL or S3
    remote_mtime = get_s3_mtime(bucket, key) if bucket and key else get_url_mtime(url or '')
    local_mtime = get_file_mtime(dest_file)

    if remote_mtime is not None and local_mtime >= remote_mtime:
        print(f'[cyan]File already exists and is up to date:[/cyan] {dest_file}')
        estimate_next_release(remote_mtime, release_interval)
        return True
    else:
        print(f'[cyan]File exists, but is out of date:[/cyan] {dest_file}')
        return False


def estimate_next_release(remote_mtime: datetime, release_interval: int = None):
    """Get estimated time until the next update"""
    if not release_interval:
        return
    elapsed = datetime.now(timezone.utc) - remote_mtime
    est_release_days = max(release_interval - elapsed.days, 1)
    print(f'[cyan]Possible new release in ~[magenta]{est_release_days}[cyan] days')


def get_file_mtime(file: Path) -> datetime:
    """Get the modified time of a file, as a timezone-aware datetime"""
    file_mtime = file.stat().st_mtime
    return datetime.fromtimestamp(file_mtime).astimezone()


def get_s3_mtime(bucket_name: str, key: str) -> datetime:
    """Get the modified time of an S3 file"""
    s3 = _get_s3_client()
    head = s3.head_object(Bucket=bucket_name, Key=key)
    return head['LastModified']


def get_url_mtime(url: str) -> Optional[datetime]:
    """Get the modified time of a URL, as a timezone-aware datetime"""
    response = requests.head(url)
    remote_last_modified = _parse_http_date(response.headers.get('Last-Modified', ''))
    # Assume UTC if no timezone is specified
    if remote_last_modified and not remote_last_modified.tzinfo:
        remote_last_modified = remote_last_modified.replace(tzinfo=timezone.utc)
    return remote_last_modified


def download_file(url: str, dest_file: PathOrStr):
    """Download a file from a URL, with progress bar"""
    # Get file size for progress bar
    response = requests.head(url)
    file_size = int(response.headers['Content-Length'])
    progress, task = _get_progress(file_size)
    print(f'Downloading to: {dest_file}')

    with progress, requests.get(url, stream=True) as response, open(dest_file, 'wb') as dl:
        for chunk in response.iter_content(chunk_size=4096):
            if not chunk:
                continue
            dl.write(chunk)
            progress.update(task, advance=len(chunk))


def download_s3_file(bucket_name: str, key: str, dest: PathOrStr):
    """Download a file from S3, with progress bar"""
    # Get file size for progress bar
    s3 = _get_s3_client()
    head = s3.head_object(Bucket=bucket_name, Key=key)
    file_size = head['ContentLength']
    progress, task = _get_progress(file_size)
    print(f'Downloading to: {dest}')

    # Download file with a callback to periodically update progress
    with progress:
        s3.download_file(
            Bucket=bucket_name,
            Key=key,
            Filename=str(dest),
            Callback=lambda n_bytes: progress.update(task, advance=n_bytes),
        )


def unzip_progress(archive_path: Path, dest_dir: Path):
    """Extract a zip file with progress"""
    # total = sum(getattr(member, "file_size", 0) for member in archive.infolist())
    # progress = get_download_progress(total)
    dest_dir.mkdir(parents=True, exist_ok=True)
    with ZipFile(archive_path) as archive:
        for member in archive.infolist():
            progress_file = ProgressIO(
                dest_dir / member.filename,
                'wb',
                total_size=getattr(member, "file_size", 0),
                description=f'Extracting {member.filename:>16}',
            )
            with archive.open(member) as f, progress_file.progress:
                copyfileobj(f, progress_file)


def untar_progress(archive_path: Path, dest_dir: Path):
    """Extract a tar file with progress"""
    dest_dir.mkdir(parents=True, exist_ok=True)
    progress_file = ProgressIO(archive_path)
    with FlatTarFile.open(fileobj=progress_file) as archive, progress_file.progress:
        archive.extractall(path=dest_dir)


def _get_progress(total: int, description: str = 'Downloading') -> ProgressTask:
    progress = Progress(
        '[progress.description]{task.description}',
        BarColumn(),
        '[progress.percentage]{task.percentage:>3.0f}%',
        TransferSpeedColumn(),
        DownloadColumn(),
        TimeRemainingColumn(),
    )
    return progress, _get_task(progress, total, description)


def _get_task(progress, total: Optional[int], description: str) -> TaskID:
    return progress.add_task(f'[cyan]{description}...', total=total)


def _get_s3_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    return boto3.client('s3', config=Config(signature_version=UNSIGNED))


def _parse_http_date(value: str) -> Optional[datetime]:
    """Attempt to parse an HTTP (RFC 5322-compatible) timestamp"""
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
