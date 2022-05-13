"""Functions for downloading and extracting archives, with progress bars"""
# TODO: Make progress bar optional
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from io import FileIO
from os.path import basename, getsize
from pathlib import Path
from shutil import copyfileobj
from tarfile import TarFile
from typing import Callable, Dict, Iterable, Optional, Tuple
from zipfile import ZipFile

import requests
from rich import print
from rich.live import Live
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table

from .constants import PathOrStr

ProgressTask = Tuple[Progress, TaskID]


class FlatTarFile(TarFile):
    """Extracts all archive contents to a flat base directory, ignoring archive subdirectories"""

    def extract(self, member, path="", **kwargs):
        if member.isfile():
            member.name = basename(member.name)
            super().extract(member, path, **kwargs)


class ProgressIO(FileIO):
    """File object wrapper that updates progress on read and write.

    Args:
        path: Path to file
        callback: Progress callback function; a new one will be created if not provided
        description: Description of the task being performed
    """

    def __init__(
        self, path, *args, callback: Callable = None, description: str = 'Extracting', **kwargs
    ):
        if callback:
            self.callback = callback
        else:
            self.progress = get_progress_dl()
            task = _get_task(self.progress, getsize(path), description)
            self.callback = lambda x: self.progress.advance(task, x)
        super().__init__(path, *args, **kwargs)

    def read(self, size):
        self.callback(size)
        return super().read(size)

    def write(self, b):
        self.callback(len(b))
        return super().write(b)


class MultiProgress:
    """Track progress of multiple processes, plus overall combined progress"""

    def __init__(
        self,
        totals: Dict[str, int],
        total_progress: Progress = None,
        job_progress: Progress = None,
        task_description: str = 'Loading',
    ):
        self.total_progress = total_progress or get_progress()
        self.total_task = self.total_progress.add_task('[cyan]Total', total=sum(totals.values()))
        self.job_progress = job_progress or get_progress()
        self.job_task = self.job_progress.add_task('[cyan]File ')

        self.table = Table.grid()
        self.table.add_row(self.total_progress)
        self.table.add_row(self.job_progress)
        self.task_description = task_description
        self.totals = totals
        self.live = Live(self.table, refresh_per_second=10)

    def __enter__(self):
        self.live.__enter__()
        return self

    def __exit__(self, *args):
        self.total_progress.tasks[0].completed = self.total_progress.tasks[0].total
        self.job_progress.tasks[0].completed = self.job_progress.tasks[0].total
        self.live.__exit__(*args)

    def start_job(self, name: PathOrStr):
        if isinstance(name, Path):
            name = _fname(name)
        self.job_progress.update(
            self.job_task,
            completed=0,
            total=self.totals[str(name)],
        )
        self.job_progress.log(f'[cyan]{self.task_description} [white]{name}[cyan]...')

    def advance(self, advance: int = 1):
        self.total_progress.advance(self.total_task, advance)
        self.job_progress.advance(self.job_task, advance)


class CSVProgress(MultiProgress):
    """Track progress of processing CSV files"""

    def __init__(self, *filenames: Path, **kwargs):
        super().__init__(totals=_get_csv_totals(filenames), **kwargs)


class ZipProgress(MultiProgress):
    """Track progress of extracting files from a zip archive"""

    def __init__(self, archive: ZipFile, **kwargs):
        super().__init__(
            total_progress=get_progress_dl(),
            job_progress=get_progress_dl(),
            totals=_get_zip_totals(archive),
            task_description='Extracting',
            **kwargs,
        )


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
    remote_mtime = _get_s3_mtime(bucket, key) if bucket and key else _get_url_mtime(url or '')
    local_mtime = _get_file_mtime(dest_file)

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


def download_file(url: str, dest_file: PathOrStr):
    """Download a file from a URL, with progress bar"""
    # Get file size for progress bar
    response = requests.head(url)
    file_size = int(response.headers['Content-Length'])
    progress = get_progress_dl()
    task = _get_task(progress, file_size, 'Downloading')
    progress.log(f'[cyan]Downloading to: {dest_file}')

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
    progress = get_progress_dl()
    task = _get_task(progress, file_size, 'Downloading')
    progress.log(f'[cyan]Downloading to:[/cyan] {dest}')

    # Download file with a callback to periodically update progress
    with progress:
        s3.download_file(
            Bucket=bucket_name,
            Key=key,
            Filename=str(dest),
            Callback=lambda n_bytes: progress.update(task, advance=n_bytes),
        )


def untar_progress(archive_path: Path, dest_dir: Path):
    """Extract a tar file with progress"""
    dest_dir.mkdir(parents=True, exist_ok=True)
    progress_file = ProgressIO(archive_path)
    with FlatTarFile.open(fileobj=progress_file) as archive, progress_file.progress:
        archive.extractall(path=dest_dir)


def unzip_progress(archive_path: Path, dest_dir: Path):
    """Extract a zip file with progress"""
    dest_dir.mkdir(parents=True, exist_ok=True)
    with ZipFile(archive_path) as archive, ZipProgress(archive) as progress:
        for member in archive.infolist():
            progress.start_job(member.filename)
            progress_file = ProgressIO(dest_dir / member.filename, 'wb', callback=progress.advance)
            with archive.open(member) as f:
                copyfileobj(f, progress_file)


def get_progress(**kwargs) -> Progress:
    """Default progress bar format"""
    return Progress(
        '[progress.description]{task.description}',
        BarColumn(),
        '[green]{task.completed}/{task.total}',
        '[progress.percentage]{task.percentage:>3.0f}%',
        TimeRemainingColumn(),
        **kwargs,
    )


def get_progress_dl(**kwargs) -> Progress:
    """Track progress of processing a file in bytes"""
    return Progress(
        '[progress.description]{task.description}',
        BarColumn(),
        '[progress.percentage]{task.percentage:>3.0f}%',
        TransferSpeedColumn(),
        DownloadColumn(),
        TimeRemainingColumn(),
        **kwargs,
    )


def get_progress_spinner(description: str = 'Loading') -> Progress:
    progress = Progress('[progress.description]{task.description}', SpinnerColumn(style='green'))
    _get_task(progress, total=None, description=description)
    return progress


def _count_lines(filename: PathOrStr) -> int:
    """Unbuffered file line counter
    Based on: https://stackoverflow.com/a/27518377/15592055
    """

    def iter_chunks(reader):
        while b := reader(1024 * 1024):
            yield b

    with open(filename, 'rb') as f:
        return sum(chunk.count(b'\n') for chunk in iter_chunks(f.raw.read))


def _fname(name: Path):
    return name.stem.split('-', 1)[-1]


def _get_file_mtime(file: Path) -> datetime:
    """Get the modified time of a file, as a timezone-aware datetime"""
    file_mtime = file.stat().st_mtime
    return datetime.fromtimestamp(file_mtime).astimezone()


def _get_s3_mtime(bucket_name: str, key: str) -> datetime:
    """Get the modified time of an S3 file"""
    s3 = _get_s3_client()
    head = s3.head_object(Bucket=bucket_name, Key=key)
    return head['LastModified']


def _get_url_mtime(url: str) -> Optional[datetime]:
    """Get the modified time of a URL, as a timezone-aware datetime"""
    response = requests.head(url)
    remote_last_modified = _parse_http_date(response.headers.get('Last-Modified', ''))
    # Assume UTC if no timezone is specified
    if remote_last_modified and not remote_last_modified.tzinfo:
        remote_last_modified = remote_last_modified.replace(tzinfo=timezone.utc)
    return remote_last_modified


def _get_csv_totals(filenames: Iterable[Path]):
    return {_fname(f): _count_lines(f) - 1 for f in filenames}


def _get_zip_totals(archive: ZipFile):
    return {member.filename: member.file_size for member in archive.infolist()}


def _get_s3_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    return boto3.client('s3', config=Config(signature_version=UNSIGNED))


def _get_task(progress, total: Optional[int], description: str) -> TaskID:
    return progress.add_task(f'[cyan]{description}...', total=total)


def _parse_http_date(value: str) -> Optional[datetime]:
    """Attempt to parse an HTTP (RFC 5322-compatible) timestamp"""
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
