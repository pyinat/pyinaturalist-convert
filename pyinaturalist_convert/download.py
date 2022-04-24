"""Functions for downloading and extracting inaturalist open data"""
# TODO: Download separate archive files in parallel?
from datetime import datetime
from os.path import basename
from pathlib import Path
from tarfile import TarFile
from typing import Optional

from .constants import PathOrStr
from .progress import get_download_progress


class FlatTarFile(TarFile):
    """Extracts all archive contents to a flat base directory, ignoring archive subdirectories"""

    def extract(self, member, path="", **kwargs):
        if member.isfile():
            member.name = basename(member.name)
            super().extract(member, path, **kwargs)


def get_file_mtime(file: Path) -> Optional[datetime]:
    """Get the modified time of a file, as a timezone-aware datetime"""
    if not file.exists:
        return None
    file_mtime = file.stat().st_mtime
    return datetime.fromtimestamp(file_mtime).astimezone()


def get_s3_mtime(bucket_name: str, key: str) -> datetime:
    """Get the modified time of an S3 file"""
    s3 = _get_s3_client()
    head = s3.head_object(Bucket=bucket_name, Key=key)
    return head['LastModified']


def download_s3_file(bucket_name: str, key: str, download_file: PathOrStr):
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


# if __name__ == '__main__':
#     start = time()
#     download_metadata()
#     print(f'Finished in {time() - start:.2f} seconds')
