"""Utilities for working with the iNat GBIF DwC archive"""
from logging import getLogger
from os.path import basename, splitext
from pathlib import Path
from typing import Dict

from .constants import DATA_DIR, DWCA_DIR, DWCA_TAXA_URL, DWCA_URL, PathOrStr
from .download import check_download, download_file, unzip_progress
from .sqlite import load_table

TAXON_COLUMN_MAP = {'id': 'id', 'scientificName': 'name', 'taxonRank': 'rank'}
# Other available fields:
# 'kingdom',
# 'phylum',
# 'class',
# 'order',
# 'family',
# 'genus',
# 'specificEpithet'
# 'infraspecificEpithet'
# 'taxonID',
# 'identifier',
# 'parentNameUsageID',
# 'modified',
# 'references',


logger = getLogger(__name__)


def download_dwca(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the GBIF DwC-A export. Reuses local data if it already exists and is
    up to date.

    Example to load into a SQLite database (using the `sqlite3` shell, from bash):

    .. code-block:: bash

        export DATA_DIR="$HOME/.local/share/pyinaturalist"
        sqlite3 -csv $DATA_DIR/observations.db ".import $DATA_DIR/gbif-observations-dwca/observations.csv observations"

    Args:
        dest_dir: Alternative download directory
    """
    _download_archive(DWCA_URL, dest_dir)


def download_taxa(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the DwC-A taxonomy export. Reuses local data if it already exists and is
    up to date.

    Args:
        dest_dir: Alternative download directory
    """
    _download_archive(DWCA_TAXA_URL, dest_dir)


def _download_archive(url: str, dest_dir: PathOrStr = DATA_DIR):
    dest_dir = Path(dest_dir).expanduser()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / basename(url)

    # Skip download if we're already up to date
    if check_download(dest_file, url=url, release_interval=7):
        return

    # Otherwise, download and extract files
    download_file(url, dest_file)
    unzip_progress(dest_file, dest_dir / splitext(basename(url))[0])


def load_taxonomy_table(
    csv_path: PathOrStr = DATA_DIR / 'inaturalist-taxonomy.dwca' / 'taxa.csv',
    db_path: PathOrStr = DATA_DIR / 'taxa.db',
    table_name: str = 'taxa',
    column_map: Dict = TAXON_COLUMN_MAP,
):
    """Create a taxonomy table from the GBIF DwC-A archive"""
    load_table(csv_path, db_path, table_name, column_map)
