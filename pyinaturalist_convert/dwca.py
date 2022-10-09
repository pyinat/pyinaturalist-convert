"""Download and convert the
`iNaturalist GBIF and taxonomy datasets <https://www.inaturalist.org/pages/developers>`_
from DwC-A to SQLite.

**Extra dependencies**: ``sqlalchemy``

**Example**: Download everything and load into a SQLite database::

    >>> from pyinaturalist_convert import load_dwca_tables
    >>> load_dwca_tables()

.. note::
    By default, data is saved in the recommended platform-specific data directory, for example
    ``~\\AppData\\Local\\`` on Windows, or ``~/.local/share/`` on Linux. Use the ``db_path``
    argument to use a different location.

**Main functions:**

.. autosummary::
    :nosignatures:

    load_dwca_tables
    load_dwca_observations
    load_dwca_taxa
"""
# TODO: Lookup and replace user_login with user_id
# TODO: Translate DwC lifeStage and sex to iNat annotations
import sqlite3
import subprocess
from logging import getLogger
from os.path import basename, splitext
from pathlib import Path
from typing import Dict, List

from pyinaturalist.constants import DATA_DIR

from .constants import DB_PATH, DWCA_OBS_CSV, DWCA_TAXA_URL, DWCA_TAXON_CSV, DWCA_URL, PathOrStr
from .db import DbTaxon, create_table, create_tables
from .download import (
    CSVProgress,
    check_download,
    download_file,
    get_progress_spinner,
    unzip_progress,
)
from .dwc import get_dwc_lookup
from .sqlite import load_table, vacuum_analyze

OBS_COLUMNS = [
    'catalogNumber',
    'captive',
    'coordinateUncertaintyInMeters',
    'decimalLatitude',
    'decimalLongitude',
    'eventDate',
    'inaturalistLogin',
    'informationWithheld',
    'modified',
    'occurrenceRemarks',
    'taxonID',
]
TAXON_COLUMN_MAP = {
    'id': 'id',
    'parentNameUsageID': 'parent_id',
    'references': 'reference_url',
    'scientificName': 'name',
    'taxonRank': 'rank',
}
TAXON_TABLE = 'taxon'
OBS_TABLE = 'observation'

logger = getLogger(__name__)


def load_dwca_tables(db_path: PathOrStr = DB_PATH):
    """Download observation and taxonomy archives and load into a SQLite database.

    As of 2022-05, this will require about 42GB of free disk space while loading, and the final
    database will be around 8GB.

    Args:
        db_path: Path to SQLite database
    """
    download_dwca_observations()
    download_dwca_taxa()
    with CSVProgress(DWCA_OBS_CSV, DWCA_TAXON_CSV) as progress:
        load_dwca_observations(db_path=db_path, progress=progress)
        load_dwca_taxa(db_path=db_path, progress=progress)
    vacuum_analyze(['observation', 'taxon'], db_path, show_spinner=True)


def download_dwca_observations(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the DwC-A research-grade observations dataset. Reuses local data if it
    already exists and is up to date.

    Example to load into a SQLite database (using the `sqlite3` shell, from bash):

    .. code-block:: bash

        export DATA_DIR="$HOME/.local/share/pyinaturalist"
        sqlite3 -csv $DATA_DIR/observations.db ".import $DATA_DIR/gbif-observations-dwca/observations.csv observations"

    Args:
        dest_dir: Alternative download directory
    """
    _download_archive(DWCA_URL, dest_dir)


def download_dwca_taxa(dest_dir: PathOrStr = DATA_DIR):
    """Download and extract the DwC-A taxonomy dataset. Reuses local data if it already exists and
    is up to date.

    Args:
        dest_dir: Alternative download directory
    """
    _download_archive(DWCA_TAXA_URL, dest_dir)


def load_dwca_observations(
    csv_path: PathOrStr = DWCA_OBS_CSV,
    db_path: PathOrStr = DB_PATH,
    progress: CSVProgress = None,
):
    """Create or update an observations SQLite table from the GBIF DwC-A archive. This keeps only the most
    relevant subset of columns available in the archive, in a format consistent with API results and
    other sources.

    To load everything as-is, see :py:func:`.load_full_dwca_observations`.
    """
    create_tables(db_path)
    column_map = _get_obs_column_map(OBS_COLUMNS)
    progress = progress or CSVProgress(csv_path)
    with progress:
        load_table(csv_path, db_path, 'observation', column_map, progress=progress)
    _cleanup_observations(db_path)


def load_full_dwca_observations(
    csv_path: PathOrStr = DWCA_OBS_CSV,
    db_path: PathOrStr = DB_PATH,
):
    """Create an observations SQLite table from the GBIF DwC-A archive, using all columns exactly
    as they appear in the archive.

    This requires the ``sqlite3`` executable to be installed on the system, since its ``.import``
    command is by far the fastest way to load this.
    """
    logger.info(f'Loading {csv_path} into {db_path}')
    subprocess.run(f'sqlite3 -csv {db_path} ".import {csv_path} observation"', shell=True)


def load_dwca_taxa(
    csv_path: PathOrStr = DWCA_TAXON_CSV,
    db_path: PathOrStr = DB_PATH,
    column_map: Dict = TAXON_COLUMN_MAP,
    progress: CSVProgress = None,
):
    """Create or update a taxonomy SQLite table from the GBIF DwC-A archive"""
    create_table(DbTaxon, db_path)

    def get_parent_id(row: Dict):
        """Get parent taxon ID from URL"""
        try:
            row['parentNameUsageID'] = int(row['parentNameUsageID'].split('/')[-1])
        except (KeyError, TypeError, ValueError):
            row['parentNameUsageID'] = None
        return row

    progress = progress or CSVProgress(csv_path)
    with progress:
        load_table(
            csv_path, db_path, 'taxon', column_map, transform=get_parent_id, progress=progress
        )
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE taxon SET parent_id=NULL WHERE parent_id=''")


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


def _cleanup_observations(db_path: PathOrStr = DB_PATH):
    """Run the following post-processing steps after loading observations:
    * Translate dwc:informationWithheld into standard geoprivacy values
    * Translate captive values into True/False
    * Vacuum/analyze
    """
    spinner = get_progress_spinner('Post-processing')
    with spinner, sqlite3.connect(db_path) as conn:
        logger.info('Finding observations with open geoprivacy')
        conn.execute("UPDATE observation SET geoprivacy='open' " "WHERE geoprivacy IS NULL")

        logger.info('Finding observations with obscured geoprivacy')
        conn.execute(
            "UPDATE observation SET geoprivacy='obscured' "
            "WHERE geoprivacy LIKE 'Coordinate uncertainty increased%'"
        )

        logger.info('Finding observations with private geoprivacy')
        conn.execute(
            "UPDATE observation SET geoprivacy='private' "
            "WHERE geoprivacy LIKE 'Coordinates hidden%'"
        )

        logger.info('Formatting captive/wild status')
        conn.execute("UPDATE observation SET captive=FALSE WHERE captive='wild'")
        conn.execute("UPDATE observation SET captive=TRUE WHERE captive IS NOT FALSE")


def _get_obs_column_map(fields: List[str]) -> Dict[str, str]:
    """Translate subset of DwC terms to API-compatible field names"""
    lookup = {k.split(':')[-1]: v.replace('.', '_') for k, v in get_dwc_lookup().items()}
    return {field: lookup[field] for field in fields}
