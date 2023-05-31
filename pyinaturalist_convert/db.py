"""ORM data models, which can be used to create and access tables in
`any database supported by SQLAlchemy <https://docs.sqlalchemy.org/en/14/dialects/>`_.
These contain a relevant subset of columns common to most iNat data sources,
suitable for combining data from API results, CSV export, DwC-A, and/or inaturalist-open-data.

Requirements for a relational database are highly variable, so this won't suit all use cases, but
at least provides a starting point.

**Extra dependencies**: ``sqlalchemy``

**Example**::

    >>> from pyinaturalist import iNatClient
    >>> from pyinaturalist_convert import create_tables, read_observations, save_observations

    >>> # Fetch all of your own observations
    >>> client = iNatClient()
    >>> observations = client.observations.search(user_id='my_username').all()

    >>> # Save to a SQLite database
    >>> create_tables('observations.db')
    >>> save_observations(observations, 'observations.db')

    >>> # Read them back from the database
    >>> for observation in get_db_observations('observations.db'):
    ...    print(observation)

**Main functions:**

.. autosummary::
    :nosignatures:

    create_tables
    get_db_observations
    get_db_taxa
    save_observations
    save_taxa

**Models:**

.. currentmodule:: pyinaturalist_convert._models

.. autosummary::
    :nosignatures:

    DbObservation
    DbPhoto
    DbTaxon
    DbUser
"""
# flake8: noqa: F401
# TODO: Abstraction for converting between DB models and attrs models
# TODO: Annotations and observation field values
# TODO: If needed, this could be done with just the stdlib sqlite3 and no SQLAlchemy
from itertools import chain
from logging import getLogger
from typing import TYPE_CHECKING, Iterable, Iterator, List, Optional

from pyinaturalist import Observation, Taxon

from .constants import DB_PATH, PathOrStr

# If SQLAlchemy isn't installed, don't raise ImportErrors at import time.
# DB Model classes require SA imports in module scope, so they're wrapped here.
try:
    from ._models import Base, DbObservation, DbPhoto, DbTaxon, DbUser
except ImportError:
    pass

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = getLogger(__name__)


def create_table(model, db_path: PathOrStr = DB_PATH):
    """Create a single table for the specified model, if it doesn't already exist"""
    from sqlalchemy import inspect

    engine = _get_engine(db_path)
    table = model.__tablename__
    if inspect(engine).has_table(table):
        logger.info(f'Table {table} already exists')
    else:
        model.__table__.create(engine)
        logger.info(f'Table {table} created')


def create_tables(db_path: PathOrStr = DB_PATH):
    """Create all tables in a SQLite database"""
    engine = _get_engine(db_path)
    Base.metadata.create_all(engine)


def get_session(db_path: PathOrStr = DB_PATH) -> 'Session':
    """Get a SQLAlchemy session for a SQLite database"""
    from sqlalchemy.orm import Session

    return Session(_get_engine(db_path), future=True)


def _get_engine(db_path):
    from sqlalchemy import create_engine

    return create_engine(f'sqlite:///{db_path}')


def get_db_observations(
    db_path: PathOrStr = DB_PATH,
    ids: Optional[Iterable[int]] = None,
    username: Optional[str] = None,
    limit: Optional[int] = None,
    page: Optional[int] = None,
    order_by_created: bool = False,
    order_by_observed: bool = False,
) -> Iterator[Observation]:
    """Load observation records and associated taxa from SQLite"""
    from sqlalchemy import desc, select

    stmt = (
        select(DbObservation)
        .join(DbObservation.taxon, isouter=True)
        .join(DbObservation.user, isouter=True)
    )
    if ids:
        stmt = stmt.where(DbObservation.id.in_(list(ids)))  # type: ignore
    if username:
        stmt = stmt.where(DbUser.login == username)
    if limit:
        stmt = stmt.limit(limit)
    if limit and page and page > 1:
        stmt = stmt.offset((page - 1) * limit)
    if order_by_created:
        stmt = stmt.order_by(desc(DbObservation.created_at))
    elif order_by_observed:
        stmt = stmt.order_by(desc(DbObservation.observed_on))

    with get_session(db_path) as session:
        for obs in session.execute(stmt):
            yield obs[0].to_model()


def get_db_taxa(
    db_path: PathOrStr = DB_PATH,
    ids: Optional[List[int]] = None,
    accept_partial: bool = True,
    limit: int = 200,
) -> Iterator[Taxon]:
    """Load taxon records from SQLite"""
    from sqlalchemy import select

    stmt = select(DbTaxon)
    if ids:
        stmt = stmt.where(DbTaxon.id.in_(ids))  # type: ignore
    if not accept_partial:
        stmt = stmt.where(DbTaxon.partial == False)
    if limit:
        stmt = stmt.limit(limit)

    with get_session(db_path) as session:
        for taxon in session.execute(stmt):
            yield taxon[0].to_model()


def save_observations(observations: Iterable[Observation], db_path: PathOrStr = DB_PATH):
    """Save Observation objects (and associated taxa and photos) to SQLite"""
    with get_session(db_path) as session:
        for observation in observations:
            session.merge(DbObservation.from_model(observation, skip_taxon=True))
        session.commit()

    save_taxa([obs.taxon for obs in observations if obs.taxon], db_path)


def save_taxa(taxa: Iterable[Taxon], db_path: PathOrStr = DB_PATH):
    """Save Taxon objects (plus ancestors and children, if available) to SQLite"""
    from sqlalchemy import select

    # Combined list of taxa plus all their unique ancestors + children
    taxa_by_id = {t.id: t for t in chain.from_iterable([t.ancestors + t.children for t in taxa])}
    taxa_by_id.update({t.id: t for t in taxa})
    unique_taxon_ids = list(taxa_by_id.keys())

    with get_session(db_path) as session:
        stmt = select(DbTaxon).where(DbTaxon.id.in_(unique_taxon_ids))  # type: ignore
        existing_taxa = {t[0].id: t[0] for t in session.execute(stmt)}

        # Merge (instead of overwriting) any existing taxa with new data
        for taxon in taxa_by_id.values():
            if db_taxon := existing_taxa.get(taxon.id):
                db_taxon.update(taxon)
            else:
                session.add(DbTaxon.from_model(taxon))

        session.commit()
