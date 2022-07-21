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
# TODO: Abstraction for converting between DB models and attrs models
# TODO: Annotations and observation field values
# TODO: If needed, this could be done with just the stdlib sqlite3 and no SQLAlchemy
from itertools import chain
from logging import getLogger
from typing import TYPE_CHECKING, Iterable, Iterator, List

from pyinaturalist import Observation, Taxon

from .constants import DB_PATH, PathOrStr

# Is SQLAlchemy isn't installed, don't raise ImportErrors at import time.
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
    ids: List[int] = None,
    limit: int = 200,
) -> Iterator[Observation]:
    """Load observation records (and associated taxa and photos) from SQLite"""
    from sqlalchemy import select

    stmt = (
        select(DbObservation)
        .join(DbObservation.photos, isouter=True)  # type: ignore  # created at runtime
        .join(DbObservation.taxon, isouter=True)
        .join(DbObservation.user, isouter=True)
    )
    if ids:
        stmt = stmt.where(DbObservation.id.in_(ids))  # type: ignore
    if limit:
        stmt = stmt.limit(limit)

    with get_session(db_path) as session:
        for obs in session.execute(stmt):
            yield obs[0].to_model()


def get_db_taxa(
    db_path: PathOrStr = DB_PATH,
    ids: List[int] = None,
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
            session.merge(DbObservation.from_model(observation))
            session.merge(DbTaxon.from_model(observation.taxon))
            session.merge(DbUser.from_model(observation.user))
            for photo in observation.photos:
                session.merge(
                    DbPhoto.from_model(
                        photo, observation_id=observation.id, user_id=observation.user.id
                    )
                )

        session.commit()


def save_taxa(taxa: Iterable[Taxon], db_path: PathOrStr = DB_PATH):
    """Save Taxon objects (plus ancestors and children, if available) to SQLite"""
    from sqlalchemy import select

    with get_session(db_path) as session:
        for taxon in taxa:
            session.merge(DbTaxon.from_model(taxon))

        # Save ancestors and children (partial records), but don't overwrite any full records
        taxonomy = {t.id: t for t in chain.from_iterable([t.ancestors + t.children for t in taxa])}
        unique_taxon_ids = list(taxonomy.keys())
        stmt = select(DbTaxon).where(DbTaxon.id.in_(unique_taxon_ids))  # type: ignore
        stmt = stmt.where(DbTaxon.partial == False)
        saved_ids = [t[0].id for t in session.execute(stmt)]
        for taxon in taxonomy.values():
            if taxon.id not in saved_ids:
                session.merge(DbTaxon.from_model(taxon))

        session.commit()
