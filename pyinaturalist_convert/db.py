"""ORM data models. These contain a relevant subset of columns common to most iNat data sources,
suitable for combining data from API results, CSV export, DwC-A, and/or inaturalist-open-data.
"""
from dataclasses import dataclass, field

# TODO: Abstraction for converting between DB models and attrs models
# TODO: Annotations and observation field values
from datetime import datetime
from typing import List

from pyinaturalist import Observation, Photo, Taxon, User
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
    select,
)
from sqlalchemy.orm import Session, registry, relationship

from pyinaturalist_convert.constants import PathOrStr

Base = registry()


def sa_field(col_type, index: bool = False, primary_key: bool = False, **kwargs):
    """Get a dataclass field with SQLAlchemy column metadata"""
    column = Column(col_type, index=index, primary_key=primary_key)
    return field(**kwargs, metadata={'sa': column})


@Base.mapped
@dataclass
class DbObservation:
    __tablename__ = 'observation'
    __sa_dataclass_metadata_key__ = 'sa'

    id: int = sa_field(Integer, primary_key=True)
    captive: bool = sa_field(Boolean, default=None)
    description: str = sa_field(String, default=None)
    geoprivacy: str = sa_field(String, default=None)
    latitude: float = sa_field(Float, default=None)
    longitude: float = sa_field(Float, default=None)
    observed_on: datetime = sa_field(DateTime, default=None, index=True)
    place_guess: str = sa_field(String, default=None)
    place_ids: str = sa_field(String, default=None)
    positional_accuracy: int = sa_field(Integer, default=None)
    quality_grade: str = sa_field(String, default=None, index=True)
    taxon_id: int = sa_field(ForeignKey('taxon.id'), default=None)
    user_id: int = sa_field(Integer, default=None)
    uuid: str = sa_field(String, default=None, index=True)

    photos = relationship('DbPhoto', back_populates='observation')  # type: ignore
    taxon = relationship('DbTaxon', back_populates='observations')  # type: ignore

    # Column aliases for inaturalist-open-data
    # observation_uuid: str = synonym('uuid')  # type: ignore
    # observer_id: int = synonym('user_id')  # type: ignore

    @classmethod
    def from_observation(cls, observation: Observation) -> 'DbObservation':
        return cls(
            id=observation.id,
            captive=observation.captive,
            latitude=observation.location[0] if observation.location else None,
            longitude=observation.location[1] if observation.location else None,
            observed_on=observation.observed_on,
            place_guess=observation.place_guess,
            place_ids=_join_ids(observation.place_ids),
            positional_accuracy=observation.positional_accuracy,
            quality_grade=observation.quality_grade,
            taxon_id=observation.taxon.id if observation.taxon else None,
            user_id=observation.user.id if observation.user else None,
            uuid=observation.uuid,
        )

    def to_observation(self) -> Observation:
        return Observation(
            id=self.id,
            captive=self.captive,
            location=(self.latitude, self.longitude),
            observed_on=self.observed_on,
            place_guess=self.place_guess,
            place_ids=_split_ids(self.place_ids),
            positional_accuracy=self.positional_accuracy,
            quality_grade=self.quality_grade,
            photos=[p.to_photo() for p in self.photos],
            taxon=self.taxon.to_taxon() if self.taxon else None,
            user=User(id=self.user_id),
            uuid=self.uuid,
        )


@Base.mapped
@dataclass
class DbTaxon:
    __tablename__ = 'taxon'
    __sa_dataclass_metadata_key__ = 'sa'

    id: int = sa_field(Integer, primary_key=True)
    active: bool = sa_field(Boolean, default=None)
    ancestor_ids: str = sa_field(String, default=None)
    iconic_taxon_id: int = sa_field(Integer, default=None)
    name: str = sa_field(String, default=None, index=True)
    parent_id: int = sa_field(String, default=None)
    preferred_common_name: str = sa_field(String, default=None)
    rank: str = sa_field(String, default=None)
    default_photo_url: str = sa_field(String, default=None)

    # ancestry: str = synonym('ancestor_ids')  # type: ignore

    observations = relationship('DbObservation', back_populates='taxon')  # type: ignore

    @classmethod
    def from_taxon(cls, taxon: Taxon) -> 'DbTaxon':
        return cls(
            id=taxon.id,
            active=taxon.is_active,
            ancestor_ids=_join_ids(taxon.ancestor_ids),
            iconic_taxon_id=taxon.iconic_taxon_id,
            name=taxon.name,
            parent_id=taxon.parent_id,
            preferred_common_name=taxon.preferred_common_name,
            rank=taxon.rank,
            default_photo_url=taxon.default_photo.url,
        )

    def to_taxon(self) -> Taxon:
        return Taxon(
            id=self.id,
            ancestor_ids=_split_ids(self.ancestor_ids),
            iconic_taxon_id=self.iconic_taxon_id,
            is_active=self.active,
            name=self.name,
            parent_id=self.parent_id,
            preferred_common_name=self.preferred_common_name,
            rank=self.rank,
            default_photo=Photo(url=self.default_photo_url),
        )


# TODO: Combine observation_id/uuid into one column?


@Base.mapped
@dataclass
class DbPhoto:
    __tablename__ = 'photo'
    __sa_dataclass_metadata_key__ = 'sa'

    id: int = sa_field(Integer, primary_key=True)
    uuid: str = sa_field(String, default=None)
    observation_id: int = sa_field(ForeignKey('observation.id'), default=None)
    # observation_uuid: Optional[str] = Field(default=None, foreign_key='observation.uuid')
    user_id: int = sa_field(Integer, default=None)
    extension: str = sa_field(String, default=None)
    license: str = sa_field(String, default=None)
    url: str = sa_field(String, default=None)
    width: int = sa_field(Integer, default=None)
    height: int = sa_field(Integer, default=None)

    observation = relationship('DbObservation', back_populates='photos')  # type: ignore

    @classmethod
    def from_photo(cls, photo: Photo, **kwargs) -> 'DbPhoto':
        return cls(
            id=photo.id,
            license=photo.license_code,
            url=photo.url,
            **kwargs,
            # uuid=photo.uuid,
        )

    def to_photo(self) -> Photo:
        return Photo(
            id=self.id,
            license_code=self.license,
            url=self.url,
            # user=User(id=self.user_id),
            # uuid=self.uuid,
        )


def create_tables(db_path: str):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)


def get_session(db_path: PathOrStr = 'observations.db') -> Session:
    engine = create_engine(f'sqlite:///{db_path}', future=True)
    return Session(engine, future=True)


def save_observations(*observations: Observation, db_path: PathOrStr = 'observations.db'):
    with get_session(db_path) as session:
        for observation in observations:
            session.add(DbObservation.from_observation(observation))
            session.add(DbTaxon.from_taxon(observation.taxon))
            for photo in observation.photos:
                session.add(
                    DbPhoto.from_photo(
                        photo,
                        observation_id=observation.id,
                        user_id=observation.user.id,
                    )
                )
        session.commit()


def read_observations(db_path: PathOrStr = 'observations.db'):
    stmt = select(DbObservation).join(DbObservation.taxon, isouter=True).join(DbObservation.photos)
    with get_session(db_path) as session:
        for obs in session.execute(stmt):
            yield obs[0].to_observation()


def _split_ids(ids_str: str = None) -> List[int]:
    return [int(i) for i in ids_str.split(',')] if ids_str else []


def _join_ids(ids: List[int] = None) -> str:
    return ','.join(map(str, ids)) if ids else ''
