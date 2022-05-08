"""ORM data models. These contain a relevant subset of columns common to most iNat data sources,
suitable for combining data from API results, CSV export, DwC-A, and/or inaturalist-open-data.
"""
# TODO: Abstraction for converting between DB models and attrs models
# TODO: Annotations and observation field values
from datetime import datetime
from typing import List, Optional

from pyinaturalist import Observation, Photo, Taxon, User
from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, select

from pyinaturalist_convert.constants import PathOrStr


class DbObservation(SQLModel, table=True):
    __tablename__ = 'observation'

    id: int = Field(primary_key=True)
    captive: Optional[bool] = None
    description: Optional[str] = None
    geoprivacy: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    observed_on: Optional[datetime] = Field(default=None, index=True)
    place_guess: Optional[str] = None
    place_ids: Optional[str] = None
    positional_accuracy: Optional[int] = None
    quality_grade: Optional[str] = None
    taxon_id: Optional[int] = Field(default=None, foreign_key='taxon.id')
    user_id: Optional[int] = None
    uuid: Optional[str] = None

    photos: List['DbPhoto'] = Relationship(back_populates='observation')
    taxon: Optional['DbTaxon'] = Relationship()

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
            taxon_id=getattr(observation.taxon, 'id', None),
            user_id=getattr(observation.user, 'id', None),
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
            # photos=[p.to_photo() for p in self.photos],
            taxon=self.taxon.to_taxon() if self.taxon else None,
            user=User(id=self.user_id),
            uuid=self.uuid,
        )


class DbTaxon(SQLModel, table=True):
    __tablename__ = 'taxon'

    id: int = Field(primary_key=True)
    active: Optional[bool] = None
    ancestor_ids: Optional[str] = None  # Comma-delimited ancestor IDs
    iconic_taxon_id: Optional[int] = None
    name: Optional[str] = Field(default=None, index=True)
    parent_id: Optional[int] = None
    preferred_common_name: Optional[str] = None
    rank: Optional[str] = None
    default_photo_url: Optional[str] = None

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
class DbPhoto(SQLModel, table=True):
    __tablename__ = 'photo'

    id: int = Field(primary_key=True)
    uuid: Optional[str] = None
    observation_id: Optional[int] = Field(default=None, foreign_key='observation.id')
    # observation_uuid: Optional[str] = Field(default=None, foreign_key='observation.uuid')
    user_id: Optional[int] = None
    extension: Optional[str] = None
    license: Optional[str] = None
    url: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None

    observation: DbObservation = Relationship(back_populates='photos')

    @classmethod
    def from_photo(cls, photo: Photo) -> 'DbPhoto':
        return cls(id=photo.id, license=photo.license_code, uuid=photo.uuid, url=photo.url)

    def to_photo(self) -> Photo:
        return Photo(
            id=self.id,
            license_code=self.license,
            uuid=self.uuid,
            url=self.url,
            user=User(id=self.user_id),
        )


def create_tables(db_path: str):
    engine = create_engine(f'sqlite:///{db_path}')
    SQLModel.metadata.create_all(engine)


def get_session(db_path: PathOrStr) -> Session:
    return Session(create_engine(f'sqlite:///{db_path}'))


def save_observations(*observations: Observation):
    session = get_session('observations.db')
    for observation in observations:
        session.add(DbObservation.from_observation(observation))
    session.commit()


def get_observations():
    session = get_session('observations.db')
    results = session.exec(select(DbObservation).join(DbTaxon, isouter=True))
    for obs in results:
        yield obs.to_observation()


def _split_ids(ids_str: str = None) -> List[int]:
    return [int(i) for i in ids_str.split(',')] if ids_str else []


def _join_ids(ids: List[int] = None) -> str:
    return ','.join(map(str, ids)) if ids else ''
