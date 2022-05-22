from dataclasses import dataclass, field
from datetime import datetime
from logging import getLogger
from typing import List

from pyinaturalist import Observation, Photo, Taxon, User
from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String
from sqlalchemy.orm import registry, relationship

Base = registry()
logger = getLogger(__name__)


def sa_field(col_type, index: bool = False, primary_key: bool = False, **kwargs):
    """Get a dataclass field with SQLAlchemy column metadata"""
    column = Column(col_type, index=index, primary_key=primary_key)
    return field(**kwargs, metadata={'sa': column})


@Base.mapped
@dataclass
class DbObservation:
    """Intermediate data model for persisting Observation data to a relational database

    Note: datetimes are stored as strings, since SQLAlchemy DateTime doesn't handle timezone offsets
    from SQLite.
    """

    __tablename__ = 'observation'
    __sa_dataclass_metadata_key__ = 'sa'

    id: int = sa_field(Integer, primary_key=True)
    captive: bool = sa_field(Boolean, default=None, index=True)
    description: str = sa_field(String, default=None)
    geoprivacy: str = sa_field(String, default=None, index=True)
    latitude: float = sa_field(Float, default=None)
    longitude: float = sa_field(Float, default=None)
    observed_on: datetime = sa_field(String, default=None, index=True)
    place_guess: str = sa_field(String, default=None)
    place_ids: str = sa_field(String, default=None)
    positional_accuracy: int = sa_field(Integer, default=None)
    quality_grade: str = sa_field(String, default=None, index=True)
    taxon_id: int = sa_field(ForeignKey('taxon.id'), default=None, index=True)
    updated_at: datetime = sa_field(String, default=None, index=True)
    user_id: int = sa_field(ForeignKey('user.id'), default=None)
    user_login: int = sa_field(Integer, default=None)
    uuid: str = sa_field(String, default=None, index=True)

    taxon = relationship('DbTaxon', backref='observations')  # type: ignore
    user = relationship('DbUser', backref='observations')  # type: ignore

    # Column aliases for inaturalist-open-data
    # observation_uuid: str = synonym('uuid')  # type: ignore
    # observer_id: int = synonym('user_id')  # type: ignore

    @classmethod
    def from_model(cls, observation: Observation) -> 'DbObservation':
        return cls(
            id=observation.id,
            captive=observation.captive,
            latitude=observation.location[0] if observation.location else None,
            longitude=observation.location[1] if observation.location else None,
            observed_on=observation.observed_on.isoformat() if observation.observed_on else None,
            place_guess=observation.place_guess,
            place_ids=_join_ids(observation.place_ids),
            positional_accuracy=observation.positional_accuracy,
            quality_grade=observation.quality_grade,
            taxon_id=observation.taxon.id if observation.taxon else None,
            updated_at=observation.updated_at.isoformat() if observation.updated_at else None,
            user_id=observation.user.id if observation.user else None,
            uuid=observation.uuid,
        )

    def to_model(self) -> Observation:
        return Observation(
            id=self.id,
            captive=self.captive,
            location=(self.latitude, self.longitude),
            observed_on=self.observed_on,
            place_guess=self.place_guess,
            place_ids=_split_ids(self.place_ids),
            positional_accuracy=self.positional_accuracy,
            quality_grade=self.quality_grade,
            photos=[p.to_model() for p in self.photos],  # type: ignore
            taxon=self.taxon.to_model() if self.taxon else None,
            updated_at=self.updated_at,
            user=User(id=self.user_id),
            uuid=self.uuid,
        )


@Base.mapped
@dataclass
class DbTaxon:
    """Intermediate data model for persisting Taxon data to a relational database.

    Since different data sources provide different levels of detail, a ``partial`` field is added
    that indicates that some fields are missing, and can be fetched from the API if needed. As an
    example, this helps distinguish between taxa with no children and taxa with unlisted children.
    """

    __tablename__ = 'taxon'
    __sa_dataclass_metadata_key__ = 'sa'

    id: int = sa_field(Integer, primary_key=True)
    active: bool = sa_field(Boolean, default=None)
    ancestor_ids: str = sa_field(String, default=None)
    child_ids: str = sa_field(String, default=None)
    count: int = sa_field(Integer, default=0)
    iconic_taxon_id: int = sa_field(Integer, default=None)
    name: str = sa_field(String, default=None, index=True)
    parent_id: int = sa_field(ForeignKey('taxon.id'), default=None, index=True)
    partial: int = sa_field(Boolean, default=False)
    preferred_common_name: str = sa_field(String, default=None)
    rank: str = sa_field(String, default=None)
    default_photo_url: str = sa_field(String, default=None)

    @classmethod
    def from_model(cls, taxon: Taxon) -> 'DbTaxon':
        return cls(
            id=taxon.id,
            active=taxon.is_active,
            ancestor_ids=_join_ids(taxon.ancestor_ids),
            child_ids=_join_ids(taxon.child_ids),
            iconic_taxon_id=taxon.iconic_taxon_id,
            name=taxon.name,
            parent_id=taxon.parent_id,
            partial=taxon._partial,
            preferred_common_name=taxon.preferred_common_name,
            rank=taxon.rank,
            default_photo_url=taxon.default_photo.url,
        )

    def to_model(self) -> Taxon:
        return Taxon(
            id=self.id,
            ancestors=self._get_taxa(self.ancestor_ids),
            children=self._get_taxa(self.child_ids),
            default_photo=Photo(url=self.default_photo_url),
            iconic_taxon_id=self.iconic_taxon_id,
            is_active=self.active,
            name=self.name,
            parent_id=self.parent_id,
            partial=self.partial,
            preferred_common_name=self.preferred_common_name,
            rank=self.rank,
        )

    def _get_taxa(self, id_str: str) -> List[Taxon]:
        return [Taxon(id=id, partial=True) for id in _split_ids(id_str)]


# TODO: Combine observation_id/uuid into one column? Or two separate foreign keys?
@Base.mapped
@dataclass
class DbPhoto:
    """Intermediate data model for persisting Photo metadata to a relational database"""

    __tablename__ = 'photo'
    __sa_dataclass_metadata_key__ = 'sa'

    id: int = sa_field(Integer, primary_key=True)
    extension: str = sa_field(String, default=None)
    height: int = sa_field(Integer, default=None)
    license: str = sa_field(String, default=None)
    observation_id: int = sa_field(ForeignKey('observation.id'), default=None, index=True)
    observation_uuid: str = sa_field(ForeignKey('observation.uuid'), default=None, index=True)
    url: str = sa_field(String, default=None)
    user_id: int = sa_field(ForeignKey('user.id'), default=None)
    width: int = sa_field(Integer, default=None)
    # uuid: str = sa_field(String, default=None)

    observation = relationship(
        'DbObservation', backref='photos', foreign_keys='DbPhoto.observation_id'
    )  # type: ignore

    @classmethod
    def from_model(cls, photo: Photo, **kwargs) -> 'DbPhoto':
        return cls(
            id=photo.id,
            license=photo.license_code,
            url=photo.url,
            **kwargs,
            # uuid=photo.uuid,
        )

    def to_model(self) -> Photo:
        return Photo(
            id=self.id,
            license_code=self.license,
            url=self.url,
            # user_id=self.user_id,
            # uuid=self.uuid,
        )


@Base.mapped
@dataclass
class DbUser:
    """Intermediate data model for persisting User data to a relational database"""

    __tablename__ = 'user'
    __sa_dataclass_metadata_key__ = 'sa'

    id: int = sa_field(Integer, primary_key=True)
    login: str = sa_field(String, default=None)
    name: str = sa_field(String, default=None)

    @classmethod
    def from_model(cls, user: User, **kwargs) -> 'DbUser':
        return cls(id=user.id, login=user.login, name=user.name)

    def to_model(self) -> User:
        return User(id=self.id, login=self.login, name=self.name)


def _split_ids(ids_str: str = None) -> List[int]:
    return [int(i) for i in ids_str.split(',')] if ids_str else []


def _join_ids(ids: List[int] = None) -> str:
    return ','.join(map(str, ids)) if ids else ''
