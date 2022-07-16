from dataclasses import dataclass, field
from datetime import datetime
from logging import getLogger
from typing import List
from urllib.parse import quote_plus, unquote

from pyinaturalist import IconPhoto, Observation, Photo, Taxon, User
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
    license_code: str = sa_field(String, default=None)
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

    photos = relationship(
        'DbPhoto', back_populates='observation', foreign_keys='DbPhoto.observation_id'
    )  # type: ignore
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
            license_code=observation.license_code,
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
            license_code=self.license_code,
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
    iconic_taxon_id: int = sa_field(Integer, default=0)
    leaf_taxon_count: int = sa_field(Integer, default=0)
    name: str = sa_field(String, default=None, index=True)
    parent_id: int = sa_field(ForeignKey('taxon.id'), default=None, index=True)
    partial: int = sa_field(Boolean, default=False)
    photo_urls: str = sa_field(String, default=None)
    preferred_common_name: str = sa_field(String, default=None)
    rank: str = sa_field(String, default=None)

    @classmethod
    def from_model(cls, taxon: Taxon) -> 'DbTaxon':
        photo_urls = _join_photo_urls(taxon.taxon_photos or [taxon.default_photo])
        return cls(
            id=taxon.id,
            active=taxon.is_active,
            ancestor_ids=_join_ids(taxon.ancestor_ids),
            count=taxon.observations_count,
            child_ids=_join_ids(taxon.child_ids),
            iconic_taxon_id=taxon.iconic_taxon_id,
            name=taxon.name,
            parent_id=taxon.parent_id,
            partial=taxon._partial,
            preferred_common_name=taxon.preferred_common_name,
            rank=taxon.rank,
            photo_urls=photo_urls,
        )

    def to_model(self) -> Taxon:
        photos = _split_photo_urls(self.photo_urls)
        return Taxon(
            id=self.id,
            ancestors=self._get_taxa(self.ancestor_ids),
            children=self._get_taxa(self.child_ids),
            default_photo=photos[0] if photos else None,
            iconic_taxon_id=self.iconic_taxon_id,
            is_active=self.active,
            name=self.name,
            observations_count=self.count,
            parent_id=self.parent_id,
            partial=self.partial,
            preferred_common_name=self.preferred_common_name,
            rank=self.rank,
            taxon_photos=photos,
        )

    def _get_taxa(self, id_str: str) -> List[Taxon]:
        return [Taxon(id=id, partial=True) for id in _split_ids(id_str)]


# TODO: Combine observation_id/uuid into one column? Or two separate foreign keys?
# TODO: Should this include taxon photos as well, or just observation photos?
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
        'DbObservation', back_populates='photos', foreign_keys='DbPhoto.observation_id'
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


def _split_photo_urls(urls_str: str) -> List[Photo]:
    return [Photo(url=unquote(u)) for u in urls_str.split(',')] if urls_str else []


def _join_photo_urls(photos: List[Photo]) -> str:
    valid_photos = [p for p in photos if p and not isinstance(p, IconPhoto)]
    # quote URLs, so when splitting we can be sure ',' is not in any URL
    return ','.join([quote_plus(p.url) for p in valid_photos])
