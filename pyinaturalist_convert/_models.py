# TODO: Could conversion between ORM models and API (attrs) models be simplified?
from logging import getLogger
from typing import Any
from urllib.parse import quote_plus, unquote

from pyinaturalist import (
    Annotation,
    Comment,
    ConservationStatus,
    EstablishmentMeans,
    IconPhoto,
    Identification,
    Observation,
    ObservationFieldValue,
    Photo,
    Taxon,
    User,
)
from sqlalchemy import ForeignKey, inspect, types
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from .taxonomy import PRECOMPUTED_COLUMNS


class Base(DeclarativeBase):
    pass


JsonField = dict[str, Any]

logger = getLogger(__name__)


class DbObservation(Base):
    """Intermediate data model for persisting Observation data to a relational database

    Notes:
        * Datetimes are stored as strings, since SQLAlchemy DateTime doesn't handle timezone offsets
          from SQLite.
        * Nested collections (annotations, comments, IDs, OFVs, tags) are stored as denormalized
          JSON fields rather than in separate tables, since current use cases for this don't require
          a full relational structure.
        * Some data sources may provide a count of identifications, but not full identification
          details. For that reason, a separate ``identifications_count`` column is added.
    """

    __tablename__ = 'observation'

    id: Mapped[int] = mapped_column(primary_key=True)
    captive: Mapped[bool | None] = mapped_column(index=True, default=None)
    created_at: Mapped[str | None] = mapped_column(index=True, default=None)
    description: Mapped[str | None] = mapped_column(default=None)
    identifications_count: Mapped[int | None] = mapped_column(default=0)
    geoprivacy: Mapped[str | None] = mapped_column(index=True, default=None)
    latitude: Mapped[float | None] = mapped_column(default=None)
    longitude: Mapped[float | None] = mapped_column(default=None)
    license_code: Mapped[str | None] = mapped_column(default=None)
    observed_on: Mapped[str | None] = mapped_column(index=True, default=None)
    place_guess: Mapped[str | None] = mapped_column(default=None)
    place_ids: Mapped[str | None] = mapped_column(default=None)
    positional_accuracy: Mapped[int | None] = mapped_column(default=None)
    quality_grade: Mapped[str | None] = mapped_column(index=True, default=None)
    taxon_id: Mapped[int | None] = mapped_column(ForeignKey('taxon.id'), index=True, default=None)
    updated_at: Mapped[str | None] = mapped_column(index=True, default=None)
    user_id: Mapped[int | None] = mapped_column(ForeignKey('user.id'), default=None)
    user_login: Mapped[str | None] = mapped_column(default=None)
    uuid: Mapped[str | None] = mapped_column(index=True, default=None)

    # Denormalized nested collections
    annotations: Mapped[list[JsonField] | None] = mapped_column(types.JSON, default=None)
    comments: Mapped[list[JsonField] | None] = mapped_column(types.JSON, default=None)
    identifications: Mapped[list[JsonField] | None] = mapped_column(types.JSON, default=None)
    ofvs: Mapped[list[JsonField] | None] = mapped_column(types.JSON, default=None)
    tags: Mapped[str | None] = mapped_column(default=None)

    # Table relationships
    photos = relationship(
        'DbPhoto', back_populates='observation', foreign_keys='DbPhoto.observation_id'
    )  # type: ignore
    taxon = relationship('DbTaxon', backref='observations')  # type: ignore
    user = relationship('DbUser', backref='observations')  # type: ignore

    @property
    def sorted_photos(self) -> list[Photo]:
        """Get photos sorted by original position in the observation"""
        return [p.to_model() for p in sorted(self.photos, key=lambda p: p.position or 0)]

    # Column aliases for inaturalist-open-data
    # observation_uuid: str = synonym('uuid')  # type: ignore
    # observer_id: int = synonym('user_id')  # type: ignore

    @classmethod
    def from_model(cls, obs: Observation, skip_taxon: bool = False) -> 'DbObservation':
        db_obs = cls(
            id=obs.id,
            annotations=_flatten_annotations(obs.annotations),
            captive=obs.captive,
            comments=_flatten_comments(obs.comments),
            created_at=obs.created_at.isoformat() if obs.created_at else None,  # type: ignore [arg-type]
            description=obs.description,
            geoprivacy=obs.geoprivacy,
            identifications=_flatten_identifications(obs.identifications),
            identifications_count=obs.identifications_count,
            latitude=obs.location[0] if obs.location else None,  # type: ignore [arg-type]
            longitude=obs.location[1] if obs.location else None,  # type: ignore [arg-type]
            license_code=obs.license_code,
            observed_on=obs.observed_on.isoformat() if obs.observed_on else None,  # type: ignore [arg-type]
            ofvs=_flatten_ofvs(obs.ofvs),
            place_guess=obs.place_guess,
            place_ids=_join_list(obs.place_ids),
            positional_accuracy=obs.positional_accuracy,
            quality_grade=obs.quality_grade,
            tags=_join_list(obs.tags),
            taxon_id=obs.taxon.id if obs.taxon else None,  # type: ignore [arg-type]
            updated_at=obs.updated_at.isoformat() if obs.updated_at else None,  # type: ignore [arg-type]
            user_id=obs.user.id if obs.user else None,  # type: ignore [arg-type]
            uuid=obs.uuid,
        )

        # Add associated records
        db_obs.photos = _get_db_obs_photos(obs)  # type: ignore
        db_obs.user = DbUser.from_model(obs.user) if obs.user else None  # type: ignore
        # Optionally skip taxon, to instead merge via db.save_taxa()
        if obs.taxon and not skip_taxon:
            db_obs.taxon = DbTaxon.from_model(obs.taxon)  # type: ignore

        return db_obs

    def to_model(self) -> Observation:
        return Observation(
            id=self.id,
            annotations=self.annotations or [],
            captive=self.captive,
            comments=self.comments or [],
            created_at=self.created_at,
            description=self.description,
            geoprivacy=self.geoprivacy,
            identifications=self.identifications or [],
            identifications_count=self.identifications_count,
            location=(self.latitude, self.longitude),
            license_code=self.license_code,
            observed_on=self.observed_on,
            ofvs=self.ofvs or [],
            place_guess=self.place_guess,
            place_ids=_split_int_list(self.place_ids),
            positional_accuracy=self.positional_accuracy,
            quality_grade=self.quality_grade,
            photos=self.sorted_photos,
            tags=_split_list(self.tags),
            taxon=self.taxon.to_model().to_dict() if self.taxon else None,
            updated_at=self.updated_at,
            user=self.user.to_model() if self.user else {'id': self.user_id},
            uuid=self.uuid,
        )


class DbTaxon(Base):
    """Intermediate data model for persisting Taxon data to a relational database.

    Since different data sources provide different levels of detail, a ``partial`` field is added
    that indicates that some fields are missing, and can be fetched from the API if needed. As an
    example, this helps distinguish between taxa with no children and taxa with unlisted children.
    """

    __tablename__ = 'taxon'

    id: Mapped[int] = mapped_column(primary_key=True)
    ancestor_ids: Mapped[str | None] = mapped_column(default=None)
    child_ids: Mapped[str | None] = mapped_column(default=None)
    conservation_status: Mapped[str | None] = mapped_column(default=None)
    establishment_means: Mapped[str | None] = mapped_column(default=None)
    iconic_taxon_id: Mapped[int | None] = mapped_column(default=0)
    is_active: Mapped[bool | None] = mapped_column(default=None)
    leaf_taxa_count: Mapped[int | None] = mapped_column(default=0)
    observations_count: Mapped[int | None] = mapped_column(default=0)
    observations_count_rg: Mapped[int | None] = mapped_column(default=0)
    name: Mapped[str | None] = mapped_column(index=True, default=None)
    parent_id: Mapped[int | None] = mapped_column(ForeignKey('taxon.id'), index=True, default=None)
    partial: Mapped[bool | None] = mapped_column(default=False)
    photo_urls: Mapped[str | None] = mapped_column(default=None)
    preferred_common_name: Mapped[str | None] = mapped_column(default=None)
    rank: Mapped[str | None] = mapped_column(default=None)
    reference_url: Mapped[str | None] = mapped_column(default=None)
    wikipedia_summary: Mapped[str | None] = mapped_column(default=None)
    wikipedia_url: Mapped[str | None] = mapped_column(default=None)

    @classmethod
    def from_model(cls, taxon: Taxon) -> 'DbTaxon':
        photo_urls = _join_photo_urls(taxon.taxon_photos or [taxon.default_photo])
        return cls(
            id=taxon.id,
            ancestor_ids=_join_list(taxon.ancestor_ids),
            child_ids=_join_list(taxon.child_ids),
            conservation_status=getattr(taxon.conservation_status, 'status_name', ''),
            establishment_means=getattr(taxon.establishment_means, 'establishment_means', ''),
            iconic_taxon_id=taxon.iconic_taxon_id,
            is_active=taxon.is_active,
            leaf_taxa_count=taxon.complete_species_count,
            observations_count=taxon.observations_count,
            name=taxon.name,
            parent_id=taxon.parent_id,
            partial=taxon._partial,
            preferred_common_name=taxon.preferred_common_name,
            rank=taxon.rank,
            reference_url=taxon.reference_url,
            photo_urls=photo_urls,
            wikipedia_summary=taxon.wikipedia_summary,
            wikipedia_url=taxon.wikipedia_url,
        )

    def to_model(self) -> Taxon:
        photos = _split_photo_urls(self.photo_urls or '')
        c_status = (
            ConservationStatus(status_name=self.conservation_status)
            if self.conservation_status
            else None
        )
        est_means = (
            EstablishmentMeans(establishment_means=self.establishment_means)
            if self.establishment_means
            else None
        )
        return Taxon(
            id=self.id,
            ancestors=_get_taxa(self.ancestor_ids or ''),
            children=_get_taxa(self.child_ids or ''),
            conservation_status=c_status,
            establishment_means=est_means,
            default_photo=photos[0] if photos else [],
            iconic_taxon_id=self.iconic_taxon_id,
            is_active=self.is_active,
            complete_species_count=self.leaf_taxa_count,
            observations_count=self.observations_count_rg or self.observations_count,
            name=self.name,
            parent_id=self.parent_id,
            partial=self.partial,
            preferred_common_name=self.preferred_common_name,
            rank=self.rank,
            reference_url=self.reference_url,
            taxon_photos=photos,
            wikipedia_summary=self.wikipedia_summary,
            wikipedia_url=self.wikipedia_url,
        )

    def update(self, taxon: Taxon):
        """Merge new values into an existing record"""
        # Don't update a full taxon record with a partial one
        if self.partial is False and taxon._partial is True:
            return

        new_taxon = self.from_model(taxon)
        for col in [c.name for c in inspect(self.__class__).columns]:
            if (new_val := getattr(new_taxon, col)) is None:
                continue
            if col not in PRECOMPUTED_COLUMNS:
                setattr(self, col, new_val)
            # Precomputed columns: Only overwrite null values
            elif getattr(self, col) in [None, 0]:
                setattr(self, col, new_val)


# TODO: Combine observation_id/uuid into one column? Or two separate foreign keys?
# TODO: Should this include taxon photos as well, or just observation photos?
class DbPhoto(Base):
    """Intermediate data model for persisting Photo data to a relational database"""

    __tablename__ = 'photo'

    id: Mapped[int] = mapped_column(primary_key=True)
    extension: Mapped[str | None] = mapped_column(default=None)
    file_path: Mapped[str | None] = mapped_column(
        default=None
    )  # current/last known local file path (if any)
    height: Mapped[int | None] = mapped_column(default=None)
    license: Mapped[str | None] = mapped_column(default=None)
    observation_id: Mapped[int | None] = mapped_column(
        ForeignKey('observation.id'), index=True, default=None
    )
    observation_uuid: Mapped[str | None] = mapped_column(
        ForeignKey('observation.uuid'), index=True, default=None
    )
    original_filename: Mapped[str | None] = mapped_column(
        default=None
    )  # name when originally uploaded to iNat
    position: Mapped[int | None] = mapped_column(default=None)
    url: Mapped[str | None] = mapped_column(default=None)
    user_id: Mapped[int | None] = mapped_column(ForeignKey('user.id'), default=None)
    width: Mapped[int | None] = mapped_column(default=None)
    uuid: Mapped[str | None] = mapped_column(default=None)

    observation = relationship(
        'DbObservation', back_populates='photos', foreign_keys='DbPhoto.observation_id'
    )  # type: ignore

    @classmethod
    def from_model(cls, photo: Photo, **kwargs) -> 'DbPhoto':
        extension = photo.original_filename.split('.')[-1] if photo.original_filename else None
        return cls(
            id=photo.id,
            extension=extension,  # type: ignore [arg-type]
            license=photo.license_code,
            original_filename=photo.original_filename,
            url=photo.url,
            uuid=photo.uuid,
            **kwargs,
        )

    def to_model(self) -> Photo:
        return Photo(
            id=self.id,
            license_code=self.license,
            observation_id=self.observation_id,
            original_filename=self.original_filename,
            user_id=self.user_id,
            url=self.url,
            uuid=self.uuid,
        )


class DbUser(Base):
    """Intermediate data model for persisting User data to a relational database"""

    __tablename__ = 'user'

    id: Mapped[int] = mapped_column(primary_key=True)
    login: Mapped[str | None] = mapped_column(default=None)
    name: Mapped[str | None] = mapped_column(default=None)

    @classmethod
    def from_model(cls, user: User, **kwargs) -> 'DbUser':
        return cls(id=user.id, login=user.login, name=user.name)

    def to_model(self) -> User:
        return User(id=self.id, login=self.login, name=self.name)


# Minor helper functions
# TODO: Refactor these into marshmallow serializers?
# ----------------------


def _flatten_annotations(
    annotations: list[Annotation] | None = None,
) -> list[JsonField] | None:
    return [_flatten_annotation(a) for a in annotations] if annotations else None


def _flatten_annotation(annotation: Annotation) -> JsonField:
    """Save an annotation as a dict of either term/value labels (if available) or IDs"""
    annotation_dict = {
        'controlled_attribute_id': annotation.controlled_attribute.id,
        'controlled_value_id': annotation.controlled_value.id,
        'term': annotation.term,
        'value': annotation.value,
    }
    return {k: v for k, v in annotation_dict.items() if v is not None}


def _flatten_comments(
    comments: list[Comment] | None = None,
) -> list[JsonField] | None:
    return [_flatten_comment(c) for c in comments] if comments else None


def _flatten_comment(comment: Comment):
    return {
        'id': comment.id,
        'body': comment.body,
        'created_at': comment.created_at.isoformat(),
        'user': {'login': comment.user.login} if comment.user else None,
    }


def _flatten_identifications(
    identifications: list[Identification] | None = None,
) -> list[JsonField] | None:
    return [_flatten_identification(i) for i in identifications] if identifications else None


def _flatten_identification(identification: Identification) -> JsonField:
    # Only store the most relevant subset of info; basically a comment + taxon ID
    id_json = _flatten_comment(identification)
    id_json['taxon'] = {'id': identification.taxon.id} if identification.taxon else None
    return id_json


def _flatten_ofvs(
    ofvs: list[ObservationFieldValue] | None = None,
) -> list[JsonField] | None:
    return [{'name': ofv.name, 'value': ofv.value} for ofv in ofvs] if ofvs else None


def _get_taxa(id_str: str) -> list[Taxon]:
    return [Taxon(id=id, partial=True) for id in _split_int_list(id_str)]


def _get_db_obs_photos(obs: Observation) -> list[DbPhoto]:
    if not obs.photos:
        return []

    photos = []
    for i, photo in enumerate(obs.photos):
        photos.append(
            DbPhoto.from_model(
                photo,
                position=i,
                observation_id=obs.id,
                user_id=obs.user.id if obs.user else None,
            )
        )
    return photos


def _split_list(values_str: str | None = None) -> list[str]:
    return values_str.split(',') if values_str else []


def _split_int_list(values_str: str | None = None) -> list[int]:
    return [int(i) for i in values_str.split(',')] if values_str else []


def _join_list(values: list | None = None) -> str:
    return ','.join(map(str, values)) if values else ''


def _split_photo_urls(urls_str: str) -> list[Photo]:
    return [Photo(url=unquote(u)) for u in urls_str.split(',')] if urls_str else []


def _join_photo_urls(photos: list[Photo]) -> str:
    valid_photos = [p for p in photos if p and not isinstance(p, IconPhoto)]
    # quote URLs, so when splitting we can be sure ',' is not in any URL
    return ','.join([quote_plus(p.url) for p in valid_photos])
