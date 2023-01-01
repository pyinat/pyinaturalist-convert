import sqlite3
from datetime import datetime

from pyinaturalist import (
    Annotation,
    Comment,
    ControlledTerm,
    ControlledTermValue,
    Identification,
    Observation,
    ObservationFieldValue,
    Photo,
    Taxon,
    User,
)

from pyinaturalist_convert.db import (
    create_tables,
    get_db_observations,
    get_db_taxa,
    save_observations,
    save_taxa,
)


def test_save_observations(tmp_path):
    db_path = tmp_path / 'observations.db'
    taxon = Taxon(id=1, name='test taxon', reference_url='https://google.com')
    user = User(id=1, login='test_user')
    annotation_1 = Annotation(
        controlled_attribute=ControlledTerm(id=1, label='term'),
        controlled_value=ControlledTermValue(id=2, label='value'),
    )
    annotation_2 = Annotation(
        controlled_attribute_id=1,
        controlled_value_id=2,
    )
    comment = Comment(
        id=1234,
        body='This is a test comment',
        created_at=datetime(2022, 2, 2),
        user=user,
    )
    identification = Identification(
        id=1234,
        body='This is a test ID comment',
        created_at=datetime(2022, 2, 2),
        user=user,
        taxon=taxon,
    )
    ofv = ObservationFieldValue(
        name="Magnification (Picture 1)",
        value=100,
    )
    obs_1 = Observation(
        id=1,
        annotations=[annotation_1, annotation_2],
        comments=[comment],
        identifications=[identification],
        identifications_count=1,
        license_code='CC-BY-NC',
        ofvs=[ofv],
        photos=[Photo(id=1, url='https://img_url')],
        place_ids=[1, 2, 3, 4],
        tags=['tag_1', 'tag_2'],
        taxon=taxon,
        user=user,
    )
    create_tables(db_path)
    save_observations([obs_1], db_path=db_path)

    results = get_db_observations(db_path)
    obs_2 = list(results)[0]
    assert obs_2.id == obs_1.id
    assert (
        obs_2.annotations[0].controlled_attribute.id == obs_2.annotations[0].controlled_attribute.id
    )
    assert obs_2.annotations[0].controlled_value.id == obs_2.annotations[0].controlled_value.id
    assert obs_2.annotations[0].term == obs_1.annotations[0].term
    assert obs_2.annotations[0].value == obs_1.annotations[0].value
    assert (
        obs_2.annotations[1].controlled_attribute.id == obs_1.annotations[1].controlled_attribute.id
    )
    assert obs_2.annotations[1].controlled_value.id == obs_1.annotations[1].controlled_value.id
    assert obs_2.comments[0].body == obs_1.comments[0].body
    assert obs_2.comments[0].user.login == obs_1.comments[0].user.login
    assert obs_2.identifications[0].created_at == obs_1.identifications[0].created_at
    assert obs_2.identifications[0].taxon.id == obs_1.identifications[0].taxon.id
    assert obs_2.identifications_count == obs_1.identifications_count
    assert obs_2.license_code == obs_1.license_code
    assert obs_2.ofvs[0].name == obs_1.ofvs[0].name
    assert obs_2.ofvs[0].value == obs_1.ofvs[0].value
    assert obs_2.photos[0].id == obs_1.photos[0].id
    assert obs_2.photos[0].url == obs_1.photos[0].url
    assert obs_2.place_ids == obs_1.place_ids
    assert obs_2.tags == obs_1.tags
    assert obs_2.taxon.id == obs_1.taxon.id
    assert obs_2.taxon.reference_url == obs_1.taxon.reference_url
    assert obs_2.user.id == obs_1.user.id

    results = get_db_observations(db_path, username='test_user', order_by_date=True)
    assert len(list(results)) == 1
    results = get_db_observations(db_path, username='nonexistent_user', limit=1)
    assert len(list(results)) == 0


def test_save_taxa(tmp_path):
    db_path = tmp_path / 'observations.db'
    photos = [Photo(id=1, url='https://img_url_1'), Photo(id=2, url='https://img_url_2')]
    taxon_1 = Taxon(
        id=3,
        name='Aves',
        rank='class',
        preferred_common_name='Birds',
        default_photo=photos[0],
        taxon_photos=photos,
    )
    create_tables(db_path)
    save_taxa([taxon_1], db_path=db_path)

    results = get_db_taxa(db_path)
    taxon_2 = list(results)[0]
    assert taxon_2.id == taxon_1.id
    assert taxon_2.name == taxon_1.name
    assert taxon_2.rank == taxon_1.rank
    assert taxon_2.default_photo.url == taxon_1.default_photo.url

    urls = [p.url for p in taxon_1.taxon_photos]
    saved_urls = [p.url for p in taxon_2.taxon_photos]
    assert saved_urls == urls


def test_save_taxa__preserve_precomputed_cols(tmp_path):
    db_path = tmp_path / 'observations.db'
    taxon_2 = Taxon(
        id=4,
        name='Gruifomres',
        rank='order',
        preferred_common_name='Cranes, Rails, and Allies',
        complete_species_count=416,
        observations_count=None,
    )
    taxon_1 = Taxon(
        id=3,
        name='Aves',
        rank='class',
        preferred_common_name='Birds',
        complete_species_count=10672,
        observations_count=18017625,
        children=[taxon_2],
    )

    create_tables(db_path)
    save_taxa([taxon_1], db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute('UPDATE taxon SET observations_count_rg = observations_count')
        conn.execute('UPDATE taxon SET observations_count = NULL')

    # Save with updated values for precomputed columns
    taxon_1.complete_species_count = 1
    taxon_1.observations_count = 1
    taxon_2.complete_species_count = None
    taxon_2.observations_count = 218279
    save_taxa([taxon_1], db_path=db_path)

    # Only previously null values (taxon_2.observations_count) in DB should be updated
    results = list(get_db_taxa(db_path))
    assert results[0].complete_species_count == 10672
    assert results[0].observations_count == 18017625
    assert results[1].complete_species_count == 416
    assert results[1].observations_count == 218279
