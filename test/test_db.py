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
    user = User(id=1, login='Test user')
    annotation_1 = Annotation(
        term=ControlledTerm(label='term'),
        value=ControlledTermValue(label='value'),
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
    assert obs_2.annotations[0].term_label == obs_1.annotations[0].term_label
    assert obs_2.annotations[0].value_label == obs_1.annotations[0].value_label
    assert (
        obs_2.annotations[1].controlled_attribute_id == obs_1.annotations[1].controlled_attribute_id
    )
    assert obs_2.annotations[1].controlled_value_id == obs_1.annotations[1].controlled_value_id
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
