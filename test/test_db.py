from pyinaturalist import Observation, Photo, Taxon, User

from pyinaturalist_convert.db import (
    create_tables,
    get_db_observations,
    get_db_taxa,
    save_observations,
    save_taxa,
)


def test_save_observations(tmp_path):
    db_path = tmp_path / 'observations.db'
    obs_1 = Observation(
        id=1,
        license_code='CC-BY-NC',
        photos=[Photo(id=1, url='https://img_url')],
        place_ids=[1, 2, 3, 4],
        taxon=Taxon(id=1),
        user=User(id=1),
    )
    create_tables(db_path)
    save_observations([obs_1], db_path=db_path)

    results = get_db_observations(db_path)
    obs_2 = list(results)[0]
    assert obs_2.id == obs_1.id
    assert obs_2.license_code == obs_1.license_code
    assert obs_2.photos[0].id == obs_1.photos[0].id
    assert obs_2.photos[0].url == obs_1.photos[0].url
    assert obs_2.place_ids == obs_1.place_ids
    assert obs_2.taxon.id == obs_1.taxon.id
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
