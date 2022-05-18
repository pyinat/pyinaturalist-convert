from pyinaturalist import Observation, Photo, Taxon, User

from pyinaturalist_convert.db import create_tables, read_observations, save_observations


def test_save_observations(tmp_path):
    db_path = tmp_path / 'observations.db'
    obs = Observation(
        id=1,
        taxon=Taxon(id=1),
        user=User(id=1),
        photos=[Photo(id=1, url='https://img_url')],
        place_ids=[1, 2, 3, 4],
    )
    create_tables(db_path)
    save_observations([obs], db_path=db_path)

    results = read_observations(db_path)
    obs_2 = list(results)[0]
    assert obs_2.id == obs.id
    assert obs_2.taxon.id == obs.taxon.id
    assert obs_2.photos[0].id == obs.photos[0].id
    assert obs_2.photos[0].url == obs.photos[0].url
    assert obs_2.user.id == obs.user.id
    assert obs_2.place_ids == obs.place_ids
