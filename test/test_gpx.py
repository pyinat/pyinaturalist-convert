from typing import Dict

import xmltodict

from pyinaturalist_convert.gpx import to_gpx
from test.conftest import load_sample_data


def test_to_gpx():
    observations = load_sample_data('observation.json')
    gpx_xml = to_gpx(observations).to_xml()
    gpx_dict = xmltodict.parse(gpx_xml)

    point = gpx_dict['gpx']['trk']['trkseg']['trkpt']
    _validate_point(point)


def test_to_gpx__waypoints():
    observations = load_sample_data('observation.json')
    gpx_xml = to_gpx(observations, waypoints=True).to_xml()
    gpx_dict = xmltodict.parse(gpx_xml)

    point = gpx_dict['gpx']['wpt']
    _validate_point(point)


def test_to_gpx__to_file(tmp_path):
    observations = load_sample_data('observation.json')
    file_path = tmp_path / 'observations.gpx'
    to_gpx(observations, file_path)

    with open(file_path) as f:
        gpx_dict = xmltodict.parse(f.read())

    point = gpx_dict['gpx']['trk']['trkseg']['trkpt']
    _validate_point(point)


def _validate_point(point: Dict):
    assert point['@lat'] == '50.646894' and point['@lon'] == '4.360086'
    assert point['time'] == '2018-09-05T14:06:00+0100'
    assert 'Lixus bardanae' in point['cmt'] and 'Sep 05, 2018' in point['cmt']
    assert point['link']['@href'].startswith('https://static.inaturalist.org/photos/')
