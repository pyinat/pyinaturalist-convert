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
    assert point['@lat'] == '32.8430971478' and point['@lon'] == '-117.2815829044'
    assert point['time'] == '2020-05-09T06:01:00-0700'
    assert 'Dirona picta' in point['cmt']
    assert 'May 09, 2020' in point['cmt']
    assert point['link']['@href'].startswith('https://inaturalist-open-data.s3.amazonaws.com/')
