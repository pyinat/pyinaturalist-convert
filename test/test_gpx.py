from typing import Dict

import xmltodict
from pyinaturalist import Observation

from pyinaturalist_convert.gpx import to_gpx
from test.conftest import SAMPLE_DATA_DIR


def test_to_gpx():
    obs = Observation.from_json_file(SAMPLE_DATA_DIR / 'observation.json')[0]
    gpx_xml = to_gpx([obs, obs]).to_xml()
    gpx_dict = xmltodict.parse(gpx_xml)

    points = gpx_dict['gpx']['trk']['trkseg']['trkpt']
    _validate_point(points[0])


def test_to_gpx__waypoints():
    obs = Observation.from_json_file(SAMPLE_DATA_DIR / 'observation.json')[0]
    gpx_xml = to_gpx([obs, obs], waypoints=True).to_xml()
    gpx_dict = xmltodict.parse(gpx_xml)

    points = gpx_dict['gpx']['wpt']
    _validate_point(points[0])


def test_to_gpx__to_file(tmp_path):
    obs = Observation.from_json_file(SAMPLE_DATA_DIR / 'observation.json')[0]
    file_path = tmp_path / 'observations.gpx'
    to_gpx([obs, obs], file_path)

    with open(file_path) as f:
        gpx_dict = xmltodict.parse(f.read())

    points = gpx_dict['gpx']['trk']['trkseg']['trkpt']
    _validate_point(points[0])


def _validate_point(point: Dict):
    assert point['@lat'] == '32.8430971478' and point['@lon'] == '-117.2815829044'
    assert point['time'] == '2020-05-09T06:01:00-07:00'
    assert 'Dirona picta' in point['cmt']
    assert 'May 09, 2020' in point['cmt']
    assert point['link']['@href'].startswith('https://inaturalist-open-data.s3.amazonaws.com/')
