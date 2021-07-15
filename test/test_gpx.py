import xmltodict

from pyinaturalist_convert.gpx import to_gpx
from test.conftest import load_sample_data


def test_to_gpx():
    observations = load_sample_data('observation.json')
    gpx_xml = to_gpx(observations)
    gpx_dict = xmltodict.parse(gpx_xml)
    point = gpx_dict['gpx']['trk']['trkseg']['trkpt']

    assert point['@lat'] == '50.646894' and point['@lon'] == '4.360086'
    assert point['time'] == '2018-09-05T14:06:00+0100'
    assert 'Lixus bardanae' in point['cmt'] and '2018-09-05' in point['cmt']
    assert point['link']['@href'].startswith('https://static.inaturalist.org/photos/')
