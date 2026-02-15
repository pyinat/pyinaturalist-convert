"""Utilities for importing iNaturalist places data from CSV"""

from typing import Any, Optional

from pyinaturalist.constants import GeoJson
from pyinaturalist.converters import convert_lat_long, try_float, try_int
from pyinaturalist.models import Place


def place_from_csv_row(row: dict[str, Any]) -> Place:
    """Convert a CSV row dict to a Place object.

    Expected CSV columns:
        id, name, display_name, latitude, longitude, swlat, swlng, nelat, nelng,
        place_type, bbox_area, ancestry, slug, admin_level, uuid, woeid

    Args:
        row: A dictionary representing a single CSV row (e.g., from csv.DictReader).

    Returns:
        A Place object populated from the CSV data.
    """
    woeid = try_int(row.get('woeid'))
    place_dict = {
        'id': try_int(row.get('id')),
        'name': row.get('name'),
        'display_name': row.get('display_name'),
        'location': convert_lat_long(row.get('latitude'), row.get('longitude')),
        'bounding_box_geojson': _convert_bbox_geojson(row),
        'place_type': try_int(row.get('place_type')),
        'bbox_area': try_float(row.get('bbox_area')),
        'slug': row.get('slug'),
        'admin_level': try_int(row.get('admin_level')),
        'uuid': row.get('uuid'),
        'woeid': None if woeid == 0 else woeid,
    }
    return Place.from_json(place_dict)


def _convert_bbox_geojson(
    row: dict[str, Any],
) -> Optional[GeoJson]:
    """Build a GeoJSON Polygon from bounding box coordinates.

    Returns None if any coordinate is missing.
    """
    swlat = try_float(row.get('swlat'))
    swlng = try_float(row.get('swlng'))
    nelat = try_float(row.get('nelat'))
    nelng = try_float(row.get('nelng'))

    if any(coord is None for coord in (swlat, swlng, nelat, nelng)):
        return None

    return {
        'type': 'Polygon',
        'coordinates': [
            [
                [swlng, swlat],
                [nelng, swlat],
                [nelng, nelat],
                [swlng, nelat],
                [swlng, swlat],
            ]
        ],
    }


def test_load():
    from pathlib import Path

    import pandas as pd

    csv_path = Path('~/.local/share/pyinaturalist/places.csv').expanduser()
    df = pd.read_csv(csv_path)
    print(df)


if __name__ == '__main__':
    test_load()
