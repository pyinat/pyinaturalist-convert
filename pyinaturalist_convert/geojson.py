"""Convert observations to
`GeoJSON FeatureCollections <https://tools.ietf.org/html/rfc7946#section-3.3>`_.

**Extra dependencies**: ``geojson``

**Example**::

    >>> from pyinaturalist import iNatClient
    >>> from pyinaturalist_convert import to_geojson

    >>> # Get all georeferenced observations made within 2km of Neal Smith Wildlife Refuge
    >>> client = iNatClient()
    >>> observations = client.observations.search(lat=41.55958, lng=-93.27904, radius=2).all()

    >>> # Convert to GeoJSON
    >>> geojson = to_geojson(observations)
"""

import json
from typing import TYPE_CHECKING, Optional

from pyinaturalist import Observation
from pyinaturalist.constants import ResponseResult

from .converters import AnyObservations, PathOrStr, flatten_observations, to_dicts, write

if TYPE_CHECKING:
    from geojson import Feature, FeatureCollection

# Basic observation attributes to include by default in geojson responses
DEFAULT_OBSERVATION_ATTRS = [
    'id',
    'photo_url',
    'positional_accuracy',
    'quality_grade',
    'taxon.id',
    'taxon.name',
    'taxon.preferred_common_name',
    'observed_on',
    'uri',
]


def to_geojson(
    observations: AnyObservations,
    filename: Optional[PathOrStr] = None,
    properties: list[str] = DEFAULT_OBSERVATION_ATTRS,
) -> Optional['FeatureCollection']:
    """Convert observations to a GeoJSON FeatureCollection.

    By default this includes some basic observation attributes as GeoJSON ``Feature`` properties.
    The ``properties`` argument can be used to override these defaults. Nested values can be accessed
    with dot notation, for example ``taxon.name``.

    Args:
        filename: An optional path to write the GeoJSON to
        properties: A list of observation attributes to include as GeoJSON properties

    Returns:
        A ``FeatureCollection`` containing observation results as ``Feature`` dicts
        (if no filename is provided)
    """
    from geojson import FeatureCollection

    feature_collection = FeatureCollection(
        [_to_geojson_feature(obs, properties) for obs in to_dicts(observations)]
    )

    if filename:
        write(json.dumps(feature_collection, indent=2), filename)
        return None
    else:
        return feature_collection


def geojson_to_observations(filename: PathOrStr) -> list[Observation]:
    """Load observations from a GeoJSON file.

    Args:
        filename: Path to a GeoJSON file
    """
    with open(filename, encoding='utf-8') as f:
        feature_collection = json.load(f)

    observations = []
    for feature in feature_collection.get('features', []):
        props = feature.get('properties') or {}
        coords = feature.get('geometry', {}).get('coordinates')

        # Unflatten dot-notation keys (e.g. 'taxon.id' -> {'taxon': {'id': ...}})
        nested: dict = {}
        for key, value in props.items():
            parts = key.split('.')
            node = nested
            for part in parts[:-1]:
                node = node.setdefault(part, {})
            node[parts[-1]] = value

        if coords:
            # GeoJSON coordinates are [longitude, latitude]
            nested['geojson'] = {'type': 'Point', 'coordinates': coords}
            nested['location'] = [coords[1], coords[0]]

        observations.append(Observation.from_json(nested))

    return observations


def _to_geojson_feature(
    observation: ResponseResult, properties: Optional[list[str]] = None
) -> 'Feature':
    from geojson import Feature, Point

    # Add geometry
    if not observation.get('geojson'):
        raise ValueError('Observation without coordinates')
    point = Point([float(coord) for coord in observation['geojson']['coordinates']])

    # Add properties
    flat_obs = flatten_observations([observation])[0]
    geom_properties = {k: flat_obs.get(k) for k in properties or []}
    feature = Feature(geometry=point, properties=geom_properties)
    return feature
