from typing import Any, Dict, List

from pyinaturalist.constants import ResponseResult

from pyinaturalist_convert.converters import AnyObservations, ensure_list, flatten_observation

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
    observations: AnyObservations, properties: List[str] = DEFAULT_OBSERVATION_ATTRS
) -> Dict[str, Any]:
    """Convert observations into a `GeoJSON FeatureCollection <https://tools.ietf.org/html/rfc7946#section-3.3>`_.

    By default this includes some basic observation attributes as GeoJSON ``Feature`` properties.
    The ``properties`` argument can be used to override these defaults. Nested values can be accessed
    with dot notation, for example ``taxon.name``.

    Returns:
        A ``FeatureCollection`` containing observation results as ``Feature`` dicts.
    """
    return {
        'type': 'FeatureCollection',
        'features': [_to_geojson_feature(obs, properties) for obs in ensure_list(observations)],
    }


def _to_geojson_feature(
    observation: ResponseResult, properties: List[str] = None
) -> ResponseResult:
    # Add geometry
    feature = {'type': 'Feature', 'geometry': observation['geojson']}
    feature['geometry']['coordinates'] = [float(i) for i in feature['geometry']['coordinates']]

    # Add properties
    flat_obs = flatten_observation(observation)
    feature['properties'] = {k: flat_obs.get(k) for k in properties or []}
    return feature
