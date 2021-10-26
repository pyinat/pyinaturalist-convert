from typing import Any, Dict, List

from pyinaturalist.constants import ResponseResult

from pyinaturalist_convert.converters import AnyObservations, ensure_list, flatten_observation

from geojson import Feature, Point, FeatureCollection
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
) -> FeatureCollection:
    """Convert observations into a `GeoJSON FeatureCollection <https://tools.ietf.org/html/rfc7946#section-3.3>`_.

    By default this includes some basic observation attributes as GeoJSON ``Feature`` properties.
    The ``properties`` argument can be used to override these defaults. Nested values can be accessed
    with dot notation, for example ``taxon.name``.

    Returns: FeatureCollection
        A ``FeatureCollection`` containing observation results as ``Feature`` dicts.
    """
    try:
        feature_collection = FeatureCollection([_to_geojson_feature(obs, properties) for obs in ensure_list(observations)])
    except Exception as err:
        print(err)
    else:
        return feature_collection


def _to_geojson_feature(
    observation: ResponseResult, properties: List[str] = None
) -> Feature:
    # Add geometry
    if not observation.get('geojson'):
        raise ValueError("Observation without coordinates")
    point = Point([float(coord) for coord in observation['geojson']['coordinates']])

    # Add properties
    flat_obs = flatten_observation(observation)
    properties = {k: flat_obs.get(k) for k in properties or []}
    feature = Feature(geometry=point, properties=properties)
    return feature
