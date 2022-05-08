from logging import getLogger

from gpxpy.gpx import GPX, GPXTrack, GPXTrackPoint, GPXTrackSegment, GPXWaypoint
from pyinaturalist import Observation
from pyinaturalist.constants import ResponseResult
from pyinaturalist.converters import convert_observation_timestamps

from .converters import AnyObservations, to_dict_list, write

logger = getLogger(__name__)


def to_gpx(observations: AnyObservations, filename: str = None, track: bool = True) -> str:
    """Convert a list of observations to a set of GPX waypoints or a GPX track

    Example:

        >>> from pyinaturalist import get_observations
        >>> from pyinaturalist_convert import to_gpx
        >>>
        >>> results = get_observations(
        ...     project_id=36883,         # ID of the 'Sugarloaf Ridge State Park' project
        ...     created_d1='2020-01-01',  # Get observations from January 2020...
        ...     created_d2='2020-09-30',  # ...through September 2020
        ...     geo=True,                 # Only get observations with coordinates
        ...     geoprivacy='open',        # Only get observations with public coordinates
        ...     page='all',               # Paginate through all response pages
        ... )
        >>> to_gpx(results, '~/tracks/observations-36883.gpx')

    Args:
        observations: JSON observations
        filename: Optional file path to write to
        track: Create an ordered GPX track; otherwise, create unordered GPX waypoints

    Returns:
        GPX XML as a string
    """
    gpx = GPX()
    points = [to_gpx_point(obs, track=track) for obs in to_dict_list(observations)]

    if track:
        gpx_track = GPXTrack()
        gpx.tracks.append(gpx_track)
        gpx_segment = GPXTrackSegment()
        gpx_track.segments.append(gpx_segment)
        gpx_segment.points = points
    else:
        gpx.waypoints = points

    gpx_xml = gpx.to_xml()
    if filename:
        write(gpx_xml, filename)
    return gpx_xml


def to_gpx_point(observation: ResponseResult, track: bool = True):
    """Convert a single observation to a GPX point

    Args:
        observation: JSON observation
        track: Indicates that this point is part of an ordered GXP track;
            otherwise, assume it is an unordered waypoint

    """
    logger.debug(f'Processing observation {observation["id"]}')
    observation = convert_observation_timestamps(observation)
    # GeoJSON coordinates are ordered as `longitude, latitude`
    long, lat = observation['geojson']['coordinates']

    # Get medium-sized photo URL, if available; otherwise just use observation URL
    if observation['photos']:
        link = observation['photos'][0]['url'].replace('square', 'medium')
    else:
        link = observation['uri']

    point_cls = GPXTrackPoint if track else GPXWaypoint
    point = point_cls(
        latitude=lat,
        longitude=long,
        time=observation['observed_on'],
        comment=str(Observation.from_json(observation)),
    )
    point.description = observation['description']
    point.link = link
    point.link_text = f'Observation {observation["id"]}'
    return point
