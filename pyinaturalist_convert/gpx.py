"""Convert observations to
`GPX tracks or waypoints <https://hikingguy.com/how-to-hike/what-is-a-gpx-file>`_.

**Extra dependencies:** ``gpxpy``

**Example**::

    >>> from pyinaturalist import get_observations
    >>> from pyinaturalist_convert import to_gpx

    >>> results = get_observations(
    ...     project_id=36883,         # ID of the 'Sugarloaf Ridge State Park' project
    ...     created_d1='2020-01-01',  # Get observations from January 2020...
    ...     created_d2='2020-09-30',  # ...through September 2020
    ...     geo=True,                 # Only get observations with coordinates
    ...     geoprivacy='open',        # Only get observations with public coordinates
    ...     page='all',               # Paginate through all response pages
    ... )
    >>> to_gpx(results, '~/tracks/observations-36883.gpx')
"""
from logging import getLogger
from typing import TYPE_CHECKING, Optional

from pyinaturalist import Observation
from pyinaturalist.constants import ResponseResult
from pyinaturalist.converters import convert_observation_timestamps

from .converters import AnyObservations, to_dicts, write

if TYPE_CHECKING:
    from gpxpy.geo import Location
    from gpxpy.gpx import GPX

logger = getLogger(__name__)


def to_gpx(
    observations: AnyObservations, filename: str = None, waypoints: bool = False
) -> Optional['GPX']:
    """Convert a list of observations to a GPX track (default) or a set of GPX waypoints.

    Args:
        observations: JSON observations
        filename: Optional file path to write to
        waypoints: Create GPX waypoints (unordered); otherwise, create a GPX track (ordered)

    Returns:
        GPX object (if no filename is provided).
        See `gpxpy <https://github.com/tkrajina/gpxpy>`_ for more usage details.
    """

    from gpxpy.gpx import GPX, GPXTrack, GPXTrackSegment

    gpx = GPX()
    points = [to_gpx_point(obs, waypoints=waypoints) for obs in to_dicts(observations)]

    if waypoints:
        gpx.waypoints = points
    else:
        gpx_track = GPXTrack()
        gpx.tracks.append(gpx_track)
        gpx_segment = GPXTrackSegment()
        gpx_track.segments.append(gpx_segment)
        gpx_segment.points = points

    if filename:
        write(gpx.to_xml(), filename)
        return None
    else:
        return gpx


def to_gpx_point(observation: ResponseResult, waypoints: bool = False) -> 'Location':
    """Convert a single observation to a GPX point

    Args:
        observation: JSON observation
        track: Indicates that this point is part of an ordered GXP track;
            otherwise, assume it is an unordered waypoint

    """
    from gpxpy.gpx import GPXTrackPoint, GPXWaypoint

    logger.debug(f'Processing observation {observation["id"]}')
    observation = convert_observation_timestamps(observation)
    # GeoJSON coordinates are ordered as `longitude, latitude`
    long, lat = observation['geojson']['coordinates']

    # Get medium-sized photo URL, if available; otherwise just use observation URL
    if observation['photos']:
        link = observation['photos'][0]['url'].replace('square', 'medium')
    else:
        link = observation['uri']

    point_cls = GPXWaypoint if waypoints else GPXTrackPoint
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
