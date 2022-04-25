# pyinaturalist-convert
[![Build status](https://github.com/JWCook/pyinaturalist-convert/workflows/Build/badge.svg)](https://github.com/JWCook/pyinaturalist-convert/actions)
[![PyPI](https://img.shields.io/pypi/v/pyinaturalist-convert?color=blue)](https://pypi.org/project/pyinaturalist-convert)
[![Conda](https://img.shields.io/conda/vn/conda-forge/pyinaturalist-convert?color=blue)](https://anaconda.org/conda-forge/pyinaturalist-convert)
[![PyPI - Python
Versions](https://img.shields.io/pypi/pyversions/pyinaturalist-convert)](https://pypi.org/project/pyinaturalist-convert)

<!-- [![Codecov](https://codecov.io/gh/JWCook/pyinaturalist-convert/branch/master/graph/badge.svg?token=FnybzVWbt2)](https://codecov.io/gh/JWCook/pyinaturalist-convert) -->

**Work in progress!**

This package provides tools to convert iNaturalist observation data to and from multiple formats.
This is mainly intended for use with data from the iNaturalist API
(via [pyinaturalist](https://github.com/niconoe/pyinaturalist)), but also works with other
iNaturalist data sources.

# Formats
Import formats currently supported:
* CSV (From either [API results](https://www.inaturalist.org/pages/api+reference#get-observations)
 or the [iNaturalist export tool](https://www.inaturalist.org/observations/export))
* JSON (from API results, either via `pyinaturalist`, `requests`, or another HTTP client)
* [`pyinaturalist.Observation`](https://pyinaturalist.readthedocs.io/en/stable/modules/pyinaturalist.models.Observation.html) objects
* Parquet

Export formats currently supported:
* CSV, Excel, and anything else supported by [tablib](https://tablib.readthedocs.io/en/stable/formats/)
* Feather, Parquet, and anything else supported by [pandas](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html)
* GeoJSON and GPX
* Darwin Core

# Installation
Install with pip:
```bash
pip install pyinaturalist-convert
```

To keep things modular, many format-specific dependencies are not installed by default, so you may need to install some
more packages depending on which formats you want. See
[pyproject.toml]([pyproject.toml](https://github.com/JWCook/pyinaturalist-convert/blob/7098c05a513ddfbc254a446aeec1dfcfa83e92ff/pyproject.toml#L44-L50))
for the full list (TODO: docs on optional dependencies).

To install all of the things:
```bash
pip install pyinaturalist-convert[all]
```

# Usage
Get your own observations and save to CSV:
```python
from pyinaturalist import get_observations
from pyinaturalist_convert import to_csv

observations = get_observations(user_id='my_username')
to_csv(observations, 'my_observations.csv')
```


# Planned and Possible Features
* Convert to an HTML report
* Convert to print-friendly format
* Export to any [SQLAlchemy-compatible database engine](https://docs.sqlalchemy.org/en/14/core/engines.html#supported-databases)
* Import and convert metadata and images from [iNaturalist open data on Amazon]()
    * See also [pyinaturalist-open-data](https://github.com/JWCook/pyinaturalist-open-data), which may eventually be merged with this package
* Import and convert observation data from the [iNaturalist GBIF Archive](https://www.inaturalist.org/pages/developers)
* Import and convert taxonomy data from the [iNaturalist Taxonomy Archive](https://www.inaturalist.org/pages/developers)
* Note: see [API Recommended Practices](https://www.inaturalist.org/pages/api+recommended+practices)
  for details on which data sources are best suited to different use cases
