# pyinaturalist-convert
**This is an incomplete work in progress!**

[Pyinaturalist](https://github.com/niconoe/pyinaturalist) extensions to convert iNaturalist observation data to and from multiple formats.

# Formats
Import formats currently supported:
* CSV (Currently from API results only, but see planned features below)
* JSON (either from a `requests.Response` or `pyinaturalist` results)
* parquet

Export formats currently supported:
* CSV
* Excel (xlsx)
* GPX (experimental)
* parquet
* pandas DataFrame


# Installation
**Note:** PyPI release coming soon.
```bash
pip install git+https://github.com/JWCook/pyinaturalist-convert.git
```

To keep things modular, many format-specific dependencies are not installed by default, so you may need to install some
more packages depending on which formats you want. See [pyproject.toml](pyproject.toml) for the full list (TODO: docs on optional dependencies).

To install all of the things:
```bash
pip install git+https://github.com/JWCook/pyinaturalist-convert.git#egg=pyinaturalist-convert[all]
```

# Usage
Basic usage example:
```python
from pyinaturalist import get_observations
from pyinaturalist_convert import to_csv

observations = get_observations(user_id='my_username')
to_csv(observations, 'my_observations.csv')
```

# Planned/possible features
* Convert to an HTML report
* Convert to Simple Darwin Core format
* Export to any [SQLAlchemy-compatible database engine](https://docs.sqlalchemy.org/en/14/core/engines.html#supported-databases)
* Import and convert observation data from the [iNaturalist export tool](https://www.inaturalist.org/observations/export) and convert it to be compatible with observation data from the iNaturalist API
* Import and convert metadata and images from [iNaturalist open data on Amazon]()
    * See also [pyinaturalist-open-data](https://github.com/JWCook/pyinaturalist-open-data), which may eventually be merged with this package
* Import and convert observation data from the [iNaturalist GBIF Archive](https://www.inaturalist.org/pages/developers)
* Import and convert observation data from the[iNaturalist Taxonomy Archive](https://www.inaturalist.org/pages/developers)
* Note: see [API Recommended Practices](https://www.inaturalist.org/pages/api+recommended+practices)
  for details on which data sources are best suited to different use cases

