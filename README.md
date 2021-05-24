# pyinaturalist-convert
**This is an incomplete work in progress!**

[Pyinaturalist](https://github.com/niconoe/pyinaturalist) extensions to convert iNaturalist observation data to and from multiple formats.

Export formats currently supported:
* CSV
* Excel (xlsx)
* GPX (experimental)
* parquet

Planned/potential features:
* Convert to an HTML report
* Convert to Simple Darwin Core format
* Import observation data from the [iNaturalist export tool](https://www.inaturalist.org/observations/export) and convert it to be compatible with observation data from the iNaturalist API
* Import and convert observation data from [pyinaturalist-open-data](https://github.com/JWCook/pyinaturalist-open-data) (and possibly just merge that package with this one)
* Import and convert observation data from [GBIF dataset](https://www.gbif.org/dataset/50c9509d-22c7-4a22-a47d-8c48425ef4a7)
* Export to any [SQLAlchemy-compatible database engine](https://docs.sqlalchemy.org/en/14/core/engines.html#supported-databases)
