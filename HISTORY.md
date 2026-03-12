# Changelog

## 0.9.0 (Unreleased)

- Add triggers to sync observation FTS table with main observation table
- Add alembic migration for observation FTS table + triggers

Darwin Core metadata:
- Replace legacy Adobe `xap` namespace with `xmp` for `CreateDate`, `Owner`, and `UsageTerms`
- Fix `observation.geoprivacy <-> dwc:informationWithheld`
- Map `taxon.variety -> to dwc:infraspecificEpithet` instead of `dwc:cultivarEpithet` (which is for horticultural cultivar
names)
- Map `observation.uuid -> dwc:occurrenceID` instead of observation URL
- Map `observation.quality_grade -> dwc:identificationVerificationStatus`
- Map `observation.species_guess -> dwc:verbatimIdentification`
- Map `observation.observed_on_details -> (dwc:year, dwc:month, dwc:day)`
- Map `observation.annotations -> (dwc:sex, dwc:lifeStage)`
- Map `observation.outlinks -> dwc:associatedReferences`
- Map `'present' (constant) -> dwc:occurrenceStatus`

## 0.8.3 (2026-03-01)

- Fix dependency check in `dwca.load_dwca_tables()`

## 0.8.2 (2026-02-28)

- Add a `fast` option to `sqlite.load_table()` (default False), to optimize for speed and disable safety settings

## 0.8.1 (2026-02-27)

- Fix check for alembic config path when installed via wheel

## 0.8.0 (2026-02-18)

- Add database migrations with Alembic and include in PyPI package
- Add a `migrate()` helper for downstream libraries
- Add a unified `export()` function to write observations to any supported format (based on file extension)
- Extend `read()` to support GeoJSON, GPX, DwC, and SQLite formats
- Improve CSV loading performance
- Rewrite taxonomy aggregation for significantly improved performance
- Optimize DwC-A table loading (delay index creation until after all rows are inserted)
- Raise a more descriptive error when SQLAlchemy is missing
- Strip timezone info from all datetime values before saving to xlsx (for compatibility with openpyxl)
- Fix issues with some observation formats passed to `to_json()`
- Update `to_geojson()` to use `location` tuple if available

## 0.7.0 (2026-01-22)

- Migrate packaging from Poetry to uv
- Drop Python 3.8 and 3.9 support; add Python 3.12, 3.13, and 3.14
- Add `file_path` and `original_filename` columns to `DbPhoto` model
- Flatten annotations using labels instead of numeric IDs
- Fix handling for observations with no photos

## 0.6.0 (2025-01-02)

- Add full-text search for observation descriptions and comments (`ObservationAutocompleter`)
- Add `username` filter and `order_by_date` option to `get_db_observations()`
- Add `page`, `order_by_created`, and `order_by_observed` parameters to `get_db_observations()`
- Preserve order of observation photos in database
- Add database columns: `Observation.tags`, `Observation.comments`, `Observation.identifications`, `Observation.created_at`, `Observation.identifications_count`, `Observation.geoprivacy`, `Observation.description`
- Add annotation and observation field value JSON columns
- Add `Taxon.reference_url` database column
- Add `DbTaxon` fields: conservation status, establishment means, Wikipedia description and URL
- Flatten observation sounds
- Fix CSV reading to force UTF-8 encoding (fixes Windows compatibility)
- Fix conservation status and establishment means handling
- Use WAL mode when writing to taxonomy database

## 0.5.0 (2022-07-21)

- Add taxon ancestry and common names to taxonomy aggregation
- Parallelize taxonomy aggregation by iconic taxon / phylum
- Add leaf taxon counts
- Move taxonomy logic to a dedicated `taxonomy` module
- Improve performance of taxonomy deduplication

## 0.4.0 (2022-06-11)

- Make pandas optional (previously a required dependency)
- Add `read()` function to load observations from common file formats
- Add DwC-to-`Observation` converter
- Add taxon common name, iconic taxon ID, subfamily, subgenus, and variety/cultivar to DwC records
- Add `Observation.license_code` and taxon photo URLs to SQLite storage
- Handle partial taxon records
- Skip observations without coordinates in GPX export

## 0.3.0 (2022-05-17)

- Add full-text search (FTS5) database built from iNaturalist taxonomy export
- Add functions to download and load DwC-A taxonomy export into SQLite
- Add taxon count aggregation
- Add option to return DwC as a dict instead of writing to XML
- Add geoprivacy info to DwC records
- Add additional DwC photo, date, and identification fields
- Allow passing taxa (not just observations) to `to_dwc()`
- Add `to_taxon_dwc()` function

## 0.2.0 (2022-02-23)

- Move GeoJSON converter from `pyinaturalist` into this package
- Fix GPX converter
- Use the `geojson` library for GeoJSON feature construction

## 0.1.0 (2021-07-14)

- Expand Darwin Core (DwC) converter: photo terms, date/time terms, taxon ancestry, photo license URLs, multi-record `SimpleDarwinRecordSet`
- Add functions to import/process CSV from the iNaturalist export tool
- Add Feather and HDF5 export wrappers

## 0.0.1 (2021-05-24)

- Initial release with basic conversion tools: CSV, XLSX, Parquet, GPX, DwC outline
