# <img alt="Data FAIR logo" src="https://cdn.jsdelivr.net/gh/data-fair/data-fair@master/ui/public/assets/logo.svg" width="30"> @data-fair/processing-export-file

[data-fair/processings](https://github.com/data-fair/processings) plugin that exports dataset lines into one or several files attached as dataset metadata attachments. Supports 7 output formats: CSV, Parquet, XLSX, GeoJSON, PMTiles, Shapefile, GeoPackage.

## Features

- **Multi-format** — generates several formats at once from a single pass (csv, parquet, xlsx, geojson, pmtiles, shp, gpkg).
- **Filters** — restricts the export to a subset of lines with `in`, `interval` or `out` filters (translated into a `qs` query via `filters2qs`).
- **Column selection** — exports every non-calculated column by default, or a user-defined subset.
- **Geographic support** — automatically detects the `latitude`, `longitude`, `geometry` or `latLon` concepts and generates geographic formats through an intermediate OGR VRT layer.
- **Streamed pagination** — reads the dataset in pages of 10 000 lines with proper backpressure, no full in-memory buffering.
- **Attachment upload** — uploads each produced file on `/metadata-attachments` and patches the dataset attachments.
- **Graceful stop** — aborts cleanly between two pages when the platform requests a stop.

## System requirements

Geographic formats all rely on a single external binary:

| Format | Required binary |
| --- | --- |
| `geojson`, `shp`, `gpkg`, `pmtiles` | `ogr2ogr` (package `gdal-bin` on Debian/Ubuntu, GDAL ≥ 3.8 for `pmtiles`) |

If a geographic format is requested without the matching binary available, the plugin emits an explicit error log.

The `csv`, `parquet` and `xlsx` formats do not require any external binary.

## Configuration

### Tab `Dataset`

| Field | Description |
| --- | --- |
| `dataset` | Source dataset to export |

### Tab `Parameters`

| Field | Description |
| --- | --- |
| `fields` | Columns to include (all non-calculated columns by default) |
| `format` | One or several formats among csv/parquet/xlsx/geojson/pmtiles/shp/gpkg |
| `filename` | Output file name without extension (default `export`) |
| `label` | Attachment label (default `Export`) |
| `filters` | Optional filters (restrict to values, value range, exclude values) |

## Development

```bash
nvm use            # Node 24
npm install
npm run build-types
npm run lint
npm run test
```

Tests are **100% local** thanks to [`nock`](https://github.com/nock/nock) which simulates a data-fair instance. Geographic tests (`test-it/run-geo.test.ts`) are automatically skipped when `ogr2ogr` or `tippecanoe` are missing.

To run against a real instance, duplicate `config/default.mjs` as `config/local-test.mjs` (gitignored) with a real `dataFairUrl` + `dataFairAPIKey`.

## Release

Publishing is handled automatically by CI: the plugin is pushed to the data-fair registry (`@data-fair/registry`), not to the public npm registry — there is no manual `npm publish`. A push to `main`/`master` publishes to the staging registry; pushing a `v*` tag publishes to production:

```bash
npm version minor       # version bump + v* tag
git push --follow-tags  # CI publishes to the production registry
```
