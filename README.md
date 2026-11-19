# FIRMS South America pipeline

## January 2026 local run

From the repository root, use Python 3.14 with the project's dependencies.
The Bash launcher uses `.venv/bin/python` when available. Set
`FIRMS_MAP_KEY` in your trusted, shell-compatible `.env` or export it
before launching. The launcher also accepts `FIRMS_API_KEY` and `MAP_KEY`
from `.env`.

```bash
./download-january.sh --check
./download-january.sh
```

The optional check queries FIRMS and reports available products and request
windows. The full command processes January 2026 for the existing South America
request bounding box. FIRMS returns CSV over HTTP. The pipeline parses it in
memory and writes only Parquet data files.

Open `outputs/firms/catalog.json` as the STAC entry point. Its `firms-sa`
collection links to one Item per observation year and one GeoParquet asset per
Item. The 2026 asset contains January observations for this increment. Each
asset uses GeoParquet 1.1, Zstandard compression, WGS 84 point geometry in
`geom`, row-level bbox metadata, and Hilbert spatial ordering.
Use `--output PATH` to select a different local directory.

The schema follows the NOAA-21 VIIRS fields. NOAA-20 and Suomi NPP VIIRS use
the same VIIRS band columns. MODIS keeps `brightness` and `bright_t31`
in separate, nullable fields because they measure different bands.
`confidence` retains the source value as text, with VIIRS letters and MODIS
scores on their original scales. `acq_time` is HHMM and `acquired_at` is a
UTC timestamp. `source` and `version` identify the FIRMS product and
processing version. Additional source columns are preserved as JSON in
`source_attributes`. A malformed row fails its window with a row number.
Observations from separately configured products, including NRT and standard
processing, are retained with their provenance.

Rerun the same command to resume. Each completed window has a checksummed
Parquet artifact and a `complete.json` record. The record includes SHA-256
and size for both the FIRMS response and its Parquet conversion. Run manifests
under `outputs/firms/runs/` record each window's action and the published
asset. A normal rerun checks product availability and skips verified windows.
Pass `--refresh` to re-fetch them and pick up corrections. A release writes
its assets and STAC metadata before atomically switching `catalog.json`.
The previous release stays readable if a refresh fails.

Use one downloader per MAP key. The local process lock coordinates runs with
the same output directory. The client checks NASA's reported transaction usage
before area requests and retries, with a provisional ceiling of 5,000
transactions per window. Independent jobs using the same key share NASA's
budget.

The January validation uses local storage. A GCS destination in `.env` is
reserved for the later cloud publishing step.

```bash
python -m unittest discover -s tests -v
```
