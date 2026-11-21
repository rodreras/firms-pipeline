# FIRMS South America pipeline

This repository publishes NASA FIRMS active-fire and thermal-anomaly detections for South America. It converts MODIS and VIIRS source records to annual GeoParquet, summarizes detections and fire radiative power (FRP) in H3 cells, and publishes PMTiles, MapLibre styles, and a STAC catalog in Google Cloud Storage (GCS).

**[This is highly inspired in Portolan's FIRMS catalog](https://github.com/portolan-mirrors/firms-catalog/)**. This project main goal is to learn more about Portolan, Geoparquet specs, Tilertoo and PMTiles, Data Engineering best practices and to work with cloud providers (CGP).

The default request box is `-82.44,-56.46,-34.10,13.58` in west, south, east, north order. Product availability determines the dates each satellite contributes. Detection counts can include repeated observations of the same fire.

[NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/) · [Dataset description and schema](publication/occurrences/README.md)

## Access the data

Open the public STAC Catalog in a STAC browser. Follow its child link to the occurrences Collection and select an annual Item. Resolve each relative asset path against the Item URL.

[Public STAC Catalog](https://storage.googleapis.com/sa-firms/firms/catalog.json) · [Occurrences Collection](https://storage.googleapis.com/sa-firms/firms/occurrences/collection.json)

```text
catalog.json
README.md
AGENTS.md
occurrences/
  collection.json
  README.md
  AGENTS.md
  year=YYYY/
    YYYY.json
    occurrences.parquet
    aggregate-l4.parquet
    aggregate-l5.parquet
    aggregate-l4.pmtiles
    aggregate-l5.pmtiles
    styles/
      h3-r4-detection_count.json
      ...
```

Use `occurrences.parquet` for point data. Use one H3 resolution to compare detection counts or FRP across cells. Each annual Item records the request window and completed intervals by product in `firms:coverage`. An Item may cover part of its year. Date-only rows record requested dates without detections. Filter on `geom IS NOT NULL` to count detections.

Download a linked point asset and run this DuckDB query from the download directory:

```sql
SELECT acq_date, source, count(*) AS detections
FROM read_parquet('occurrences.parquet', hive_partitioning = false)
WHERE geom IS NOT NULL
GROUP BY acq_date, source
ORDER BY acq_date, source;
```

For an HTTPS URL, load the DuckDB `httpfs` extension and use the resolved URL in `read_parquet`. The [dataset query guide](publication/occurrences/AGENTS.md) includes time, location, and FRP examples.

Open the PMTiles and styles for the year and resolution of interest. The Collection points to the latest year with maps. Styles cover detection count, total FRP, mean FRP, and maximum FRP. Each style uses color breaks from its own year and resolution. Compare the break values before you compare colors across maps. Years without detections have no PMTiles.

## How the pipeline works

The production entry point is [`python -m src.cloud`](src/cloud.py). GitHub Actions uses [`src.workflow_dates`](src/workflow_dates.py) to select inclusive dates and split the request at year boundaries.

| Step | Modules and dependencies |
| --- | --- |
| Request FIRMS data | `src.firms_http` checks product availability and the shared request budget. It handles timeouts, retries, and rate-limit responses. `src.satellites` lists the configured products. |
| Validate and cache chunks | `src.ingest` plans requests and tracks manifests. `src.normalize` validates FIRMS CSV and writes typed Parquet with PyArrow. `src.cloud` verifies and reuses checksummed chunks in the private GCS prefix. |
| Merge an annual partition | `src.annual` applies product-day coverage and merges fetched rows with the verified previous year. `src.catalog` builds the Hilbert-ordered WGS 84 points and STAC metadata with PySTAC. |
| Derive summaries and maps | `src.dataset` computes the H3 summaries with DuckDB and its H3 extension. `src.visualization` uses tylertoo's Python API for PMTiles and NumPy for the MapLibre style breaks. |
| Publish | `src.gcs_annual` and `src.gcs_collection` use the Google Cloud CLI to publish assets, Items, and the Collection. `src.cloud` publishes the root Catalog last and `src.logs` records structured events. |

When a run adds one day to a year, it verifies the previous occurrence file, merges the observations, and rebuilds the year's affected assets. It records the new checksums on the Item. Replacement checks compare the stored bytes with the **previous** Item's checksums and use the current GCS generations as write preconditions. Files whose bytes match are reused. The root Catalog can stay byte-identical because it links to the same Collection path.

The publisher stages the static catalog and collection guides from [`publication/`](publication/). Future runs preserve the ID of an existing migrated Collection. The [Portolan TODO](TODO.md) tracks the thumbnail, remaining metadata and Parquet work, and public validation.

## Reproduce the pipeline

Use Python 3.14 and [uv](https://docs.astral.sh/uv/). Install the locked project and map dependencies:

```bash
uv sync --locked --extra visualization
uv run --no-sync python -m unittest discover -s tests -v
```

### Build a local catalog

Set `FIRMS_MAP_KEY` in the environment or in a trusted, shell-compatible `.env` file. The launcher also accepts `FIRMS_API_KEY` or `MAP_KEY`.

```bash
./download-january.sh --check
./download-january.sh
```

The check reports availability and planned request windows. The full command writes a versioned local catalog under `outputs/firms/`. Open `outputs/firms/catalog.json`. Use `--output PATH` to choose a different directory. A rerun reuses verified windows. Pass `--refresh` to request them again.

To reproduce from a completed CSV download without querying FIRMS, pass its raw run manifest and matching dates and bbox:

```bash
uv run --no-sync python -m src.ingest \
  --csv-manifest path/to/raw/runs/run-id.json \
  --start-date 2026-01-01 --end-date 2026-01-31 \
  --bbox=-82.44,-56.46,-34.10,13.58 \
  --output outputs/firms-january-test
```

The local ingest path writes occurrences and local STAC. Use the cloud entry point to exercise the H3, maps, and GCS publication stages together.

### Run the cloud pipeline

Install and authenticate `gcloud`. Set `FIRMS_MAP_KEY`, `GCLOUD_URI`, and `GCLOUD_PRIVATE_URI` in `.env` or the environment. Use distinct, non-overlapping GCS prefixes. Give readers access only to the public prefix. The publishing account needs to read and create objects at both prefixes and replace objects at the public prefix.

```bash
uv run --no-sync python -c "import duckdb; duckdb.connect().execute('INSTALL h3 FROM community')"
uv run --no-sync python -m src.cloud --start-date 2026-01-18 --end-date 2026-01-18
```

Dates are inclusive. Use `--bbox` to set the request box for a separate dataset. PMTiles and styles are on by default. The cloud job keeps intermediate data in a temporary directory and records progress on stderr. GCS stores private cache chunks, run manifests, event logs, and replacement backups.

To build maps for previously published H3 assets, preview the [map backfill](scripts/backfill_pmtiles.py) with `--start-year` and `--end-year`. It stages the outputs and logs under `tmp/pmtiles-backfill-*/`. Pause the publisher before you apply a backfill with `--apply`.

## Schedule and recovery

- **Daily ingestion** is scheduled at **04:00 UTC**. GitHub can delay or drop a scheduled run. Each successful daily job requests the **seven completed UTC days** before the workflow creation date. Already covered product-days are skipped. The next daily run can fill a missed date while it remains inside that window.
- **Manual recovery** uses the daily workflow's `start_date` and `end_date` inputs. Enter an explicit inclusive range for gaps outside the rolling window. Re-run a failed job to use its original dates and reuse verified chunks.
- **Historical backfill** is manual. Its default range starts on 2000-01-01 and ends on the UTC workflow creation date. Use explicit completed dates if you want to avoid marking a partial current day as covered. The workflow publishes one year at a time.

The workflows share the `firms-production` concurrency group. Coordinate local publishers with Actions. Configure the GitHub environment with `FIRMS_MAP_KEY`, `GCLOUD_URI`, and `GCLOUD_PRIVATE_URI`. Set `GCP_WORKLOAD_IDENTITY_PROVIDER` and `GCP_SERVICE_ACCOUNT` as environment variables or secrets for Workload Identity Federation.

GCS replaces each object atomically. A hard stop can leave the Item and some files on different generations. If a retry reports a published checksum mismatch, pause publication. Inspect the private `runs/<run-id>/annual-backup/` files and restore the expected bytes with generation checks before rerunning. A corrupt private chunk also needs operator recovery.
