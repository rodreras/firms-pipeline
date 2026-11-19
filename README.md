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

## Reuse a completed CSV download

Pass a completed raw run manifest under the raw output directory's `runs/`
folder. The importer verifies each CSV against its recorded SHA-256 and byte
size, preserves its original download timestamp, and writes the annual
GeoParquet, STAC catalog, and a new local run manifest without contacting FIRMS.

```bash
python -m src.ingest \
  --csv-manifest outputs/firms-raw/runs/d14ce1f2fcd94a3ea5e54c2c65677611.json \
  --start-date 2026-01-01 --end-date 2026-01-31 \
  --bbox=-82.44,-56.46,-34.10,13.58 \
  --output outputs/firms-january-test
```

The dates are inclusive and the bbox is west, south, east, north in WGS 84
degrees. The arguments also apply to the online downloader and must match
the raw manifest for an offline import. With no overrides, the downloader
uses the January 2026 dates and the existing South America request box.
STAC Item bounds come from the observations, and an empty collection uses
the request bbox.

## Run the GCS pipeline

Set both destinations in `.env` at the repository root:

```dotenv
GCLOUD_URI=gs://sa-firms/firms
GCLOUD_PRIVATE_URI=gs://your-private-bucket/firms-operations
```

The destinations need distinct, non-overlapping prefixes. For a single
bucket, use separately permissioned managed folders and grant readers access
only to the dataset folder. A bucket-wide public grant would expose both
prefixes. The pipeline uses the permissions already configured in GCS.
The authenticated Google Cloud CLI must be able to create and read objects
at both prefixes and replace the public annual files and collection.
Replacing an object requires `storage.objects.create` and
`storage.objects.delete`. The NASA key comes from `FIRMS_MAP_KEY` or
`FIRMS_API_KEY`.

The pipeline uses the DuckDB community H3 extension. Install it once from
the project Python environment if it is absent:

```bash
python -c "import duckdb; duckdb.connect().execute('INSTALL h3 FROM community')"
```

Run ingestion and upload together:

```bash
python -m src.cloud --start-date 2026-01-01 --end-date 2026-01-31
```

The command uses a temporary work directory for conversion and aggregation
and removes it on exit. GCS holds the persistent outputs. Live JSON logs
on stderr show download and processing progress, request-budget waits, HTTP
retries, and GCS uploads. The private run log records these events through
publication or failure. The request bbox
can be set with `--bbox`. Each run adds its years to the existing collection.

```text
AGENTS.md
README.md
catalog.json
occurrences/
  collection.json
  year=2026/
    2026.json
    occurrences.parquet
    aggregate-l5.parquet
    aggregate-l4.parquet
```

The annual Items include observation coverage, request dates, source products,
and GCS URIs. The aggregate assets contain WGS 84 H3 cell polygons in the
`geom` column. Each row includes the year, H3 index, detection count, and FRP
count, sum, average, and maximum. For a cell without measured FRP the FRP
count is zero and the other FRP metrics are null.

The private prefix holds SHA-256-verified normalized chunks and completion
markers under `chunks/`. Each run records its start under `runs/<run-id>/`.
Completed and handled failed runs upload `events.ndjson` and `manifest.json`
there. On an interrupted run, already committed chunks are available on retry.
Chunk payloads are uploaded before their completion markers. The run saves
a private publication record and uploads the root catalog after the assets,
Items, and collection are available. Annual files use fixed public names.
Each replacement has a GCS generation precondition. The pipeline writes the
Parquet assets before the annual Item and updates the collection last. It
backs up changed objects under the private run's `annual-backup/` prefix
before replacement and attempts to restore them on a handled failure. GCS
cannot switch several fixed-name objects atomically, so readers can see
mixed generations during publication or after an abrupt interruption.
The annual Item records completed request intervals per product in
`firms:coverage`. Overlapping requests skip those days, and a retry checks the
private cache for each remaining request chunk.

Extending a year verifies the previous occurrences checksum, merges the new
observations, and recomputes the aggregates from the merged table. A fetched
product and UTC date replace the previous rows for that product and day, so
a first update from a legacy Item without coverage metadata can safely
reprocess overlap. Years previously published with versioned Item paths move
to flat paths when extended. Existing objects at the old paths are retained.
Run one publisher at a time for a given dataset. Previously committed chunks
are available for retry.

```bash
python -m unittest discover -s tests -v
```

## GitHub Actions

The ingestion workflows use the `firms-production` GitHub environment. Set
these GitHub environment secrets before dispatching either workflow.

| Secret | Value |
| --- | --- |
| `FIRMS_MAP_KEY` | NASA FIRMS MAP key |
| `GCLOUD_URI` | Public GCS dataset root |
| `GCLOUD_PRIVATE_URI` | Private GCS cache, manifests, and backups root |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | Full Google Cloud workload identity provider resource name |
| `GCP_SERVICE_ACCOUNT` | Email of the publishing service account |

Configure Google Cloud Workload Identity Federation to allow the trusted
repository and ref to impersonate that account. Restrict deployment refs in
the GitHub environment and give the account only the permissions needed on
the two GCS prefixes described above.

- **FIRMS historical backfill** runs on manual dispatch. The defaults are
  January 1, 2000 through the UTC date the workflow was created, inclusive.
  For example, a run created on November 19, 2026 ends on November 19, 2026.
  Supply either or both dates to narrow a dispatch. The job publishes one
  calendar year at a time and FIRMS availability determines which
  product-days can be downloaded. Current-day results reflect what FIRMS
  has at request time. Later runs skip covered product-days, so use a
  completed date as the end if you need to avoid marking a partial day
  covered.
- **FIRMS daily ingestion** requests the seven completed UTC days before
  the workflow creation date. It is scheduled at **04:00 UTC** on the default
  branch. GitHub cron is best effort and can run late or drop a scheduled
  run. There is no guaranteed publication time. The rolling window can fill
  recent missed dates. For an older gap, manually dispatch with the inclusive
  start and end dates.
- Both workflows use the same publisher concurrency group. A running job
  keeps its slot. GitHub may replace a pending job with a newer one, and
  publishers started outside GitHub Actions must be coordinated separately.
  Both jobs use the existing five-day FIRMS request windows and shared-key
  budget checks with a 5,000-transaction ceiling. The client waits for budget
  to recover, honors supported HTTP 429 retry delays, and bounds HTTP
  retries. Reserve the key for one publisher at a time.

Re-run a failed Actions job to use the original workflow date and inputs.
Completed product-days are skipped, and checksummed private chunks let the
same interval resume where it stopped. The runner has a 350-minute limit.
Use smaller date ranges when a long backfill needs multiple dispatches.
The publisher stages and checks the annual data, H3 aggregates, and STAC
links before uploading. The public catalog points to current annual Items.
Source versions are preserved in observations, and the private ingestion
manifest records the processor commit.

A hard stop during replacement of an existing annual partition can leave
the public files and Item on different generations. If a retry reports a
published checksum mismatch, pause ingestion for the dataset and locate the
affected run's private `annual-backup/` prefix. Verify the expected bytes
against the Item and have the dataset operator restore the affected objects
with generation preconditions before rerunning. A corrupted private chunk
also requires operator recovery before a retry can use it. The seven-day
window fills uncovered product-days. It leaves covered data and observations
from distinct FIRMS products in place, including overlapping NRT and SP.

The **Pull request checks** workflow runs Ruff lint, Ruff formatting for
changed Python files, the local tests, and a redacted Gitleaks scan of PR
commits. It uses no production credentials. Enable its two jobs as required
checks in branch protection to enforce them. To scan staged changes locally,
install pre-commit and run `pre-commit install` from the repository root.
