"""Ingest FIRMS observations and publish a GCS dataset in one run."""

import argparse
import json
import os
import shutil
import subprocess
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import duckdb
from dotenv import dotenv_values

from src.annual import item_coverage
from src.dataset import GCSUploadError, cloud_uri, load_h3, sha256, stage_dataset, stage_root, upload_dataset
from src.firms_http import FirmsClient, FirmsError
from src.ingest import (
    BBOX, JANUARY_END, JANUARY_START, Chunk, parse_bbox, run, verified_record, write_json,
)
from src.normalize import SCHEMA_VERSION
from src.logs import event, run_logging
from src.gcs_collection import COLLECTION_PATH, publish_collection, read_collection, yearly_links
from src.gcs_annual import update_annual_objects


def get_object(uri: str, target: Path, *, optional: bool = False) -> bool:
    target.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["gcloud", "storage", "cp", uri, str(target)], capture_output=True, text=True,
    )
    if result.returncode == 0:
        return True
    message = result.stderr.lower()
    if optional and any(marker in message for marker in (
        "matched no objects", "no urls matched", "http error 404", "status=404",
        "notfoundexception: 404",
    )):
        return False
    raise FirmsError(f"Unable to read private GCS object {target.name}")


def published_items(
    output: Path, destination: str, links: dict[int, str], start: date, end: date, bbox: str,
) -> dict[int, dict]:
    items = {}
    for year, href in links.items():
        if not start.year <= year <= end.year:
            continue
        path = Path("occurrences") / href
        local = output / "previous" / path
        get_object(cloud_uri(destination, path), local)
        item = json.loads(local.read_bytes())
        if item["id"] != str(year) or item["properties"]["firms:request_bbox"] != [
            float(value) for value in bbox.split(",")
        ]:
            raise ValueError(f"Published annual Item {year} uses a different request bbox or ID")
        items[year] = item
    return items


def published_occurrences(
    output: Path, destination: str, href: str, item: dict,
) -> Path:
    asset = item["assets"]["data"]
    if asset["href"] != "./occurrences.parquet":
        raise ValueError("Published Item has an unexpected occurrences href")
    path = (Path("occurrences") / href).with_name("occurrences.parquet")
    uri = cloud_uri(destination, path)
    digest = asset.get("firms:sha256")
    if digest is None:
        result = subprocess.run([
            "gcloud", "storage", "objects", "describe", uri, "--raw", "--format=value(metadata.sha256)",
        ], capture_output=True, text=True)
        if result.returncode == 0:
            digest = result.stdout.strip()
    local = output / "previous" / path
    get_object(uri, local)
    if sha256(local) != digest:
        raise GCSUploadError(path, "existing_object_checksum_mismatch", "published occurrences failed verification")
    event("prior_year_verified", uri=uri)
    return local


class GCSChunkCache:
    def __init__(self, destination: str) -> None:
        self.destination = f"{destination}/chunks/schema-{SCHEMA_VERSION}"

    def restore(self, output: Path, chunk: Chunk) -> None:
        directory = chunk.directory(output)
        completion = directory / "complete.json"
        key = completion.relative_to(output)
        if not get_object(cloud_uri(self.destination, key), completion, optional=True):
            event("chunk_cache_miss", **chunk.reference())
            return

        record = json.loads(completion.read_bytes())
        digest = record.get("parquet_sha256", "")
        if not isinstance(digest, str) or len(digest) != 64 or any(
            char not in "0123456789abcdef" for char in digest
        ):
            raise FirmsError("Private chunk completion has an invalid Parquet checksum")
        payload = directory / f"{digest}.parquet"
        get_object(cloud_uri(self.destination, payload.relative_to(output)), payload)
        if verified_record(output, directory, chunk) is None:
            raise FirmsError(f"Private chunk failed verification for {chunk.source} {chunk.start}")
        event("chunk_restored", **chunk.reference())

    def commit(self, output: Path, chunk: Chunk, record: dict) -> None:
        completion = (chunk.directory(output) / "complete.json").relative_to(output)
        upload_dataset(output, self.destination, [Path(record["output"]), completion])
        event("chunk_committed", **chunk.reference())


def publish_run(
    client: FirmsClient,
    destination: str,
    private: str,
    start_date: date,
    end_date: date,
    bbox: str,
) -> str:
    run_id = uuid4().hex
    with tempfile.TemporaryDirectory(prefix="firms-cloud-") as work:
        output = Path(work)
        report = Path("runs") / run_id
        (output / report).mkdir(parents=True)
        log = output / report / "events.ndjson"
        manifest_file = output / report / "manifest.json"
        started = {
            "run_id": run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "requested_start": start_date.isoformat(),
            "requested_end": end_date.isoformat(),
            "bbox": bbox,
        }
        with run_logging(log):
            event("run_started", **started)
            write_json(output / report / "started.json", started)
            manifest = {}
            failure = None
            try:
                upload_dataset(output, private, [report / "started.json"])
                with duckdb.connect(config={"threads": 1}) as connection:
                    event("h3_extension_loading")
                    load_h3(connection)
                    event("published_collection_reading")
                    _, collection = read_collection(cloud_uri(destination, COLLECTION_PATH))
                    links = yearly_links(collection)
                    prior_items = published_items(output, destination, links, start_date, end_date, bbox)
                    covered = {}
                    for item in prior_items.values():
                        for product, days in item_coverage(item["properties"]).items():
                            covered.setdefault(product, set()).update(days)
                    manifest = run(
                        output, client, refresh=False, start_date=start_date, end_date=end_date,
                        bbox=bbox, cache=GCSChunkCache(private), covered_days=covered, run_id=run_id,
                        publish_empty_dates=True,
                    )
                    staged = output / "publication"
                    years = [asset["year"] for asset in manifest["publication"]["assets"]]
                    if years:
                        previous = {
                            year: (prior_items[year], published_occurrences(
                                output, destination, links[year], prior_items[year],
                            ))
                            for year in years if year in prior_items
                        }
                        files = stage_dataset(
                            manifest, output, staged, destination, connection,
                            previous_years=previous,
                        )
                    else:
                        staged.mkdir()
                        files = [stage_root(staged)]
                backup_uri = cloud_uri(private, report / "annual-backup")
                manifest["cloud_publication"] = {
                    "catalog": f"{destination}/catalog.json",
                    "objects": [cloud_uri(destination, path) for path in files],
                    "annual_backup_prefix": backup_uri,
                    "outcome": "staged",
                }
                if years:
                    metadata = [path for path in files[:-1] if path.parts[0] != "occurrences"]
                    annual = [path for path in files if path.parts[0] == "occurrences" and path != COLLECTION_PATH]
                    expected = {}
                    for year in years:
                        if year not in prior_items:
                            continue
                        item_path = Path("occurrences") / links[year]
                        if len(item_path.parts) == 3:
                            expected[item_path] = sha256(output / "previous" / item_path)
                            for asset in prior_items[year]["assets"].values():
                                if digest := asset.get("firms:sha256"):
                                    expected[item_path.parent / asset["href"]] = digest
                            expected[item_path.with_name("occurrences.parquet")] = sha256(previous[year][1])
                    with update_annual_objects(
                        staged, destination, metadata + annual, expected, backup_destination=backup_uri,
                    ):
                        publish_collection(
                            staged, destination, expected_items={year: links.get(year) for year in years},
                        )
                write_json(output / report / "ready.json", manifest)
                upload_dataset(output, private, [report / "ready.json"])
                with update_annual_objects(
                    staged, destination, files[-1:], {}, backup_destination=backup_uri,
                ):
                    pass
                manifest["cloud_publication"]["outcome"] = "complete"
                event("catalog_published", run_id=run_id, objects=len(files))
            except (Exception, KeyboardInterrupt) as error:
                failure = error
                ingest_record = output / "runs" / f"{run_id}.json"
                if not manifest and ingest_record.exists():
                    manifest = json.loads(ingest_record.read_bytes())
                details = {"error_type": type(error).__name__}
                if isinstance(error, GCSUploadError):
                    details.update(error_reason=error.reason, failed_object=error.object_path)
                manifest.update(outcome="failed", **details)
                if "cloud_publication" in manifest:
                    manifest["cloud_publication"]["outcome"] = "failed"
                event("run_failed", level="ERROR", run_id=run_id, **details)
                raise
            finally:
                manifest = {**started, **manifest, "finished_at": datetime.now(timezone.utc).isoformat()}
                write_json(manifest_file, manifest)
                try:
                    snapshot = output / "report-upload"
                    (snapshot / report).mkdir(parents=True)
                    for path in (log, manifest_file):
                        shutil.copyfile(path, snapshot / path.relative_to(output))
                    upload_dataset(snapshot, private, [report / log.name, report / manifest_file.name])
                except Exception:
                    if failure is None:
                        raise
                    failure.add_note("Private run log or manifest upload also failed.")
    return run_id


def parse_destination(value: str) -> str:
    destination = value.rstrip("/")
    uri = urlsplit(destination)
    parts = uri.path.split("/")[1:]
    if (
        uri.scheme != "gs" or not uri.netloc or not parts or uri.query or uri.fragment
        or any(part in {"", ".", ".."} for part in parts)
        or any(char in destination for char in "*[]#?%")
        or any(char.isspace() for char in destination)
    ):
        raise ValueError("expected a GCS URI with a dataset or operations prefix (gs://bucket/prefix)")
    return destination


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, default=JANUARY_START)
    parser.add_argument("--end-date", type=date.fromisoformat, default=JANUARY_END)
    parser.add_argument("--bbox", type=parse_bbox, default=BBOX)
    parser.add_argument("--transaction-limit", type=int, default=5000)
    args = parser.parse_args()
    if args.start_date > args.end_date:
        parser.error("--start-date must be on or before --end-date")
    if args.transaction_limit < 180:
        parser.error("--transaction-limit must be at least 180")
    config = {**dotenv_values(".env"), **os.environ}
    try:
        destination = parse_destination(config.get("GCLOUD_URI") or "")
        private = parse_destination(config.get("GCLOUD_PRIVATE_URI") or "")
    except ValueError as error:
        parser.error(f"set GCLOUD_URI and GCLOUD_PRIVATE_URI in .env: {error}")
    if (
        destination == private or destination.startswith(private + "/")
        or private.startswith(destination + "/")
    ):
        parser.error("GCLOUD_URI and GCLOUD_PRIVATE_URI must be separate, non-overlapping prefixes")
    key = config.get("FIRMS_MAP_KEY") or config.get("FIRMS_API_KEY") or config.get("MAP_KEY")
    if not key:
        parser.error("set FIRMS_MAP_KEY or FIRMS_API_KEY in .env")
    if shutil.which("gcloud") is None:
        parser.error("the authenticated Google Cloud CLI is required")
    try:
        run_id = publish_run(
            FirmsClient(key, args.transaction_limit), destination, private,
            args.start_date, args.end_date, args.bbox,
        )
    except (OSError, ValueError, FirmsError, duckdb.Error) as error:
        message = "\n".join([str(error), *getattr(error, "__notes__", [])])
        parser.exit(1, f"{message}\n")
    print(json.dumps({"catalog": f"{destination}/catalog.json", "run_id": run_id}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
