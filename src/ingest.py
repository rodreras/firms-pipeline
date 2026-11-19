"""Ingest FIRMS data and publish local GeoParquet and STAC."""

import argparse
import fcntl
import hashlib
import json
import os
import subprocess
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from src.firms_http import FirmsClient, FirmsError
from src.local_files import atomic_write
from src.logs import event
from src.satellites import SATELLITE_LIST

BBOX = "-82.44,-56.46,-34.10,13.58"
REGION = "south-america-bbox"
JANUARY_START = date(2026, 1, 1)
JANUARY_END = date(2026, 1, 31)


@dataclass(frozen=True)
class Chunk:
    source: str
    start: date
    days: int
    bbox: str = BBOX

    def reference(self) -> dict:
        return {
            "source": self.source,
            "region": REGION,
            "bbox": self.bbox,
            "requested_start": self.start.isoformat(),
            "requested_end": (self.start + timedelta(days=self.days - 1)).isoformat(),
            "source_reference": "NASA FIRMS Area CSV API",
        }

    def directory(self, output: Path) -> Path:
        return output / REGION / self.bbox / self.source / f"{self.start}_{self.days}d"


class ChunkCache(Protocol):
    def restore(self, output: Path, chunk: Chunk) -> None: ...

    def commit(self, output: Path, chunk: Chunk, record: dict) -> None: ...


def plan_chunks(
    availability: dict[str, tuple[date, date]],
    start_date: date = JANUARY_START,
    end_date: date = JANUARY_END,
    bbox: str = BBOX,
    covered_days: dict[str, set[date]] | None = None,
) -> Iterator[Chunk]:
    for source in SATELLITE_LIST:
        if source not in availability:
            continue
        first, last = availability[source]
        start, end = max(first, start_date), min(last, end_date)
        covered = (covered_days or {}).get(source, set())
        while start <= end:
            if start in covered:
                start += timedelta(days=1)
                continue
            window = min(5, (min(end, date(start.year, 12, 31)) - start).days + 1)
            days = next((offset for offset in range(1, window)
                         if start + timedelta(days=offset) in covered), window)
            yield Chunk(source, start, days, bbox)
            start += timedelta(days=days)


def write_json(path: Path, record: dict) -> None:
    atomic_write(path, (json.dumps(record, indent=2) + "\n").encode())


def verified_record(output: Path, directory: Path, chunk: Chunk) -> dict | None:
    from src.normalize import SCHEMA_VERSION

    try:
        record = json.loads((directory / "complete.json").read_bytes())
        if any(record.get(key) != value for key, value in chunk.reference().items()):
            return None
        if record.get("schema_version") != SCHEMA_VERSION:
            return None
        digest = record["parquet_sha256"]
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            return None
        payload = directory / f"{digest}.parquet"
        if record["output"] != str(payload.relative_to(output)):
            return None
        with payload.open("rb") as stream:
            if os.fstat(stream.fileno()).st_size != record["parquet_byte_size"]:
                return None
            if hashlib.file_digest(stream, "sha256").hexdigest() == digest:
                return record
    except (FileNotFoundError, AttributeError, KeyError, TypeError, ValueError):
        pass
    return None


def download_chunk(
    output: Path,
    chunk: Chunk,
    fetch: Callable[[Chunk], bytes],
    *,
    refresh: bool = False,
    run_id: str | None = None,
    source_record: dict | None = None,
) -> tuple[str, dict]:
    from src.normalize import SCHEMA_VERSION, geoparquet, normalize

    directory = chunk.directory(output)
    directory.mkdir(parents=True, exist_ok=True)
    previous = verified_record(output, directory, chunk)
    if previous is not None and not refresh and source_record is None:
        return "skipped", previous

    body = fetch(chunk)
    digest = hashlib.sha256(body).hexdigest()
    downloaded_at = datetime.now(timezone.utc).isoformat()
    if source_record is not None:
        if len(body) != source_record["byte_size"] or digest != source_record["sha256"]:
            raise FirmsError(f"CSV checksum or size mismatch for {chunk.source} {chunk.start}")
        downloaded_at = source_record["downloaded_at"]
    if previous is not None and digest == previous["sha256"]:
        return "unchanged", previous

    observations = normalize(body, chunk.source, chunk.start, chunk.start + timedelta(days=chunk.days - 1))
    parquet = geoparquet(observations)
    parquet_digest = hashlib.sha256(parquet).hexdigest()
    payload = directory / f"{parquet_digest}.parquet"
    atomic_write(payload, parquet)
    record = {
        **chunk.reference(),
        "run_id": run_id,
        "downloaded_at": downloaded_at,
        "sha256": digest,
        "byte_size": len(body),
        "source_row_count": len(observations),
        "accepted_row_count": len(observations),
        "rejected_row_count": 0,
        "schema_version": SCHEMA_VERSION,
        "parquet_sha256": parquet_digest,
        "parquet_byte_size": len(parquet),
        "output": str(payload.relative_to(output)),
        "outcome": "complete",
    }
    write_json(directory / "complete.json", record)
    return ("changed" if previous is not None else "downloaded"), record


def run(
    output: Path,
    client: FirmsClient | None,
    *,
    refresh: bool,
    start_date: date = JANUARY_START,
    end_date: date = JANUARY_END,
    bbox: str = BBOX,
    csv_manifest: Path | None = None,
    cache: ChunkCache | None = None,
    covered_days: dict[str, set[date]] | None = None,
    run_id: str | None = None,
) -> dict:
    try:
        from src.catalog import publish_catalog
    except ImportError:
        raise FirmsError("Load the project Python environment with its GeoParquet and STAC dependencies") from None

    runs = output / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    run_id = run_id or uuid4().hex
    code = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parent,
        capture_output=True, text=True, check=False,
    )
    manifest = {
        "run_id": run_id,
        "code_commit": code.stdout.strip() if code.returncode == 0 else None,
        "region": REGION,
        "bbox": bbox,
        "requested_start": start_date.isoformat(),
        "requested_end": end_date.isoformat(),
        "refresh": refresh,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "chunks": [],
        "outcome": "interrupted",
    }
    manifest_path = runs / f"{run_id}.json"
    write_json(manifest_path, manifest)

    records = {}
    csv_root = None

    def fetch(chunk: Chunk) -> bytes:
        if csv_root is not None:
            path = (csv_root / records[chunk]["output"]).resolve()
            if not path.is_relative_to(csv_root) or path.suffix != ".csv":
                raise FirmsError(f"Invalid CSV path for {chunk.source} {chunk.start}")
            return path.read_bytes()
        assert client is not None
        return client.area(chunk.source, chunk.bbox, chunk.start, chunk.days)

    try:
        if csv_manifest is not None:
            csv_root = csv_manifest.resolve().parent.parent
            records = load_csv_records(csv_manifest, manifest)
            chunks = list(records)
            manifest["csv_manifest"] = str(csv_manifest)
        else:
            assert client is not None
            event("firms_availability_started")
            availability = client.availability()
            planned = list(plan_chunks(availability, start_date, end_date, bbox))
            chunks = list(plan_chunks(availability, start_date, end_date, bbox, covered_days))
            manifest["unavailable_products"] = sorted(set(SATELLITE_LIST) - availability.keys())
            manifest["products_outside_window"] = sorted(
                (set(SATELLITE_LIST) & availability.keys()) - {chunk.source for chunk in chunks}
            )
        if not chunks:
            if csv_manifest is None and planned:
                manifest["outcome"] = "complete"
                manifest["publication"] = {"assets": []}
                event("request_already_published")
                return manifest
            raise FirmsError("No FIRMS chunks are available for the requested dates")
        event("ingestion_plan", chunks=len(chunks))
        for index, chunk in enumerate(chunks, start=1):
            event("chunk_started", index=index, total=len(chunks), **chunk.reference())
            manifest["current_chunk"] = chunk.reference()
            write_json(manifest_path, manifest)
            if cache is not None:
                cache.restore(output, chunk)
            action, record = download_chunk(
                output, chunk, fetch, refresh=refresh, run_id=run_id,
                source_record=records.get(chunk),
            )
            if cache is not None and action != "skipped":
                cache.commit(output, chunk, record)
            manifest["chunks"].append({"action": action, **record})
            del manifest["current_chunk"]
            event("chunk_finished", index=index, action=action, rows=record["accepted_row_count"])
        event("yearly_occurrences_started")
        manifest["publication"] = publish_catalog(output, manifest)
        manifest["outcome"] = "complete"
    except (FirmsError, OSError, ValueError) as error:
        manifest["outcome"] = "failed"
        manifest["error_type"] = type(error).__name__
        raise
    finally:
        manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_json(manifest_path, manifest)
    return manifest


def load_csv_records(path: Path, request: dict) -> dict[Chunk, dict]:
    source = json.loads(path.read_bytes())
    scope = ("region", "bbox", "requested_start", "requested_end")
    if source.get("outcome") != "complete" or any(source.get(k) != request[k] for k in scope):
        raise FirmsError("CSV manifest must be complete and match the requested region, bbox and dates")

    records = {}
    last_by_source = {}
    try:
        for record in sorted(source["chunks"], key=lambda row: (row["source"], row["requested_start"])):
            start = date.fromisoformat(record["requested_start"])
            end = date.fromisoformat(record["requested_end"])
            product = record["source"]
            chunk = Chunk(product, start, (end - start).days + 1, request["bbox"])
            if (
                record["outcome"] != "complete"
                or product not in SATELLITE_LIST
                or not isinstance(record["output"], str)
                or not isinstance(record["byte_size"], int)
                or not isinstance(record["sha256"], str)
                or not isinstance(record["downloaded_at"], str)
                or start.year != end.year
                or not (request["requested_start"] <= start.isoformat() <= end.isoformat()
                        <= request["requested_end"])
                or start <= last_by_source.get(product, date.min)
                or any(record[key] != value for key, value in chunk.reference().items())
            ):
                raise ValueError
            records[chunk] = record
            last_by_source[product] = end
    except (KeyError, TypeError, ValueError):
        raise FirmsError("CSV manifest has invalid or overlapping chunks") from None
    return records


def parse_bbox(value: str) -> str:
    parts = [part.strip() for part in value.split(",")]
    try:
        west, south, east, north = map(float, parts)
        if -180 <= west < east <= 180 and -90 <= south < north <= 90:
            return ",".join(parts)
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("expected west,south,east,north in WGS 84 degrees")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("outputs/firms"))
    mode = parser.add_mutually_exclusive_group()
    parser.add_argument("--start-date", type=date.fromisoformat, default=JANUARY_START)
    parser.add_argument("--end-date", type=date.fromisoformat, default=JANUARY_END)
    parser.add_argument("--bbox", type=parse_bbox, default=BBOX)
    mode.add_argument("--csv-manifest", type=Path, help="import a completed local raw CSV run manifest")
    mode.add_argument("--refresh", action="store_true")
    mode.add_argument(
        "--check", action="store_true",
        help="check FIRMS access and show the request plan without downloading observations",
    )
    parser.add_argument("--transaction-limit", type=int, default=5000)
    args = parser.parse_args()
    if args.transaction_limit < 180:
        parser.error("--transaction-limit must be at least 180")
    if args.start_date > args.end_date:
        parser.error("--start-date must be on or before --end-date")
    client = None
    if args.csv_manifest is None:
        key = os.environ.get("FIRMS_MAP_KEY")
        if not key:
            parser.error("set FIRMS_MAP_KEY in the downloader environment")
        client = FirmsClient(key, args.transaction_limit)

    try:
        if args.check:
            assert client is not None
            chunks = list(plan_chunks(client.availability(), args.start_date, args.end_date, args.bbox))
            if not chunks:
                raise FirmsError("No configured FIRMS products are available for the requested dates")
            print(json.dumps({
                "outcome": "ready",
                "products": sorted({chunk.source for chunk in chunks}),
                "chunks": len(chunks),
            }))
            return 0
        args.output.mkdir(parents=True, exist_ok=True)
        with (args.output / ".ingest.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise FirmsError("another downloader is using this output directory") from None
            manifest = run(
                args.output, client, refresh=args.refresh, start_date=args.start_date,
                end_date=args.end_date, bbox=args.bbox, csv_manifest=args.csv_manifest,
            )
    except (FirmsError, ValueError) as error:
        print(json.dumps({"outcome": "failed", "error": str(error)}))
        return 1
    except OSError:
        print(json.dumps({"outcome": "failed", "error": "local filesystem operation failed"}))
        return 1
    print(json.dumps({
        "outcome": "complete",
        "chunks": len(manifest["chunks"]),
        "catalog": str(args.output / manifest["publication"]["catalog"]),
        "manifest": str(args.output / "runs" / f"{manifest['run_id']}.json"),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
