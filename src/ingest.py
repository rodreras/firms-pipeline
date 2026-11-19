"""Download January 2026 FIRMS data and publish local GeoParquet and STAC."""

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
from uuid import uuid4

from src.firms_http import FirmsClient, FirmsError
from src.local_files import atomic_write
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

    def reference(self) -> dict:
        return {
            "source": self.source,
            "region": REGION,
            "bbox": BBOX,
            "requested_start": self.start.isoformat(),
            "requested_end": (self.start + timedelta(days=self.days - 1)).isoformat(),
            "source_reference": "NASA FIRMS Area CSV API",
        }

    def directory(self, output: Path) -> Path:
        return output / REGION / BBOX / self.source / f"{self.start}_{self.days}d"


def plan_chunks(availability: dict[str, tuple[date, date]]) -> Iterator[Chunk]:
    for source in SATELLITE_LIST:
        if source not in availability:
            continue
        first, last = availability[source]
        start, end = max(first, JANUARY_START), min(last, JANUARY_END)
        while start <= end:
            days = min(5, (end - start).days + 1)
            yield Chunk(source, start, days)
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
) -> tuple[str, dict]:
    from src.normalize import SCHEMA_VERSION, geoparquet, normalize

    directory = chunk.directory(output)
    directory.mkdir(parents=True, exist_ok=True)
    previous = verified_record(output, directory, chunk)
    if previous is not None and not refresh:
        return "skipped", previous

    body = fetch(chunk)
    downloaded_at = datetime.now(timezone.utc).isoformat()
    digest = hashlib.sha256(body).hexdigest()
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


def run(output: Path, client: FirmsClient, *, refresh: bool) -> dict:
    try:
        from src.catalog import publish_catalog
    except ImportError:
        raise FirmsError("Load the project Python environment with its GeoParquet and STAC dependencies") from None

    runs = output / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    run_id = uuid4().hex
    code = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parent,
        capture_output=True, text=True, check=False,
    )
    manifest = {
        "run_id": run_id,
        "code_commit": code.stdout.strip() if code.returncode == 0 else None,
        "region": REGION,
        "bbox": BBOX,
        "requested_start": JANUARY_START.isoformat(),
        "requested_end": JANUARY_END.isoformat(),
        "refresh": refresh,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "chunks": [],
        "outcome": "interrupted",
    }
    manifest_path = runs / f"{run_id}.json"
    write_json(manifest_path, manifest)

    def fetch(chunk: Chunk) -> bytes:
        return client.area(chunk.source, BBOX, chunk.start, chunk.days)

    try:
        availability = client.availability()
        chunks = list(plan_chunks(availability))
        manifest["unavailable_products"] = sorted(set(SATELLITE_LIST) - availability.keys())
        manifest["products_outside_window"] = sorted(
            (set(SATELLITE_LIST) & availability.keys()) - {chunk.source for chunk in chunks}
        )
        if not chunks:
            raise FirmsError("No configured FIRMS products are available for January 2026")
        for chunk in chunks:
            manifest["current_chunk"] = chunk.reference()
            write_json(manifest_path, manifest)
            action, record = download_chunk(output, chunk, fetch, refresh=refresh, run_id=run_id)
            manifest["chunks"].append({"action": action, **record})
            del manifest["current_chunk"]
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("outputs/firms"))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--refresh", action="store_true")
    mode.add_argument(
        "--check", action="store_true",
        help="check FIRMS access and show the January plan without downloading observations",
    )
    parser.add_argument("--transaction-limit", type=int, default=5000)
    args = parser.parse_args()
    if args.transaction_limit < 180:
        parser.error("--transaction-limit must be at least 180")
    key = os.environ.get("FIRMS_MAP_KEY")
    if not key:
        parser.error("set FIRMS_MAP_KEY in the downloader environment")

    client = FirmsClient(key, args.transaction_limit)
    try:
        if args.check:
            chunks = list(plan_chunks(client.availability()))
            if not chunks:
                raise FirmsError("No configured FIRMS products are available for January 2026")
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
                args.output, client, refresh=args.refresh
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
