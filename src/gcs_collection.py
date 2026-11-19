"""Publish the stable occurrences collection using GCS generation checks."""

import json
import subprocess
from pathlib import Path, PurePosixPath

from src.dataset import GCSUploadError, cloud_uri, sha256
from src.ingest import write_json
from src.logs import event

COLLECTION_PATH = Path("occurrences/collection.json")


def yearly_links(collection: dict | None) -> dict[int, str]:
    result = {}
    for link in (collection or {}).get("links", []):
        if link["rel"] != "item":
            continue
        path = PurePosixPath(link["href"])
        year = path.parts[0].removeprefix("year=") if path.parts else ""
        if (not year.isdecimal() or path.parts[0] != f"year={year}"
                or ".." in path.parts or path.name != f"{year}.json"):
            raise ValueError("Unexpected annual STAC Item href")
        if int(year) in result:
            raise ValueError("Collection contains multiple Items for a year")
        result[int(year)] = path.as_posix()
    return result


def merge_collections(previous: dict, incoming: dict) -> dict:
    if previous["id"] != incoming["id"]:
        raise ValueError("Existing STAC collection ID does not match the occurrences collection")

    links = yearly_links(previous) | yearly_links(incoming)
    extents = [document["extent"] for document in (previous, incoming)]
    boxes = [box for extent in extents for box in extent["spatial"]["bbox"]]
    intervals = [interval for extent in extents for interval in extent["temporal"]["interval"]]
    starts, ends = zip(*intervals)
    return {
        **previous,
        "extent": {
            "spatial": {"bbox": [[min(b[0] for b in boxes), min(b[1] for b in boxes),
                                  max(b[2] for b in boxes), max(b[3] for b in boxes)]]},
            "temporal": {"interval": [[None if None in starts else min(starts),
                                       None if None in ends else max(ends)]]},
        },
        "links": [link for link in previous["links"] if link["rel"] != "item"]
        + [{"rel": "item", "href": f"./{links[year]}"} for year in sorted(links)],
    }


def read_collection(uri: str) -> tuple[str, dict | None]:
    result = subprocess.run([
        "gcloud", "storage", "objects", "describe", uri, "--raw", "--format=value(generation)",
    ], capture_output=True, text=True)
    if result.returncode != 0:
        message = result.stderr.lower()
        if any(marker in message for marker in (
            "404", "matched no objects", "no urls matched", "not found",
        )):
            return "0", None
        raise GCSUploadError(COLLECTION_PATH, "collection_read_failed", result.stderr.strip())
    generation = result.stdout.strip()
    if not generation.isdecimal():
        raise GCSUploadError(COLLECTION_PATH, "invalid_generation", "GCS returned no collection generation")
    existing = subprocess.run(
        ["gcloud", "storage", "cat", f"{uri}#{generation}"], capture_output=True, text=True,
    )
    if existing.returncode != 0:
        if any(marker in existing.stderr.lower() for marker in ("404", "not found", "no urls matched")):
            raise GCSUploadError(COLLECTION_PATH, "collection_changed", "the read generation disappeared")
        raise GCSUploadError(COLLECTION_PATH, "collection_read_failed", existing.stderr.strip())
    return generation, json.loads(existing.stdout)


def publish_collection(
    output: Path, destination: str, *, expected_items: dict[int, str | None] | None = None,
) -> None:
    uri = cloud_uri(destination, COLLECTION_PATH)
    path = output / COLLECTION_PATH
    incoming = json.loads(path.read_bytes())
    for attempt in range(1, 4):
        try:
            generation, previous = read_collection(uri)
        except GCSUploadError as error:
            if error.reason != "collection_changed":
                raise
            event("collection_update_retry", level="WARNING", uri=uri, attempt=attempt)
            continue
        current_items = yearly_links(previous)
        incoming_items = yearly_links(incoming)
        for year, expected in (expected_items or {}).items():
            current = current_items.get(year)
            if current != expected and current != incoming_items[year]:
                raise GCSUploadError(
                    COLLECTION_PATH, "annual_item_changed",
                    f"year {year} changed during processing. Rerun to merge with the current year.",
                )
        merged = merge_collections(previous, incoming) if previous is not None else incoming
        if merged == previous:
            event("gcs_object_reused", uri=uri)
            return
        write_json(path, merged)
        digest = sha256(path)
        event("collection_upload_started", uri=uri, generation=generation)
        result = subprocess.run([
            "gcloud", "storage", "cp", f"--if-generation-match={generation}",
            f"--custom-metadata=sha256={digest}", str(path), uri,
        ], capture_output=True, text=True)
        if result.returncode == 0:
            event("collection_upload_finished", uri=uri)
            return
        message = result.stderr.strip()
        if "412" not in message and "conditionnotmet" not in message.lower():
            raise GCSUploadError(COLLECTION_PATH, "collection_write_failed", message)
        event("collection_update_retry", level="WARNING", uri=uri, attempt=attempt)
    raise GCSUploadError(COLLECTION_PATH, "collection_changed", "retry after concurrent updates")
