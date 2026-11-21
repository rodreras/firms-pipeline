"""Build PMTiles and styles from published H3 Parquet. Use --apply to publish."""

import argparse
import json
import os
import shutil
import sys
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import dotenv_values

from src.cloud import parse_destination
from src.dataset import cloud_uri, sha256
from src.gcs_annual import copy_generation, current_generation, update_annual_objects
from src.gcs_collection import COLLECTION_PATH, yearly_links
from src.ingest import write_json
from src.logs import event, run_logging
from src.visualization import MAX_ZOOM, build_visuals, set_collection_visuals


def download(public: str, path: Path, target: Path) -> None:
    uri = cloud_uri(public, path)
    generation = current_generation(uri, path)
    if generation == "0":
        raise ValueError(f"Missing published object: {path}")
    event("backfill_download", object=path.as_posix(), generation=generation)
    copy_generation(uri, generation, target, path)


def stage(work: Path, public: str, start: int, end: int) -> tuple[list[Path], dict[Path, str]]:
    staged = work / "staged"
    originals = work / "original"
    download(public, COLLECTION_PATH, originals / COLLECTION_PATH)
    collection = json.loads((originals / COLLECTION_PATH).read_bytes())
    expected = {COLLECTION_PATH: sha256(originals / COLLECTION_PATH)}
    paths = []
    for year, href in sorted(yearly_links(collection).items()):
        if not start <= year <= end:
            continue
        item_path = COLLECTION_PATH.parent / href
        original = originals / item_path
        download(public, item_path, original)
        item = json.loads(original.read_bytes())
        if item.get("collection") != collection["id"]:
            raise ValueError(f"Item {item_path} refers to a different Collection")
        expected[item_path] = sha256(original)
        visuals = {}
        for resolution in MAX_ZOOM:
            name = f"aggregate-l{resolution}.parquet"
            asset = item["assets"].get(f"aggregate-l{resolution}")
            if asset is None or Path(asset["href"]) != Path(name):
                raise ValueError(f"Missing or unexpected H3 asset for {year}, resolution {resolution}")
            path = item_path.parent / name
            local = originals / path
            download(public, path, local)
            digest = asset.get("firms:sha256")
            if digest and sha256(local) != digest:
                raise ValueError(f"Published H3 checksum did not match the Item: {path}")
            visuals.update(build_visuals(local, staged / item_path.parent, resolution))
        if not visuals:
            event("backfill_year_skipped_empty", year=year)
            continue
        for key, asset in visuals.items():
            path = item_path.parent / asset["href"]
            digest = item["assets"].get(key, {}).get("firms:sha256")
            if digest:
                expected[path] = digest
            asset.update({"firms:sha256": sha256(staged / path), "gcs:uri": cloud_uri(public, path)})
            paths.append(path)
        item["assets"].update(visuals)
        write_json(staged / item_path, item)
        paths.append(item_path)
        set_collection_visuals(collection, year, href, visuals)
        event("backfill_year_staged", year=year, visual_assets=len(visuals))
    if paths:
        write_json(staged / COLLECTION_PATH, collection)
        paths.append(COLLECTION_PATH)
    return paths, expected


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-year", type=int, default=1)
    parser.add_argument("--end-year", type=int, default=9999)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.start_year > args.end_year:
        parser.error("Start year must be on or before end year")
    config = {**dotenv_values(ROOT / ".env"), **os.environ}
    public = parse_destination(config.get("GCLOUD_URI") or "")
    private = parse_destination(config.get("GCLOUD_PRIVATE_URI") or "")
    if public == private or public.startswith(private + "/") or private.startswith(public + "/"):
        parser.error("The public and private GCS prefixes must be distinct and non-overlapping")
    if shutil.which("gcloud") is None:
        parser.error("Install and authenticate gcloud")
    run_id = f"pmtiles-backfill-{uuid4().hex}"
    work = ROOT / "tmp" / run_id
    work.mkdir(mode=0o700)
    with run_logging(work / "events.ndjson"):
        event("backfill_started", work=str(work), apply=args.apply)
        try:
            paths, expected = stage(work, public, args.start_year, args.end_year)
            if args.apply and paths:
                backup = f"{private}/runs/{run_id}/annual-backup"
                event("backfill_publication_started", backup=backup, objects=len(paths))
                with update_annual_objects(work / "staged", public, paths, expected, backup_destination=backup):
                    pass
            event("backfill_complete", applied=args.apply, objects=len(paths))
        except (Exception, KeyboardInterrupt) as error:
            event("backfill_failed", level="ERROR", error=str(error))
            raise


if __name__ == "__main__":
    main()
