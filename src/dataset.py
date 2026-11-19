"""Build and upload the dataset layout from an ingestion run."""

import hashlib
import json
import shutil
import subprocess
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pystac
from pyproj import CRS
from shapely import from_wkt, to_wkb, total_bounds

from src.annual import chunk_coverage, compact_coverage, item_coverage, merge_observations
from src.catalog import PARQUET_MEDIA_TYPE, save_bytes, save_stac, yearly_asset
from src.logs import event

RESOLUTIONS = (4,5)
SUMMARY_COLUMNS = {
    "year": ("int32", "UTC observation year."),
    "h3_cell": ("uint64", "Numeric H3 cell index."),
    "detection_count": ("int64", "Number of FIRMS detections in this cell and year."),
    "frp_count": ("int64", "Number of detections with measured fire radiative power."),
    "frp_sum": ("float64", "Sum of measured fire radiative power in megawatts."),
    "frp_avg": ("float64", "Mean of measured fire radiative power in megawatts."),
    "frp_max": ("float64", "Maximum measured fire radiative power in megawatts."),
    "geom": ("WKB", "H3 cell boundary polygon in WGS 84 degrees."),
}

DATASET_README = b"""# NASA FIRMS South America

Open catalog.json to discover the occurrences collection and its annual items.
Each year directory holds occurrences.parquet (WGS 84 point GeoParquet) and
aggregate-l4.parquet and aggregate-l5.parquet (polygon GeoParquet grouped
by H3 cell and UTC observation year). The Item records the processed date range and source
products. A year directory can cover a partial year.

detection_count includes all detections, and frp_count includes only detections
with an FRP value. frp_sum, frp_avg, and frp_max use measured FRP in megawatts
and are null when the group has no measured FRP. Detections from different
products can refer to the same fire. Counts represent detections.
"""
DATASET_AGENTS = b"""# Querying this dataset

Start with catalog.json and follow the annual STAC Items. Resolve relative
asset hrefs from the Item URL or use the asset's gcs:uri with GCS credentials.
Use the Item's temporal coverage and firms:requested_start and
firms:requested_end when comparing annual partitions. For occurrences use
acquired_at and the longitude and latitude columns for filtering. For H3
summaries select one resolution. See README.md for the measures and units.
"""


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def load_h3(connection: duckdb.DuckDBPyConnection) -> None:
    try:
        connection.execute("LOAD h3")
    except duckdb.Error as error:
        raise ValueError(
            "DuckDB H3 is unavailable. In the project Python environment run "
            "duckdb.connect().execute('INSTALL h3 FROM community') first."
        ) from error


def aggregate(connection: duckdb.DuckDBPyConnection, path: Path, resolution: int) -> pa.Table:
    summary = connection.execute(
        """
        SELECT year, h3_cell, count(*) AS detection_count, count(frp) AS frp_count,
               sum(frp) AS frp_sum, avg(frp) AS frp_avg, max(frp) AS frp_max,
               h3_cell_to_boundary_wkt(h3_cell) AS boundary
        FROM (
            SELECT CAST(year(acq_date) AS INTEGER) AS year,
                   h3_latlng_to_cell(latitude, longitude, ?) AS h3_cell, frp
            FROM read_parquet(?, hive_partitioning = false)
        )
        GROUP BY year, h3_cell
        ORDER BY year, h3_cell
        """,
        [resolution, str(path)],
    ).to_arrow_table()
    geometry = from_wkt(summary["boundary"].to_numpy())
    column = {
        "encoding": "WKB",
        "geometry_types": ["Polygon"],
        "crs": CRS.from_epsg(4326).to_json_dict(),
    }
    if len(summary):
        column["bbox"] = total_bounds(geometry).tolist()
    metadata = {"version": "1.1.0", "primary_column": "geom", "columns": {"geom": column}}
    result = summary.drop(["boundary"]).append_column("geom", pa.array(to_wkb(geometry), type=pa.binary()))
    return result.replace_schema_metadata({"geo": json.dumps(metadata)})


def cloud_uri(destination: str, path: Path) -> str:
    return f"{destination}/{path.as_posix()}"


def stage_dataset(
    manifest: dict, source: Path, output: Path, destination: str,
    connection: duckdb.DuckDBPyConnection,
    *, previous_years: dict[int, tuple[dict, Path]] | None = None,
) -> list[Path]:
    source = source.resolve()
    assets = manifest.get("publication", {}).get("assets", [])
    if manifest["outcome"] != "complete" or not assets:
        raise ValueError("Expected a completed ingestion manifest with annual Parquet assets")
    if len({asset["year"] for asset in assets}) != len(assets):
        raise ValueError("Ingestion manifest contains duplicate annual assets")

    collection_path = Path("occurrences/collection.json")
    collection = None
    files = []
    for record in sorted(assets, key=lambda asset: asset["year"]):
        year = record["year"]
        parquet = (source / record["path"]).resolve()
        if not parquet.is_relative_to(source) or sha256(parquet) != record["sha256"]:
            raise ValueError(f"Invalid annual Parquet input for {year}")
        item = pystac.Item.from_file(str(parquet.with_suffix(".json")))
        if collection is None:
            collection = pystac.Collection.from_file(str(parquet.parent.parent / "collection.json"))
            collection.clear_links()
        year_chunks = [chunk for chunk in manifest["chunks"] if chunk["requested_start"][:4] == str(year)]
        new_coverage = chunk_coverage(year_chunks)
        coverage = {source: days.copy() for source, days in new_coverage.items()}
        first = min(chunk["requested_start"] for chunk in year_chunks)
        last = max(chunk["requested_end"] for chunk in year_chunks)
        products = set(new_coverage)
        previous = (previous_years or {}).get(year)
        rebuilt = None
        if previous is not None:
            prior_item, prior_parquet = previous
            properties = prior_item["properties"]
            first = min(first, properties["firms:requested_start"])
            last = max(last, properties["firms:requested_end"])
            products.update(properties["firms:products"])
            for product, days in item_coverage(properties).items():
                coverage.setdefault(product, set()).update(days)
            observations = merge_observations(prior_parquet, parquet, new_coverage, year)
            rebuilt, item = yearly_asset(observations, year, date.fromisoformat(first), date.fromisoformat(last))
            item.collection_id = collection.id

        item.clear_links()
        item.id = str(year)
        item.properties.update({
            "firms:requested_start": first,
            "firms:requested_end": last,
            "firms:request_bbox": [float(value) for value in manifest["bbox"].split(",")],
            "firms:products": sorted(products),
            "firms:coverage": compact_coverage(coverage),
            "firms:source": "NASA FIRMS Area CSV API",
        })
        partition = Path("occurrences") / f"year={year}"
        target_dir = output / partition
        target_dir.mkdir(parents=True, exist_ok=True)
        occurrences_path = partition / "occurrences.parquet"
        if rebuilt is None:
            shutil.copyfile(parquet, output / occurrences_path)
        else:
            save_bytes(output / occurrences_path, rebuilt)

        occurrences = item.assets["data"]
        occurrences.href = "./occurrences.parquet"
        for key in list(item.properties):
            if key.startswith(("table:", "proj:")):
                occurrences.extra_fields[key] = item.properties.pop(key)
        for resolution in RESOLUTIONS:
            path = partition / f"aggregate-l{resolution}.parquet"
            event("h3_aggregation_started", year=year, resolution=resolution)
            summary = aggregate(connection, output / occurrences_path, resolution)
            pq.write_table(summary, output / path, compression="zstd")
            item.add_asset(f"aggregate-l{resolution}", pystac.Asset(
                href=f"./{path.name}", media_type=PARQUET_MEDIA_TYPE, roles=["data"],
                title=f"H3 resolution {resolution} annual detection and FRP summary",
                extra_fields={
                    "h3:resolution": resolution,
                    "proj:code": "EPSG:4326",
                    "table:primary_geometry": "geom",
                    "table:row_count": len(summary),
                    "table:columns": [
                        {"name": name, "type": dtype, "description": description}
                        for name, (dtype, description) in SUMMARY_COLUMNS.items()
                    ],
                },
            ))
            event("h3_aggregation_finished", year=year, resolution=resolution, cells=len(summary))

        item_path = partition / f"{year}.json"
        item.properties["gcs:uri"] = cloud_uri(destination, item_path)
        for asset in item.assets.values():
            asset_path = partition / asset.href
            asset.extra_fields["gcs:uri"] = cloud_uri(destination, asset_path)
            asset.extra_fields["firms:sha256"] = sha256(output / asset_path)
            files.append(asset_path)
        root = "../" * len(partition.parts)
        collection_href = "../" * (len(partition.parts) - 1) + "collection.json"
        item.add_link(pystac.Link("root", f"{root}catalog.json"))
        item.add_link(pystac.Link("collection", collection_href))
        item.add_link(pystac.Link("parent", collection_href))
        save_stac(output / item_path, item)
        collection.add_link(pystac.Link("item", f"./{item_path.relative_to(collection_path.parent).as_posix()}"))
        files.append(item_path)

    collection.add_link(pystac.Link("root", "../catalog.json"))
    collection.add_link(pystac.Link("parent", "../catalog.json"))
    collection.add_link(pystac.Link("license", "https://www.earthdata.nasa.gov/data/use-policy"))
    save_stac(output / collection_path, collection)
    save_bytes(output / "README.md", DATASET_README)
    save_bytes(output / "AGENTS.md", DATASET_AGENTS)
    stage_root(output)
    staged = pystac.Catalog.from_file(str(output / "catalog.json"))
    staged_collection, = staged.get_children()
    if len(list(staged_collection.get_items())) != len(assets):
        raise ValueError("Staged collection does not link to all annual items")
    return files + [collection_path, Path("README.md"), Path("AGENTS.md"), Path("catalog.json")]


def stage_root(output: Path) -> Path:
    catalog = pystac.Catalog(id="south-america-corner", description="Geospatial datasets for South America.")
    catalog.add_link(pystac.Link("root", "./catalog.json"))
    catalog.add_link(pystac.Link("child", "./occurrences/collection.json"))
    path = Path("catalog.json")
    save_stac(output / path, catalog)
    return path


class GCSUploadError(ValueError):
    def __init__(self, path: Path, reason: str, message: str) -> None:
        self.object_path = path.as_posix()
        self.reason = reason
        super().__init__(f"GCS upload failed for {self.object_path}: {message}")


def upload_dataset(output: Path, destination: str, files: list[Path]) -> None:
    for path in files:
        local = output / path
        uri = cloud_uri(destination, path)
        digest = sha256(local)
        event("gcs_upload_started", uri=uri, bytes=local.stat().st_size)
        result = subprocess.run([
            "gcloud", "storage", "cp", "--if-generation-match=0",
            f"--custom-metadata=sha256={digest}", str(local), uri,
        ], capture_output=True, text=True)
        if result.returncode == 0:
            event("gcs_upload_finished", uri=uri)
            continue
        existing = subprocess.run([
            "gcloud", "storage", "objects", "describe", uri, "--raw",
            "--format=value(metadata.sha256)",
        ], capture_output=True, text=True)
        if existing.returncode == 0:
            if existing.stdout.strip() == digest:
                event("gcs_object_reused", uri=uri)
                continue
            raise GCSUploadError(
                path, "existing_object_checksum_mismatch",
                "the destination object already exists and its SHA-256 metadata differs or is missing. "
                f"Expected SHA-256 {digest}, found {existing.stdout.strip() or 'no checksum'}. "
                "Published assets and cached chunks are immutable.",
            )
        message = result.stderr.strip()
        if "412" in message or "conditionnotmet" in message.lower():
            raise GCSUploadError(
                path, "precondition_failed", "the create-only write failed and the existing checksum "
                "could not be verified. Check the destination and object read permissions.",
            )
        reason = "permission_denied" if any(
            token in message.lower() for token in ("403", "permission denied", "accessdenied")
        ) else "copy_failed"
        raise GCSUploadError(path, reason, message or "gcloud returned no error text")
