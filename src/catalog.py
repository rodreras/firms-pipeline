"""Publish yearly GeoParquet and a self-contained local STAC catalog."""

import hashlib
import json
from collections import defaultdict
from datetime import date, datetime, time, timezone
from pathlib import Path

import geopandas as gpd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pystac
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.table import Column, TableExtension
from shapely.geometry import MultiPoint, mapping

from src.local_files import atomic_write
from src.normalize import COLUMNS, COVERING, GEOMETRY, geoparquet

PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"


def save_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.read_bytes() != data:
        atomic_write(path, data)


def save_stac(path: Path, document: pystac.STACObject) -> None:
    content = document.to_dict(include_self_link=False, transform_hrefs=False)
    save_bytes(path, (json.dumps(content, indent=2) + "\n").encode())


def yearly_asset(table: pa.Table, year: int, start: date, end: date) -> tuple[bytes, pystac.Item]:
    bbox = footprint = None
    first = datetime.combine(start, time.min, timezone.utc)
    last = datetime.combine(end, time.max, timezone.utc)
    if len(table):
        longitude = pc.min_max(table["longitude"]).as_py()
        latitude = pc.min_max(table["latitude"]).as_py()
        bbox = [longitude["min"], latitude["min"], longitude["max"], latitude["max"]]
        footprint = mapping(MultiPoint([
            (bbox[0], bbox[1]), (bbox[0], bbox[3]), (bbox[2], bbox[1]), (bbox[2], bbox[3]),
        ]).convex_hull)
        points = gpd.GeoSeries(gpd.points_from_xy(table["longitude"], table["latitude"], crs=4326))
        hilbert = pa.array(points.hilbert_distance(total_bounds=[-180, -90, 180, 90]))
        ordered = table.append_column("_hilbert", hilbert)
        indices = pc.sort_indices(ordered, sort_keys=[
            ("_hilbert", "ascending"), ("acquired_at", "ascending"),
            ("source", "ascending"), ("longitude", "ascending"), ("latitude", "ascending"),
        ])
        table = table.take(indices)
        times = pc.min_max(table["acquired_at"]).as_py()
        first, last = times["min"], times["max"]
    item = pystac.Item(
        id=f"firms-sa-{year}", geometry=footprint, bbox=bbox, datetime=None,
        start_datetime=first, end_datetime=last, properties={},
    )
    extension = TableExtension.ext(item, add_if_missing=True)
    extension.row_count = len(table)
    extension.primary_geometry = GEOMETRY
    extension.columns = [
        Column({"name": name, "type": str(dtype), "description": description})
        for name, dtype, description in COLUMNS
    ] + [
        Column({"name": GEOMETRY, "type": "WKB", "description": "Detection point."}),
        Column({"name": COVERING, "type": "struct", "description": "Point bounds in WGS 84 degrees."}),
    ]
    ProjectionExtension.ext(item, add_if_missing=True).code = "EPSG:4326"
    item.add_asset("data", pystac.Asset(
        href=f"./firms-sa-{year}.parquet", media_type=PARQUET_MEDIA_TYPE, roles=["data"],
    ))
    return geoparquet(table, bbox), item


def publish_catalog(output: Path, manifest: dict) -> dict:
    """Commit the catalog entry point after its versioned assets and metadata."""
    by_year = defaultdict(list)
    for chunk in manifest["chunks"]:
        table = pq.ParquetFile(output / chunk["output"]).read().drop([GEOMETRY, COVERING])
        year = date.fromisoformat(chunk["requested_start"]).year
        by_year[year].append(table)

    assets = {}
    for year, tables in sorted(by_year.items()):
        start = max(date(year, 1, 1), date.fromisoformat(manifest["requested_start"]))
        end = min(date(year, 12, 31), date.fromisoformat(manifest["requested_end"]))
        assets[year] = yearly_asset(pa.concat_tables(tables), year, start, end)

    catalog = pystac.Catalog(
        id="south-america-corner", description="Geospatial datasets for South America.",
    )
    items = [item for _, item in assets.values()]
    boxes = [item.bbox for item in items if item.bbox is not None]
    if boxes:
        bbox = [min(b[0] for b in boxes), min(b[1] for b in boxes),
                max(b[2] for b in boxes), max(b[3] for b in boxes)]
    else:
        bbox = [float(value) for value in manifest["bbox"].split(",")]
    collection = pystac.Collection(
        id="firms-sa",
        description="NASA FIRMS active fire and thermal anomaly detections within the South America request bounding box.",
        license="other",
        extent=pystac.Extent(
            pystac.SpatialExtent([bbox]),
            pystac.TemporalExtent([[
                min(item.common_metadata.start_datetime for item in items),
                max(item.common_metadata.end_datetime for item in items),
            ]]),
        ),
        providers=[pystac.Provider(
            name="NASA FIRMS", roles=[pystac.ProviderRole.PRODUCER],
            url="https://firms.modaps.eosdis.nasa.gov/",
        )],
    )
    collection.add_link(pystac.Link("license", "https://www.earthdata.nasa.gov/data/use-policy"))
    digest = hashlib.sha256(json.dumps(collection.to_dict(), sort_keys=True).encode())
    for data, item in assets.values():
        digest.update(hashlib.sha256(data).digest())
        digest.update(hashlib.sha256(json.dumps(item.to_dict(), sort_keys=True).encode()).digest())
    release = Path("releases") / digest.hexdigest()
    catalog.add_link(pystac.Link("root", "./catalog.json"))
    catalog.add_link(pystac.Link("child", f"./{release.as_posix()}/firms-sa/collection.json"))
    collection.add_link(pystac.Link("root", "../../../catalog.json"))
    collection.add_link(pystac.Link("parent", "../../../catalog.json"))

    outputs = []
    for year, (data, item) in assets.items():
        partition = release / "firms-sa" / str(year)
        item.collection_id = collection.id
        item.add_link(pystac.Link("collection", "../collection.json"))
        item.add_link(pystac.Link("parent", "../collection.json"))
        item.add_link(pystac.Link("root", "../../../../catalog.json"))
        collection.add_link(pystac.Link("item", f"./{year}/{item.id}.json"))
        parquet = partition / f"{item.id}.parquet"
        save_bytes(output / parquet, data)
        save_stac(output / partition / f"{item.id}.json", item)
        outputs.append({
            "year": year,
            "path": parquet.as_posix(),
            "rows": sum(len(table) for table in by_year[year]),
            "sha256": hashlib.sha256(data).hexdigest(),
            "byte_size": len(data),
        })
    save_stac(output / release / "firms-sa" / "collection.json", collection)
    save_stac(output / "catalog.json", catalog)
    return {"catalog": "catalog.json", "assets": outputs}
