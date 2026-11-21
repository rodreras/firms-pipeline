"""Build annual H3 tile archives and MapLibre styles from GeoParquet."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.ingest import write_json
from src.logs import event

PMTILES = "application/vnd.pmtiles"
STYLE = "application/vnd.mapbox.style+json"
WEB_MAP_LINKS = "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json"
MAX_ZOOM = {4: 8, 5: 10}
METRICS = {
    "detection_count": "Detection count",
    "frp_sum": "Total FRP (MW)",
    "frp_avg": "Mean FRP (MW)",
    "frp_max": "Maximum FRP (MW)",
}
COLORS = ("#fff3b0", "#feb24c", "#fd8d3c", "#f03b20", "#bd0026", "#67000d")


def make_style(values: pa.ChunkedArray, archive: str, layer: str, metric: str) -> dict:
    numeric = values.to_numpy().astype(float)
    finite = numeric[np.isfinite(numeric) & (numeric >= 0)]
    breaks = []
    if len(finite):
        candidates = np.quantile(finite, [0.5, 0.75, 0.9, 0.97, 0.995])
        breaks = sorted({float(value) for value in candidates if value > finite.min()})
    color = COLORS[0]
    if breaks:
        color = ["step", ["number", ["get", metric], 0], COLORS[0]]
        for threshold, shade in zip(breaks, COLORS[1:]):
            color.extend([threshold, shade])
    return {
        "version": 8,
        "name": METRICS[metric],
        "metadata": {"firms:metric": metric, "firms:breaks": breaks, "firms:missing_color": "#808080"},
        "sources": {"data": {"type": "vector", "url": f"../{archive}"}},
        "layers": [{
            "id": metric, "type": "fill", "source": "data", "source-layer": layer,
            "paint": {"fill-color": ["case", ["==", ["typeof", ["get", metric]], "number"],
                                     color, "#808080"], "fill-opacity": 0.8},
        }],
    }


def build_visuals(source: Path, output: Path, resolution: int) -> dict[str, dict]:
    import tylertoo

    table = pq.read_table(source)
    if not table.num_rows:
        event("pmtiles_skipped_empty", resolution=resolution, source=source.name)
        return {}
    layer = f"h3-r{resolution}"
    archive = f"{source.stem}.pmtiles"
    output.mkdir(parents=True, exist_ok=True)
    event("pmtiles_build_started", resolution=resolution, source=source.name)
    with TemporaryDirectory(dir=output) as work:
        tile_input = Path(work) / "h3.parquet"
        overview = Path(work) / "overview.parquet"
        index = table.schema.get_field_index("h3_cell")
        converted = table.set_column(index, "h3_cell", pc.cast(table["h3_cell"], pa.string()))
        pq.write_table(converted, tile_input, compression="zstd")
        tylertoo.overview(str(tile_input), str(overview), max_zoom=MAX_ZOOM[resolution])
        report = tylertoo.export_pmtiles(str(overview), str(output / archive), layer_name=layer)
    event("pmtiles_build_finished", resolution=resolution, source=source.name, **report)
    assets = {layer: {
        "href": f"./{archive}", "type": PMTILES, "roles": ["visual"],
        "title": f"H3 resolution {resolution} map",
    }}
    styles = output / "styles"
    styles.mkdir(exist_ok=True)
    for metric in METRICS:
        path = f"styles/{layer}-{metric}.json"
        write_json(output / path, make_style(table[metric], archive, layer, metric))
        assets[f"style-{layer}-{metric}"] = {
            "href": f"./{path}", "type": STYLE, "roles": ["style"],
            "title": f"H3 resolution {resolution} · {METRICS[metric]}",
        }
    return assets


def set_collection_visuals(collection: dict, year: int, item_href: str, assets: dict) -> None:
    if not assets or year < collection.get("firms:visual_year", 0):
        return
    base = Path(item_href).parent
    collection["firms:visual_year"] = year
    extensions = collection.setdefault("stac_extensions", [])
    if WEB_MAP_LINKS not in extensions:
        extensions.append(WEB_MAP_LINKS)
    collection["links"] = [link for link in collection["links"] if link["rel"] != "pmtiles"]
    current = collection.setdefault("assets", {})
    for name in list(current):
        if name.startswith(("h3-r", "style-h3-r")):
            del current[name]
    for name, asset in assets.items():
        entry = {**asset, "href": f"./{(base / asset['href']).as_posix()}"}
        if entry["type"] == PMTILES:
            collection["links"].append({
                "rel": "pmtiles", "pmtiles:layers": [name],
                **{key: entry[key] for key in ("href", "type", "title")},
            })
        current[name] = entry
    default = current.get("style-h3-r4-detection_count") or current["style-h3-r5-detection_count"]
    default["roles"] = ["style", "default"]
