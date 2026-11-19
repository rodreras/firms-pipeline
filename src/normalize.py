"""Convert FIRMS area responses into a common VIIRS NOAA-21-based schema."""

import csv
import io
import json
import math
from datetime import date, datetime, time, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from shapely import points, to_wkb

from src.firms_http import csv_rows

SCHEMA_VERSION = 1
FLOAT_FIELDS = ("bright_ti4", "scan", "track", "bright_ti5", "frp", "brightness", "bright_t31")
TEXT_FIELDS = ("satellite", "instrument", "confidence", "version", "daynight")
COLUMNS = [
    ("latitude", pa.float64(), "Detection latitude in degrees."),
    ("longitude", pa.float64(), "Detection longitude in degrees."),
    ("bright_ti4", pa.float64(), "VIIRS I-4 brightness temperature in kelvin."),
    ("scan", pa.float64(), "Along-scan pixel size in kilometers."),
    ("track", pa.float64(), "Along-track pixel size in kilometers."),
    ("acq_date", pa.date32(), "UTC acquisition date."),
    ("acq_time", pa.string(), "UTC acquisition time in HHMM format."),
    ("satellite", pa.string(), "Satellite identifier supplied by FIRMS."),
    ("instrument", pa.string(), "Instrument identifier supplied by FIRMS."),
    ("confidence", pa.string(), "Source confidence. VIIRS uses l, n, h. MODIS uses 0–100."),
    ("version", pa.string(), "FIRMS product version and processing designation."),
    ("bright_ti5", pa.float64(), "VIIRS I-5 brightness temperature in kelvin."),
    ("frp", pa.float64(), "Fire radiative power in megawatts."),
    ("daynight", pa.string(), "Day or night flag supplied by FIRMS."),
    ("brightness", pa.float64(), "MODIS channel 21/22 brightness temperature in kelvin."),
    ("bright_t31", pa.float64(), "MODIS channel 31 brightness temperature in kelvin."),
    ("acquired_at", pa.timestamp("us", tz="+00:00"), "UTC acquisition timestamp."),
    ("source", pa.string(), "FIRMS API product identifier."),
    ("source_attributes", pa.string(), "Additional source columns encoded as a JSON object."),
]
GEOMETRY = "geom"
COVERING = "bbox"


def numeric(value: str) -> float | None:
    if not value:
        return None
    number = float(value)
    if not math.isfinite(number):
        raise ValueError
    return number


def normalize(body: bytes, source: str, start: date, end: date) -> pa.Table:
    modis = source.startswith("MODIS_")
    bands = {"brightness", "bright_t31"} if modis else {"bright_ti4", "bright_ti5"}
    required = {"latitude", "longitude", "acq_date", "acq_time", "confidence"} | bands
    observations = []
    row_number = 1
    try:
        rows = csv_rows(body)
        header = next(rows)
        if not required.issubset(header) or len(header) != len(set(header)):
            raise ValueError

        known = {name for name, _, _ in COLUMNS}
        for row_number, values in enumerate(rows, start=2):
            if not values:
                continue
            if len(values) != len(header):
                raise ValueError
            row = dict(zip(header, values, strict=True))
            latitude, longitude = float(row["latitude"]), float(row["longitude"])
            if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                raise ValueError
            acquired = date.fromisoformat(row["acq_date"])
            hhmm = row["acq_time"]
            if not (hhmm.isascii() and hhmm.isdecimal() and len(hhmm) <= 4):
                raise ValueError
            hhmm = hhmm.zfill(4)
            clock = time(int(hhmm[:2]), int(hhmm[2:]), tzinfo=timezone.utc)
            confidence = row["confidence"]
            if modis:
                if not (confidence.isascii() and confidence.isdecimal() and 0 <= int(confidence) <= 100):
                    raise ValueError
            elif confidence.lower() not in {"l", "n", "h"}:
                raise ValueError
            if not start <= acquired <= end:
                raise ValueError
            observations.append({
                **{name: numeric(row.get(name, "")) for name in FLOAT_FIELDS},
                **{name: row.get(name) or None for name in TEXT_FIELDS},
                "latitude": latitude,
                "longitude": longitude,
                "acq_date": acquired,
                "acq_time": hhmm,
                "acquired_at": datetime.combine(acquired, clock),
                "source": source,
                "source_attributes": json.dumps(
                    {key: value for key, value in row.items() if key not in known}, sort_keys=True
                ),
            })
    except (StopIteration, csv.Error, ValueError, OverflowError):
        raise ValueError(f"Invalid FIRMS {source} schema or values at row {row_number}") from None
    return pa.Table.from_pylist(observations, schema=pa.schema([
        (name, dtype) for name, dtype, _ in COLUMNS
    ]))


def geoparquet(table: pa.Table, bbox: list[float] | None = None) -> bytes:
    longitude = table["longitude"].to_numpy()
    latitude = table["latitude"].to_numpy()
    geometry_wkb = pa.array(to_wkb(points(longitude, latitude)), type=pa.binary())
    table = table.append_column(GEOMETRY, geometry_wkb)
    bounds = pa.StructArray.from_arrays(
        [pa.array(longitude), pa.array(latitude), pa.array(longitude), pa.array(latitude)],
        names=["xmin", "ymin", "xmax", "ymax"],
    )
    table = table.append_column(COVERING, bounds)
    geometry = {
        "encoding": "WKB",
        "geometry_types": ["Point"],
        "crs": CRS.from_epsg(4326).to_json_dict(),
        "covering": {"bbox": {name: [COVERING, name] for name in ("xmin", "ymin", "xmax", "ymax")}},
    }
    if bbox is not None:
        geometry["bbox"] = bbox
    metadata = {"version": "1.1.0", "primary_column": GEOMETRY, "columns": {GEOMETRY: geometry}}
    table = table.replace_schema_metadata({"geo": json.dumps(metadata)})
    output = io.BytesIO()
    pq.write_table(table, output, compression="zstd")
    return output.getvalue()
