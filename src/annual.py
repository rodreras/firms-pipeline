"""Merge normalized annual observations using source and UTC observation date."""

from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.normalize import COVERING, GEOMETRY


def interval_days(interval: list[str]) -> set[date]:
    start, end = map(date.fromisoformat, interval)
    return {start + timedelta(days=offset) for offset in range((end - start).days + 1)}


def chunk_coverage(chunks: list[dict]) -> dict[str, set[date]]:
    coverage: dict[str, set[date]] = {}
    for chunk in chunks:
        interval = [chunk["requested_start"], chunk["requested_end"]]
        coverage.setdefault(chunk["source"], set()).update(interval_days(interval))
    return coverage


def compact_coverage(coverage: dict[str, set[date]]) -> dict[str, list[list[str]]]:
    compact = {}
    for source, days in sorted(coverage.items()):
        spans = []
        for day in sorted(days):
            if spans and (day - spans[-1][1]).days == 1:
                spans[-1][1] = day
            else:
                spans.append([day, day])
        compact[source] = [[start.isoformat(), end.isoformat()] for start, end in spans]
    return compact


def item_coverage(properties: dict) -> dict[str, set[date]]:
    return {
        source: set().union(*(interval_days(interval) for interval in intervals))
        for source, intervals in properties.get("firms:coverage", {}).items()
    }


def merge_observations(
    previous: Path, incoming: Path, replacements: dict[str, set[date]], year: int,
) -> pa.Table:
    old = pq.ParquetFile(previous).read().drop([GEOMETRY, COVERING])
    new = pq.ParquetFile(incoming).read().drop([GEOMETRY, COVERING])
    if len(old) and (pc.year(old["acq_date"]).unique().to_pylist() != [year]):
        raise ValueError(f"Published occurrences contain dates from outside {year}")
    keep = pa.array([True] * len(old), type=pa.bool_())
    for source, days in replacements.items():
        replace = pc.and_(
            pc.equal(old["source"], source),
            pc.is_in(old["acq_date"], value_set=pa.array(sorted(days), type=pa.date32())),
        )
        keep = pc.and_(keep, pc.invert(replace))
    return pa.concat_tables([old.filter(keep), new])
