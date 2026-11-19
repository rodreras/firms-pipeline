import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from urllib.error import HTTPError
from unittest.mock import Mock, patch

import duckdb
import geopandas as gpd
import pyarrow.parquet as pq
import pystac

from src.firms_http import FirmsClient
from src.ingest import Chunk, atomic_write, download_chunk, plan_chunks, run


class IngestTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name)
        self.chunk = Chunk("MODIS_SP", date(2026, 1, 1), 5)
        self.body = (
            b"latitude,longitude,acq_date,acq_time,brightness,bright_t31,confidence\n"
            b"-10,-60,2026-01-01,0010,310,290,80\n"
        )

    def download(self, fetch=None, refresh=False):
        return download_chunk(
            self.output, self.chunk, fetch or (lambda _: self.body), refresh=refresh
        )

    def test_plan_intersects_january_and_product_availability(self):
        availability = {
            "MODIS_SP": (date(2000, 11, 1), date(2026, 1, 7)),
            "MODIS_NRT": (date(2026, 2, 1), date(2026, 2, 10)),
        }
        self.assertEqual(list(plan_chunks(availability)), [
            self.chunk, Chunk("MODIS_SP", date(2026, 1, 6), 2)
        ])

    def test_custom_window_preserves_bbox_and_splits_at_year_boundary(self):
        start, end = date(2025, 12, 30), date(2026, 1, 3)
        bbox = "-70,-20,-50,0"
        self.assertEqual(list(plan_chunks({"MODIS_SP": (start, end)}, start, end, bbox)), [
            Chunk("MODIS_SP", start, 2, bbox),
            Chunk("MODIS_SP", date(2026, 1, 1), 3, bbox),
        ])

    def test_offline_import_verifies_csv_and_preserves_download_time(self):
        import hashlib

        raw = self.output / "raw"
        runs = raw / "runs"
        runs.mkdir(parents=True)
        csv = raw / "firms.csv"
        csv.write_bytes(self.body)
        manifest_path = runs / "raw.json"
        chunk = {
            **self.chunk.reference(),
            "outcome": "complete",
            "output": csv.name,
            "byte_size": len(self.body),
            "sha256": hashlib.sha256(self.body).hexdigest(),
            "downloaded_at": "2026-11-18T21:39:09+00:00",
        }
        manifest_path.write_text(json.dumps({
            **self.chunk.reference(),
            "requested_end": "2026-01-31",
            "outcome": "complete",
            "chunks": [chunk],
        }))
        published = self.output / "published"
        result = run(published, None, refresh=False, csv_manifest=manifest_path)
        self.assertEqual(result["chunks"][0]["downloaded_at"], chunk["downloaded_at"])
        self.assertEqual(result["publication"]["assets"][0]["rows"], 1)
        root = published / result["publication"]["catalog"]
        original = root.read_bytes()
        csv.write_bytes(self.body.replace(b"-60", b"-61"))
        with self.assertRaisesRegex(RuntimeError, "CSV checksum or size mismatch"):
            run(published, None, refresh=False, csv_manifest=manifest_path)
        self.assertEqual(root.read_bytes(), original)

    def test_verified_chunk_is_skipped_and_corruption_is_repaired(self):
        status, record = self.download()
        self.assertEqual(status, "downloaded")
        self.assertEqual(record["source_row_count"], 1)
        self.assertEqual(self.download(lambda _: self.fail("duplicate request"))[0], "skipped")
        (self.output / record["output"]).write_bytes(b"corrupt")
        self.assertEqual(self.download()[0], "downloaded")
        self.assertEqual(pq.read_table(self.output / record["output"])["longitude"].to_pylist(), [-60.0])

    def test_interruption_before_completion_keeps_previous_version(self):
        _, original = self.download()
        replacement = self.body.replace(b"-60", b"-61")

        def interrupt(path, data):
            if path.suffix == ".json":
                raise OSError("interrupted")
            atomic_write(path, data)

        with patch("src.ingest.atomic_write", side_effect=interrupt):
            with self.assertRaises(OSError):
                self.download(lambda _: replacement, refresh=True)
        self.assertEqual(self.download(), ("skipped", original))
        status, updated = self.download(lambda _: replacement, refresh=True)
        self.assertEqual(status, "changed")
        self.assertNotEqual(updated["sha256"], original["sha256"])

    def test_invalid_response_does_not_complete_chunk(self):
        with self.assertRaises(ValueError):
            self.download(lambda _: b"Invalid MAP_KEY")
        self.assertEqual(self.download()[0], "downloaded")

    def test_header_only_response_is_a_completed_empty_chunk(self):
        _, record = self.download(lambda _: self.body.splitlines(keepends=True)[0])
        self.assertEqual(record["source_row_count"], 0)
        self.assertEqual(self.download()[0], "skipped")

    def test_failed_run_resumes_without_downloading_completed_chunk(self):
        client = Mock()
        client.availability.return_value = {
            "MODIS_SP": (date(2026, 1, 1), date(2026, 1, 6))
        }
        client.area.side_effect = [self.body, ValueError("invalid response")]
        with self.assertRaises(ValueError):
            run(self.output, client, refresh=False)
        client.area.reset_mock()
        client.area.side_effect = None
        client.area.return_value = self.body.replace(b"2026-01-01", b"2026-01-06")
        manifest = run(self.output, client, refresh=False)
        self.assertEqual(manifest["outcome"], "complete")
        self.assertEqual([chunk["action"] for chunk in manifest["chunks"]], [
            "skipped", "downloaded"
        ])
        self.assertEqual(client.area.call_count, 1)

    def test_identical_refresh_keeps_existing_completion(self):
        _, original = self.download()
        self.assertEqual(self.download(refresh=True), ("unchanged", original))

    def test_january_twice_with_http_retry_and_yearly_stac(self):
        area_calls = []
        availability = (
            b"data_id,min_date,max_date\nMODIS_SP,2000-11-01,2026-01-31\n"
            b"VIIRS_NOAA20_SP,2026-01-31,2026-01-31\n"
            b"VIIRS_NOAA21_SP,2026-01-31,2026-01-31\n"
        )

        def open_response(url, *, timeout):
            if "/mapkey_status/" in url:
                return io.BytesIO(b'{"current_transactions":0,"transaction_limit":5000}')
            if "/data_availability/" in url:
                return io.BytesIO(availability)
            _, source, _, _, start = url.rsplit("/", 4)
            area_calls.append((source, start))
            if len(area_calls) == 1:
                raise HTTPError(url, 503, "", {}, io.BytesIO())
            if source.startswith("VIIRS"):
                return io.BytesIO(
                    b"latitude,longitude,acq_date,acq_time,bright_ti4,bright_ti5,confidence\n"
                    + f"-11,-61,{start},23,335,290,h\n".encode()
                )
            return io.BytesIO(self.body.replace(b"2026-01-01", start.encode()))

        with (
            patch("src.firms_http.urlopen", side_effect=open_response),
            patch("src.firms_http.time.sleep") as sleep,
        ):
            first = run(self.output, FirmsClient("dummy", 5000), refresh=False)
            root = self.output / "catalog.json"
            first_mtime = root.stat().st_mtime_ns
            second = run(self.output, FirmsClient("dummy", 5000), refresh=False)
        self.assertEqual(area_calls[:2], [("MODIS_SP", "2026-01-01")] * 2)
        self.assertEqual(len(area_calls), 10)
        sleep.assert_called_once()
        self.assertTrue(all(chunk["action"] == "skipped" for chunk in second["chunks"]))
        self.assertEqual(first["publication"], second["publication"])
        self.assertEqual(first_mtime, root.stat().st_mtime_ns)
        collection = next(pystac.Catalog.from_file(str(root)).get_children())
        item = next(collection.get_items())
        self.assertEqual(item.id, "firms-sa-2026")
        self.assertEqual(item.collection_id, collection.id)
        asset = self.output / first["publication"]["assets"][0]["path"]
        self.assertEqual(Path(item.assets["data"].get_absolute_href()).resolve(), asset)
        self.assertFalse(list(self.output.rglob("*.csv")))
        parquet = pq.ParquetFile(asset)
        self.assertEqual(parquet.metadata.num_rows, 9)
        self.assertEqual(parquet.metadata.row_group(0).column(0).compression, "ZSTD")
        geo = json.loads(parquet.schema_arrow.metadata[b"geo"])
        self.assertEqual(geo["version"], "1.1.0")
        self.assertEqual(item.bbox, [-61.0, -11.0, -60.0, -10.0])
        self.assertEqual(geo["columns"]["geom"]["bbox"], item.bbox)
        frame = gpd.read_parquet(asset)
        self.assertEqual(frame.crs.to_epsg(), 4326)
        self.assertEqual(frame.geometry.iloc[0].geom_type, "Point")
        self.assertEqual(len(gpd.read_parquet(asset, bbox=(-62, -12, -61, -10))), 2)
        modis = frame[frame.source == "MODIS_SP"]
        viirs = frame[frame.source.str.startswith("VIIRS")]
        self.assertTrue(modis.bright_ti4.isna().all())
        self.assertEqual(set(modis.confidence), {"80"})
        self.assertEqual(set(viirs.bright_ti4), {335})
        self.assertEqual(set(viirs.acq_time), {"0023"})
        with duckdb.connect() as connection:
            count = connection.execute(
                "SELECT count(*) FROM read_parquet(?) WHERE longitude BETWEEN -62 AND -61",
                [str(asset)],
            ).fetchone()[0]
        self.assertEqual(count, 2)

    def test_failed_refresh_preserves_published_catalog(self):
        client = Mock()
        client.availability.return_value = {"MODIS_SP": (date(2026, 1, 1), date(2026, 1, 6))}
        second = self.body.replace(b"2026-01-01", b"2026-01-06")
        client.area.side_effect = [self.body, second]
        first = run(self.output, client, refresh=False)
        root = self.output / first["publication"]["catalog"]
        asset = self.output / first["publication"]["assets"][0]["path"]
        original_root, original_asset = root.read_bytes(), asset.read_bytes()
        client.area.side_effect = [self.body.replace(b"-60", b"-62"), ValueError("invalid response")]
        with self.assertRaises(ValueError):
            run(self.output, client, refresh=True)
        self.assertEqual(root.read_bytes(), original_root)
        self.assertEqual(asset.read_bytes(), original_asset)
        client.area.side_effect = [self.body.replace(b"-60", b"-62"), second]
        corrected = run(self.output, client, refresh=True)
        updated = self.output / corrected["publication"]["assets"][0]["path"]
        self.assertNotEqual(updated, asset)
        self.assertEqual(sorted(pq.read_table(updated)["longitude"].to_pylist()), [-62.0, -60.0])
        self.assertEqual(asset.read_bytes(), original_asset)
        self.assertNotEqual(root.read_bytes(), original_root)

    def test_empty_month_can_be_cataloged(self):
        client = Mock()
        client.availability.return_value = {"MODIS_SP": (date(2026, 1, 1), date(2026, 1, 1))}
        client.area.return_value = self.body.splitlines(keepends=True)[0]
        manifest = run(self.output, client, refresh=False)
        catalog = pystac.Catalog.from_file(str(self.output / "catalog.json"))
        item = next(next(catalog.get_children()).get_items())
        self.assertIsNone(item.geometry)
        self.assertEqual(manifest["publication"]["assets"][0]["rows"], 0)

