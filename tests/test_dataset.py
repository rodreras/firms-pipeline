import json
import tempfile
import unittest
from contextlib import contextmanager, redirect_stderr
from datetime import date
from io import StringIO
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import Mock, patch

import duckdb
import geopandas as gpd
import pyarrow.parquet as pq
import pystac

from src.dataset import GCSUploadError, stage_dataset, upload_dataset
from src.ingest import run


class DatasetTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.destination = "gs://test-bucket/firms/version-1"

    def test_annual_layout_and_summary(self):
        client = Mock()
        client.availability.return_value = {"MODIS_SP": (date(2026, 1, 1), date(2026, 1, 2))}
        client.area.return_value = (
            b"latitude,longitude,acq_date,acq_time,brightness,bright_t31,confidence,frp\n"
            b"-10,-60,2026-01-01,0010,310,290,80,1\n"
            b"-10,-60,2026-01-02,0010,310,290,80,5\n"
            b"-10,-60,2026-01-02,0020,310,290,80,\n"
            b"-11,-60,2026-01-02,0020,310,290,80,\n"
        )
        source = self.root / "source"
        manifest = run(source, client, refresh=False)
        output = self.root / "dataset"
        with duckdb.connect() as connection, patch("src.dataset.RESOLUTIONS", (6, 9)):
            # Keep this layout and SQL test independent of extension downloads.
            connection.execute("""
                CREATE MACRO h3_latlng_to_cell(lat, lng, resolution) AS
                    CAST(resolution * 100 - lat AS UBIGINT)
            """)
            connection.execute("""
                CREATE MACRO h3_cell_to_boundary_wkt(cell) AS
                    'POLYGON ((-60 -10, -59 -10, -59 -9, -60 -9, -60 -10))'
            """)
            files = stage_dataset(manifest, source, output, self.destination, connection)

        partition = Path("occurrences/year=2026")
        self.assertEqual(set(files), {
            Path("catalog.json"), Path("occurrences/collection.json"),
            Path("README.md"), Path("AGENTS.md"),
            *(partition / name for name in (
                "2026.json", "occurrences.parquet", "aggregate-l6.parquet", "aggregate-l9.parquet",
            )),
        })
        self.assertEqual(files[-1], Path("catalog.json"))
        item, = pystac.Catalog.from_file(str(output / "catalog.json")).get_items(recursive=True)
        self.assertEqual(item.id, "2026")
        self.assertEqual(item.properties["firms:products"], ["MODIS_SP"])
        self.assertEqual(item.assets["data"].extra_fields["gcs:uri"],
                         f"{self.destination}/{partition}/occurrences.parquet")
        self.assertEqual((output / partition / "occurrences.parquet").read_bytes(),
                         (source / manifest["publication"]["assets"][0]["path"]).read_bytes())
        for resolution in (6, 9):
            path = output / partition / f"aggregate-l{resolution}.parquet"
            first, second = pq.ParquetFile(path).read().to_pylist()
            frame = gpd.read_parquet(path)
            self.assertEqual(frame.crs.to_epsg(), 4326)
            self.assertEqual(frame.geometry.geom_type.tolist(), ["Polygon", "Polygon"])
            self.assertTrue(frame.geometry.is_valid.all())
            self.assertEqual(frame.total_bounds.tolist(), [-60.0, -10.0, -59.0, -9.0])
            self.assertIsInstance(first.pop("geom"), bytes)
            self.assertEqual(first, {
                "year": 2026, "h3_cell": resolution * 100 + 10,
                "detection_count": 3, "frp_count": 2,
                "frp_sum": 6.0, "frp_avg": 3.0, "frp_max": 5.0,
            })
            self.assertEqual(second["frp_count"], 0)
            self.assertIsNone(second["frp_sum"])
        self.assertEqual(json.loads((output / partition / "2026.json").read_text())["collection"], "firms-sa")

    def test_upload_reuses_identical_objects_and_stops_on_conflicts(self):
        asset = Path("occurrences/year=2026/occurrences.parquet")
        catalog = Path("catalog.json")
        (self.root / asset.parent).mkdir(parents=True)
        (self.root / asset).write_bytes(b"test")
        (self.root / catalog).write_bytes(b"catalog")
        denied = CompletedProcess([], 1, stderr="precondition failed")
        with patch("src.dataset.subprocess.run") as process:
            process.side_effect = [
                denied, CompletedProcess([], 0, stdout="different"),
            ]
            with self.assertRaisesRegex(GCSUploadError, "immutable") as conflict:
                upload_dataset(self.root, self.destination, [asset, catalog])
            self.assertEqual(conflict.exception.reason, "existing_object_checksum_mismatch")
            self.assertEqual(conflict.exception.object_path, asset.as_posix())
            self.assertEqual(process.call_count, 2)

        with patch("src.dataset.subprocess.run") as process:
            def call(command, **kwargs):
                if "describe" in command:
                    digest = process.call_args_list[0].args[0][4].split("=")[-1]
                    return CompletedProcess([], 0, stdout=digest + "\n")
                if command[-1].endswith(".parquet"):
                    return denied
                return CompletedProcess([], 0)

            process.side_effect = call
            upload_dataset(self.root, self.destination, [asset, catalog])
            self.assertEqual(process.call_args.args[0][-1], f"{self.destination}/catalog.json")
            self.assertIn("--if-generation-match=0", process.call_args.args[0])


class CloudPipelineTests(unittest.TestCase):
    def test_private_chunk_resume_and_publication(self):
        from src.cloud import publish_run
        from src.ingest import BBOX
        from src.gcs_collection import merge_collections

        client = Mock()
        start = date(2026, 1, 1)
        client.availability.return_value = {"MODIS_SP": (start, start)}
        client.area.return_value = (
            b"latitude,longitude,acq_date,acq_time,brightness,bright_t31,confidence,frp\n"
            b"-10,-60,2026-01-01,0010,310,290,80,3\n"
        )
        public = "gs://test/firms"
        private = "gs://test/operations"
        objects = {}
        uploaded = []
        fail_catalog = True

        def load_h3(connection):
            connection.execute("""
                CREATE MACRO h3_latlng_to_cell(lat, lng, resolution) AS CAST(resolution AS UBIGINT);
                CREATE MACRO h3_cell_to_boundary_wkt(cell) AS
                    'POLYGON ((-60 -10, -59 -10, -59 -9, -60 -9, -60 -10))';
            """)

        def upload(output, destination, paths):
            for path in paths:
                uri = f"{destination}/{path.as_posix()}"
                if fail_catalog and uri == f"{public}/catalog.json":
                    raise GCSUploadError(path, "copy_failed", "catalog upload interrupted")
                data = (output / path).read_bytes()
                self.assertEqual(objects.setdefault(uri, data), data)
                uploaded.append(uri)

        def download(uri, target, *, optional=False):
            if uri not in objects and optional:
                return False
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(objects[uri])
            return True

        def publish_collection(output, destination, *, expected_items=None):
            path = "occurrences/collection.json"
            uri = f"{destination}/{path}"
            incoming = json.loads((output / path).read_bytes())
            previous = json.loads(objects[uri]) if uri in objects else None
            objects[uri] = json.dumps(merge_collections(previous, incoming) if previous else incoming).encode()
            uploaded.append(uri)

        def read_collection(uri):
            return ("1", json.loads(objects[uri])) if uri in objects else ("0", None)

        @contextmanager
        def update_annual_objects(output, destination, paths, expected, *, backup_destination):
            upload(output, destination, paths)
            yield

        console = StringIO()
        with (
            redirect_stderr(console),
            patch("src.dataset.RESOLUTIONS", (6, 9)),
            patch("src.cloud.read_collection", side_effect=read_collection),
            patch("src.cloud.update_annual_objects", side_effect=update_annual_objects),
            patch("src.cloud.publish_collection", side_effect=publish_collection),
            patch("src.cloud.load_h3", side_effect=load_h3),
            patch("src.cloud.upload_dataset", side_effect=upload),
            patch("src.cloud.get_object", side_effect=download),
        ):
            with self.assertRaisesRegex(GCSUploadError, "catalog upload interrupted"):
                publish_run(client, public, private, start, start, BBOX)
            self.assertNotIn(f"{public}/catalog.json", objects)
            failed_record, = [
                json.loads(data) for uri, data in objects.items() if uri.endswith("/manifest.json")
            ]
            fail_catalog = False
            second = publish_run(client, public, private, start, start, BBOX)
            publish_run(client, public, private, start, start, BBOX)
            client.area.assert_called_once()
            older = date(2025, 1, 1)
            client.availability.return_value = {"MODIS_SP": (older, older)}
            client.area.return_value = client.area.return_value.replace(b"2026-01-01", b"2025-01-01")
            publish_run(client, public, private, older, older, BBOX)

        self.assertIn('"level": "ERROR"', console.getvalue())
        self.assertIn('"event": "h3_aggregation_started"', console.getvalue())
        self.assertEqual(failed_record["outcome"], "failed")
        self.assertEqual(failed_record["error_reason"], "copy_failed")
        self.assertEqual(failed_record["failed_object"], "catalog.json")
        self.assertIn(b'"failed_object": "catalog.json"',
                      objects[f"{private}/runs/{failed_record['run_id']}/events.ndjson"])
        self.assertEqual(failed_record["cloud_publication"]["outcome"], "failed")
        self.assertEqual(failed_record["chunks"][0]["action"], "downloaded")
        second_record = json.loads(objects[f"{private}/runs/{second}/manifest.json"])
        self.assertEqual(second_record["chunks"], [])
        self.assertEqual(second_record["cloud_publication"]["outcome"], "complete")
        self.assertIn(b"request_already_published", objects[f"{private}/runs/{second}/events.ndjson"])
        collection = f"{public}/occurrences/collection.json"
        document = json.loads(objects[collection])
        self.assertEqual(
            [link["href"] for link in document["links"] if link["rel"] == "item"],
            [
                next(link["href"] for link in document["links"]
                     if link["rel"] == "item" and link["href"].endswith(f"/{year}.json"))
                for year in (2025, 2026)
            ],
        )
        first, last = document["extent"]["temporal"]["interval"][0]
        self.assertTrue(first.startswith("2025-01-01"))
        self.assertTrue(last.startswith("2026-01-01"))
        current = next(link["href"] for link in document["links"] if link["href"].startswith("./year=2026/"))
        item_uri = f"{public}/occurrences/{current.removeprefix('./')}"
        item = json.loads(objects[item_uri])
        aggregate_uri = item["assets"]["aggregate-l9"]["gcs:uri"]
        self.assertEqual(aggregate_uri, item_uri.removesuffix("2026.json") + "aggregate-l9.parquet")
        self.assertIn(aggregate_uri, objects)
        self.assertEqual(
            [uri for uri in uploaded if uri.startswith(public)][-1], f"{public}/catalog.json"
        )
        self.assertTrue(all("/runs/" not in uri and "/chunks/" not in uri
                            for uri in objects if uri.startswith(public)))


class CollectionUpdateTests(unittest.TestCase):
    def test_generation_conflict_rereads_and_preserves_concurrent_year(self):
        from src.gcs_collection import COLLECTION_PATH, publish_collection

        def collection(year):
            return {
                "id": "firms-sa",
                "links": [
                    {"rel": "root", "href": "../catalog.json"},
                    {"rel": "item", "href": f"./year={year}/{year}.json"},
                ],
                "extent": {
                    "spatial": {"bbox": [[-60, -10, -50, 0]]},
                    "temporal": {"interval": [[f"{year}-01-01T00:00:00Z", f"{year}-01-31T00:00:00Z"]]},
                },
            }

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / COLLECTION_PATH
            path.parent.mkdir()
            path.write_text(json.dumps(collection(2025)))
            with (
                patch("src.gcs_collection.read_collection", side_effect=[
                    ("0", None), ("42", collection(2026)),
                ]),
                patch("src.gcs_collection.subprocess.run", side_effect=[
                    CompletedProcess([], 1, stderr="HTTP 412 conditionNotMet"),
                    CompletedProcess([], 0),
                ]) as command,
            ):
                publish_collection(root, "gs://test/firms")

            self.assertIn("--if-generation-match=0", command.call_args_list[0].args[0])
            self.assertIn("--if-generation-match=42", command.call_args_list[1].args[0])
            result = json.loads(path.read_bytes())
            self.assertEqual([link["href"] for link in result["links"] if link["rel"] == "item"], [
                "./year=2025/2025.json", "./year=2026/2026.json",
            ])
