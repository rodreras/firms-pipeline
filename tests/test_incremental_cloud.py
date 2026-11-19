import io
import json
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow.parquet as pq

from src.cloud import publish_run
from src.dataset import GCSUploadError, RESOLUTIONS
from src.ingest import BBOX


class MemoryGCS:
    def __init__(self):
        self.objects = {}
        self.reject_collection = False

    def __call__(self, command, **kwargs):
        def result(code=0, stdout="", stderr=""):
            return subprocess.CompletedProcess(command, code, stdout, stderr)

        if command[0] == "git":
            return result(stdout="test-revision")
        action = command[2]
        if action == "objects":
            obj = self.objects.get(command[4])
            if obj is None:
                return result(1, stderr="HTTP error 404")
            _, generation, digest = obj
            return result(stdout=digest if "metadata.sha256" in command[-1] else str(generation))
        if action == "cat":
            uri, generation = command[-1].split("#")
            data, current, _ = self.objects[uri]
            if current != int(generation):
                return result(1, stderr="HTTP error 404")
            return result(stdout=data.decode())
        if action == "cp" and command[-1].startswith("gs://"):
            uri = command[-1]
            generation = self.objects.get(uri, (None, 0, None))[1]
            expected = int(command[3].partition("=")[2])
            if expected != generation:
                return result(1, stderr="HTTP 412 conditionNotMet")
            if self.reject_collection and uri.endswith("/occurrences/collection.json"):
                return result(1, stderr="HTTP 403")
            self.objects[uri] = (
                Path(command[-2]).read_bytes(), generation + 1, command[4].split("=")[-1],
            )
            return result()
        uri, _, generation = command[-2].partition("#")
        if uri not in self.objects or (generation and int(generation) != self.objects[uri][1]):
            return result(1, stderr="HTTP error 404")
        Path(command[-1]).write_bytes(self.objects[uri][0])
        return result()


class IncrementalCloudTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryGCS()
        self.public = "gs://test/firms"
        self.private = "gs://test/operations"
        self.client = Mock()
        self.client.availability.return_value = {
            "MODIS_SP": (date(2020, 1, 1), date(2026, 3, 1)),
        }
        self.client.area.side_effect = self.csv_data
        patches = (
            patch("subprocess.run", side_effect=self.store),
            patch("src.cloud.load_h3", side_effect=self.load_h3),
            redirect_stderr(io.StringIO()),
        )
        for context in patches:
            self.enterContext(context)

    @staticmethod
    def load_h3(connection):
        connection.execute("""
            CREATE MACRO h3_latlng_to_cell(lat, lng, resolution) AS CAST(resolution AS UBIGINT);
            CREATE MACRO h3_cell_to_boundary_wkt(cell) AS
                'POLYGON ((-60 -10, -59 -10, -59 -9, -60 -9, -60 -10))';
        """)

    @staticmethod
    def csv_data(source, bbox, start, days):
        columns = "latitude,longitude,acq_date,acq_time,brightness,bright_t31,confidence,frp\n"
        rows = [
            f"-10,-60,{day},0010,310,290,80,{day.day}\n"
            for day in (start + timedelta(days=index) for index in range(days))
        ]
        return (columns + "".join(rows)).encode()

    def publish(self, first, last):
        return publish_run(self.client, self.public, self.private,
                           date.fromisoformat(first), date.fromisoformat(last), BBOX)

    def collection(self):
        return json.loads(self.store.objects[f"{self.public}/occurrences/collection.json"][0])

    def items(self):
        return {
            item["id"]: item for item in (
                json.loads(self.store.objects[f"{self.public}/occurrences/{link['href'].removeprefix('./')}"][0])
                for link in self.collection()["links"] if link["rel"] == "item"
            )
        }

    def rows(self, asset):
        return pq.ParquetFile(io.BytesIO(self.store.objects[asset["gcs:uri"]][0])).read().to_pylist()

    def test_add_year_then_extend_with_overlap_and_retry(self):
        self.publish("2020-12-31", "2020-12-31")
        self.publish("2021-11-18", "2021-11-19")
        previous = self.items()["2021"]
        self.assertEqual(len(self.rows(previous["assets"]["data"])), 2)
        self.client.area.reset_mock()

        self.publish("2021-11-19", "2021-11-20")
        self.client.area.assert_called_once_with("MODIS_SP", BBOX, date(2021, 11, 20), 1)
        self.publish("2021-11-19", "2021-11-20")
        self.client.area.assert_called_once()

        items = self.items()
        self.assertEqual(set(items), {"2020", "2021"})
        current = items["2021"]
        self.assertEqual(current["properties"]["firms:requested_end"], "2021-11-20")
        self.assertEqual(sorted(row["acq_date"].day for row in self.rows(current["assets"]["data"])),
                         [18, 19, 20])
        expected = f"{self.public}/occurrences/year=2021"
        self.assertEqual(current["properties"]["gcs:uri"], f"{expected}/2021.json")
        self.assertEqual(current["assets"]["data"]["gcs:uri"], f"{expected}/occurrences.parquet")
        self.assertFalse(any("/versions/" in uri for uri in self.store.objects if uri.startswith(self.public)))
        for resolution in RESOLUTIONS:
            summary, = self.rows(current["assets"][f"aggregate-l{resolution}"])
            self.assertEqual(summary["detection_count"], 3)
            self.assertEqual(summary["frp_sum"], 57)
            self.assertEqual(summary["frp_avg"], 19)
            self.assertEqual(summary["frp_max"], 20)

    def test_cross_year_update_failure_preserves_both_items_and_reuses_cache(self):
        self.publish("2025-12-30", "2026-01-01")
        before = self.collection()
        self.client.area.reset_mock()
        self.store.reject_collection = True

        with self.assertRaisesRegex(GCSUploadError, "occurrences/collection.json"):
            self.publish("2025-12-29", "2026-01-02")
        self.assertEqual(self.collection(), before)
        current_asset = self.items()["2025"]["assets"]["data"]["gcs:uri"]
        backup, = [obj for uri, obj in self.store.objects.items()
                   if "/annual-backup/occurrences/year=2025/occurrences.parquet" in uri]
        self.assertEqual(backup[0], self.store.objects[current_asset][0])
        self.assertEqual(len(self.rows(self.items()["2025"]["assets"]["data"])), 2)
        self.assertEqual(len(self.rows(self.items()["2026"]["assets"]["data"])), 1)
        self.assertEqual(self.client.area.call_count, 2)

        self.store.reject_collection = False
        self.publish("2025-12-29", "2026-01-02")
        self.assertEqual(self.client.area.call_count, 2)
        self.assertEqual(len(self.rows(self.items()["2025"]["assets"]["data"])), 3)
        self.assertEqual(len(self.rows(self.items()["2026"]["assets"]["data"])), 2)


    def test_legacy_item_reprocesses_overlap_once_and_preserves_old_rows(self):
        self.publish("2021-11-18", "2021-11-19")
        collection_uri = f"{self.public}/occurrences/collection.json"
        collection = self.collection()
        link = next(link for link in collection["links"] if link["rel"] == "item")
        item = self.items()["2021"]
        data = item["assets"]["data"]
        legacy = f"{self.public}/occurrences/year=2021"
        self.store.objects[f"{legacy}/occurrences.parquet"] = self.store.objects[data["gcs:uri"]]
        data["gcs:uri"] = f"{legacy}/occurrences.parquet"
        del data["firms:sha256"]
        del item["properties"]["firms:coverage"]
        link["href"] = "./year=2021/2021.json"
        _, generation, digest = self.store.objects[collection_uri]
        self.store.objects[collection_uri] = (json.dumps(collection).encode(), generation, digest)
        self.store.objects[f"{legacy}/2021.json"] = (json.dumps(item).encode(), 1, "")

        self.client.area.reset_mock()
        self.publish("2021-11-19", "2021-11-20")
        self.client.area.assert_called_once_with("MODIS_SP", BBOX, date(2021, 11, 19), 2)
        current = self.items()["2021"]
        self.assertEqual(sorted(row["acq_date"].day for row in self.rows(current["assets"]["data"])),
                         [18, 19, 20])
        self.assertEqual(len(self.rows(data)), 3)
