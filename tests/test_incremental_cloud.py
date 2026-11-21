import io
import json
import subprocess
import unittest
from contextlib import redirect_stderr
from datetime import date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow.parquet as pq

from src.cloud import publish_run
from src.dataset import GCSUploadError, RESOLUTIONS
from src.firms_http import FirmsClient, FirmsError
from src.ingest import BBOX
from src.satellites import SATELLITE_LIST


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
            return result(
                stdout=digest if "metadata.sha256" in command[-1] else str(generation)
            )
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
                Path(command[-2]).read_bytes(),
                generation + 1,
                command[4].split("=")[-1],
            )
            return result()
        uri, _, generation = command[-2].partition("#")
        if uri not in self.objects or (
            generation and int(generation) != self.objects[uri][1]
        ):
            return result(1, stderr="HTTP error 404")
        Path(command[-1]).write_bytes(self.objects[uri][0])
        return result()


class CloudPublicationFixture(unittest.TestCase):
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

    def publish(self, first, last, *, render_maps=False):
        return publish_run(
            self.client,
            self.public,
            self.private,
            date.fromisoformat(first),
            date.fromisoformat(last),
            BBOX,
            render_maps=render_maps,
        )

    def collection(self):
        return json.loads(
            self.store.objects[f"{self.public}/occurrences/collection.json"][0]
        )

    def items(self):
        return {
            item["id"]: item
            for item in (
                json.loads(
                    self.store.objects[
                        f"{self.public}/occurrences/{link['href'].removeprefix('./')}"
                    ][0]
                )
                for link in self.collection()["links"]
                if link["rel"] == "item"
            )
        }

    def rows(self, asset):
        return (
            pq.ParquetFile(io.BytesIO(self.store.objects[asset["gcs:uri"]][0]))
            .read()
            .to_pylist()
        )


class IncrementalCloudTests(CloudPublicationFixture):
    def test_add_year_then_extend_with_overlap_and_retry(self):
        self.publish("2020-12-31", "2020-12-31")
        self.publish("2021-11-18", "2021-11-19")
        previous = self.items()["2021"]
        self.assertEqual(len(self.rows(previous["assets"]["data"])), 2)
        self.client.area.reset_mock()

        self.publish("2021-11-19", "2021-11-20")
        self.client.area.assert_called_once_with(
            "MODIS_SP", BBOX, date(2021, 11, 20), 1
        )
        self.publish("2021-11-19", "2021-11-20")
        self.client.area.assert_called_once()

        items = self.items()
        self.assertEqual(set(items), {"2020", "2021"})
        current = items["2021"]
        self.assertEqual(current["properties"]["firms:requested_end"], "2021-11-20")
        self.assertEqual(
            sorted(row["acq_date"].day for row in self.rows(current["assets"]["data"])),
            [18, 19, 20],
        )
        expected = f"{self.public}/occurrences/year=2021"
        self.assertEqual(current["properties"]["gcs:uri"], f"{expected}/2021.json")
        self.assertEqual(
            current["assets"]["data"]["gcs:uri"], f"{expected}/occurrences.parquet"
        )
        self.assertFalse(
            any(
                "/versions/" in uri
                for uri in self.store.objects
                if uri.startswith(self.public)
            )
        )
        for resolution in RESOLUTIONS:
            (summary,) = self.rows(current["assets"][f"aggregate-l{resolution}"])
            self.assertEqual(summary["detection_count"], 3)
            self.assertEqual(summary["frp_sum"], 57)
            self.assertEqual(summary["frp_avg"], 19)
            self.assertEqual(summary["frp_max"], 20)

    def test_one_day_rebuilds_annual_data_maps_and_metadata(self):
        first, second = "2021-11-18", "2021-11-19"

        def load_h3(connection):
            self.load_h3(connection)
            connection.execute("""
                CREATE OR REPLACE MACRO h3_latlng_to_cell(lat, lng, resolution)
                    AS CAST(resolution AS UBIGINT) + CASE WHEN lat = -11 THEN 10 ELSE 0 END;
            """)

        def csv_data(source, bbox, start, days):
            data = self.csv_data(source, bbox, start, days)
            if start.isoformat() == first:
                data += f"-11,-60,{start},0010,310,290,80,9\n".encode()
            return data

        self.client.area.side_effect = csv_data
        self.enterContext(patch("src.cloud.load_h3", side_effect=load_h3))
        self.publish(first, first, render_maps=True)
        prior = self.public_snapshot()
        old_item = self.items()["2021"]

        self.publish(second, second, render_maps=True)
        current = self.public_snapshot()
        item = self.items()["2021"]
        properties = item["properties"]
        self.assertEqual(properties["firms:requested_start"], first)
        self.assertEqual(properties["firms:requested_end"], second)
        self.assertEqual(
            properties["firms:coverage"]["MODIS_SP"],
            [[first, second]],
        )
        self.assertEqual(
            datetime.fromisoformat(properties["start_datetime"]).date().isoformat(),
            first,
        )
        self.assertEqual(
            datetime.fromisoformat(properties["end_datetime"]).date().isoformat(),
            second,
        )
        self.assertEqual(properties["firms:products"], ["MODIS_SP"])

        assets = item["assets"]
        self.assertEqual(assets["data"]["table:row_count"], 3)
        self.assertEqual(
            sorted(row["acq_date"].isoformat() for row in self.rows(assets["data"])),
            [first, first, second],
        )
        changed = ["data"]
        for resolution in RESOLUTIONS:
            key = f"aggregate-l{resolution}"
            layer = f"h3-r{resolution}"
            summary_asset = assets[key]
            self.assertEqual(summary_asset["table:row_count"], 2)
            row, other = self.rows(summary_asset)
            self.assertEqual(other["detection_count"], 1)
            self.assertEqual(
                [
                    row[name]
                    for name in (
                        "detection_count",
                        "frp_count",
                        "frp_sum",
                        "frp_avg",
                        "frp_max",
                    )
                ],
                [2, 2, 37, 18.5, 19],
            )
            self.assertTrue(current[assets[layer]["gcs:uri"]][0].startswith(b"PMTiles"))
            changed.extend([key, layer])
            for metric in ("detection_count", "frp_sum", "frp_avg", "frp_max"):
                style_key = f"style-{layer}-{metric}"
                changed.append(style_key)
                style = json.loads(current[assets[style_key]["gcs:uri"]][0])
                self.assertEqual(style["layers"][0]["source-layer"], layer)
                self.assertEqual(style["metadata"]["firms:metric"], metric)

        for key, asset in assets.items():
            uri = asset["gcs:uri"]
            data, generation, metadata_digest = current[uri]
            self.assertEqual(sha256(data).hexdigest(), asset["firms:sha256"], key)
            self.assertEqual(metadata_digest, asset["firms:sha256"], key)
            if key in changed:
                self.assertNotEqual(
                    asset["firms:sha256"], old_item["assets"][key]["firms:sha256"]
                )
                self.assertEqual(generation, prior[uri][1] + 1)

        catalog_uri = f"{self.public}/catalog.json"
        self.assertEqual(current[catalog_uri], prior[catalog_uri])
        catalog = json.loads(current[catalog_uri][0])
        (child,) = (link for link in catalog["links"] if link["rel"] == "child")
        self.assertEqual(child["href"], "./occurrences/collection.json")
        collection = self.collection()
        self.assertEqual(
            collection["extent"]["temporal"]["interval"],
            [[properties["start_datetime"], properties["end_datetime"]]],
        )
        self.assertEqual(collection["firms:visual_year"], 2021)
        for resolution in RESOLUTIONS:
            layer = f"h3-r{resolution}"
            self.assertEqual(
                collection["assets"][layer]["href"],
                f"./year=2021/aggregate-l{resolution}.pmtiles",
            )
        self.assertEqual(set(self.items()), {"2021"})

        self.client.area.reset_mock()
        self.publish(second, second, render_maps=True)
        self.client.area.assert_not_called()
        self.assertEqual(self.public_snapshot(), current)

    def test_cross_year_update_failure_preserves_both_items_and_reuses_cache(self):
        self.publish("2025-12-30", "2026-01-01")
        before = self.collection()
        self.client.area.reset_mock()
        self.store.reject_collection = True

        with self.assertRaisesRegex(GCSUploadError, "occurrences/collection.json"):
            self.publish("2025-12-29", "2026-01-02")
        self.assertEqual(self.collection(), before)
        current_asset = self.items()["2025"]["assets"]["data"]["gcs:uri"]
        (backup,) = [
            obj
            for uri, obj in self.store.objects.items()
            if "/annual-backup/occurrences/year=2025/occurrences.parquet" in uri
        ]
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
        self.store.objects[f"{legacy}/occurrences.parquet"] = self.store.objects[
            data["gcs:uri"]
        ]
        data["gcs:uri"] = f"{legacy}/occurrences.parquet"
        del data["firms:sha256"]
        del item["properties"]["firms:coverage"]
        link["href"] = "./year=2021/2021.json"
        _, generation, digest = self.store.objects[collection_uri]
        self.store.objects[collection_uri] = (
            json.dumps(collection).encode(),
            generation,
            digest,
        )
        self.store.objects[f"{legacy}/2021.json"] = (json.dumps(item).encode(), 1, "")

        self.client.area.reset_mock()
        self.publish("2021-11-19", "2021-11-20")
        self.client.area.assert_called_once_with(
            "MODIS_SP", BBOX, date(2021, 11, 19), 2
        )
        current = self.items()["2021"]
        self.assertEqual(
            sorted(row["acq_date"].day for row in self.rows(current["assets"]["data"])),
            [18, 19, 20],
        )
        self.assertEqual(len(self.rows(data)), 3)

    def public_snapshot(self):
        return {
            uri: obj
            for uri, obj in self.store.objects.items()
            if uri.startswith(f"{self.public}/")
        }

    def test_backfill_interruption_then_30_day_rerun(self):
        first, last = "2020-12-17", "2021-01-15"
        stop = date(2020, 12, 22)

        def interrupted(source, bbox, start, days):
            if start == stop:
                raise KeyboardInterrupt
            return self.csv_data(source, bbox, start, days)

        self.client.area.side_effect = interrupted
        with self.assertRaises(KeyboardInterrupt):
            self.publish(first, last)
        self.assertEqual(self.public_snapshot(), {})
        self.assertEqual(
            sum(uri.endswith("/complete.json") for uri in self.store.objects), 1
        )

        self.client.area.reset_mock()
        self.client.area.side_effect = self.csv_data
        self.publish(first, last)
        self.assertEqual(
            [call.args[2] for call in self.client.area.call_args_list],
            [
                date(2020, 12, 22),
                date(2020, 12, 27),
                date(2021, 1, 1),
                date(2021, 1, 6),
                date(2021, 1, 11),
            ],
        )
        self.assertTrue(
            all(call.args[3] == 5 for call in self.client.area.call_args_list)
        )
        rows = [
            row["acq_date"]
            for item in self.items().values()
            for row in self.rows(item["assets"]["data"])
        ]
        self.assertEqual(
            sorted(rows),
            [date.fromisoformat(first) + timedelta(days=i) for i in range(30)],
        )

        before = self.public_snapshot()
        self.client.area.reset_mock()
        self.publish(first, last)
        self.client.area.assert_not_called()
        self.assertEqual(self.public_snapshot(), before)

        published_assets = {
            uri: obj[0] for uri, obj in before.items() if uri.endswith(".parquet")
        }
        self.store = MemoryGCS()
        with patch("subprocess.run", side_effect=self.store):
            self.publish(first, last)
        self.assertEqual(
            {
                uri: obj[0]
                for uri, obj in self.public_snapshot().items()
                if uri.endswith(".parquet")
            },
            published_assets,
        )

    def test_backfill_with_altered_cached_bytes_fails_closed(self):
        self.client.area.side_effect = [
            self.csv_data("MODIS_SP", BBOX, date(2021, 1, 1), 5),
            KeyboardInterrupt,
        ]
        with self.assertRaises(KeyboardInterrupt):
            self.publish("2021-01-01", "2021-01-06")
        (uri,) = (
            uri
            for uri in self.store.objects
            if "/chunks/" in uri and uri.endswith(".parquet")
        )
        payload, generation, digest = self.store.objects[uri]
        self.store.objects[uri] = (
            bytes([payload[0] ^ 1]) + payload[1:],
            generation,
            digest,
        )

        self.client.area.reset_mock()
        with self.assertRaisesRegex(FirmsError, "Private chunk failed verification"):
            self.publish("2021-01-01", "2021-01-06")
        self.client.area.assert_not_called()
        self.assertEqual(self.public_snapshot(), {})

    def test_daily_hard_stop_after_occurrences_write_requires_recovery(self):
        class RunnerStopped(BaseException):
            pass

        self.publish("2021-11-18", "2021-11-18")
        before = self.public_snapshot()
        item = self.items()["2021"]
        uri = item["assets"]["data"]["gcs:uri"]

        def stop_after_write(command, **kwargs):
            result = self.store(command, **kwargs)
            if command[:3] == ["gcloud", "storage", "cp"] and command[-1] == uri:
                self.assertEqual(result.returncode, 0)
                raise RunnerStopped
            return result

        with patch("subprocess.run", side_effect=stop_after_write):
            with self.assertRaises(RunnerStopped):
                self.publish("2021-11-19", "2021-11-19")
        self.assertEqual(self.items()["2021"], item)
        self.assertNotEqual(self.store.objects[uri], before[uri])
        (backup,) = (
            obj
            for path, obj in self.store.objects.items()
            if "/annual-backup/" in path and path.endswith("/occurrences.parquet")
        )
        self.assertEqual(backup[0], before[uri][0])

        self.client.area.reset_mock()
        with self.assertRaises(GCSUploadError) as failure:
            self.publish("2021-11-19", "2021-11-19")
        self.assertEqual(failure.exception.reason, "existing_object_checksum_mismatch")
        self.client.area.assert_not_called()

    def test_daily_schema_change_in_each_product_fails_before_publication(self):
        self.publish("2021-11-18", "2021-11-18")
        before = self.public_snapshot()
        day = date(2021, 11, 19)
        for product in SATELLITE_LIST:
            with self.subTest(product=product):
                self.client.availability.return_value = {product: (day, day)}
                self.client.area.return_value = (
                    b"lat,longitude,acq_date,acq_time,confidence\n"
                )
                self.client.area.side_effect = None
                with self.assertRaisesRegex(
                    ValueError, rf"Invalid FIRMS {product} schema or values at row 1"
                ):
                    self.publish(day.isoformat(), day.isoformat())
                self.assertEqual(self.public_snapshot(), before)
        self.assertFalse(
            any("/2021-11-19_1d/complete.json" in uri for uri in self.store.objects)
        )

    def test_missed_daily_date_stays_missing_until_requested_explicitly(self):
        self.publish("2021-11-18", "2021-11-18")
        self.publish("2021-11-20", "2021-11-20")

        def published_days():
            return sorted(
                row["acq_date"].day
                for row in self.rows(self.items()["2021"]["assets"]["data"])
            )

        self.assertEqual(published_days(), [18, 20])
        self.client.area.reset_mock()
        self.publish("2021-11-18", "2021-11-20")
        self.client.area.assert_called_once_with(
            "MODIS_SP", BBOX, date(2021, 11, 19), 1
        )
        self.assertEqual(published_days(), [18, 19, 20])

    def test_daily_and_backfill_fail_when_budget_or_http_retries_are_exhausted(self):
        clear = b'{"current_transactions": 0, "transaction_limit": 5000}'
        full = b'{"current_transactions": 5000, "transaction_limit": 5000}'
        availability = b"source,min_date,max_date\nMODIS_SP,2000-11-01,2026-11-19\n"
        before = self.public_snapshot()

        for first in ("2000-01-01", "2026-11-19"):
            for timed_out in (False, True):
                with self.subTest(first=first, timed_out=timed_out):
                    area_requests = []

                    def request(url, *, timeout):
                        self.assertEqual(timeout, 60)
                        if "/mapkey_status/" in url:
                            budget = (
                                clear
                                if timed_out or request_mock.call_count == 1
                                else full
                            )
                            return io.BytesIO(budget)
                        if "/data_availability/" in url:
                            return io.BytesIO(availability)
                        self.assertIn("/api/area/", url)
                        area_requests.append(url)
                        raise TimeoutError

                    with (
                        patch.object(
                            self, "client", FirmsClient("synthetic-map-key", 5000)
                        ),
                        patch(
                            "src.firms_http.urlopen", side_effect=request
                        ) as request_mock,
                        patch("src.firms_http.time.sleep") as sleep,
                    ):
                        reason = (
                            "failed after 5 attempts"
                            if timed_out
                            else "budget did not recover"
                        )
                        with self.assertRaisesRegex(FirmsError, reason):
                            self.publish(first, "2026-11-19")
                    self.assertEqual(len(area_requests), 5 if timed_out else 0)
                    self.assertEqual(sleep.call_count, 4 if timed_out else 10)
                    if not timed_out:
                        self.assertTrue(
                            all(call.args == (60,) for call in sleep.call_args_list)
                        )
                    self.assertEqual(self.public_snapshot(), before)
