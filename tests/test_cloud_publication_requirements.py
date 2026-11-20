import hashlib
import json
from datetime import date

from src.dataset import RESOLUTIONS

import test_incremental_cloud


class CloudPublicationRequirements(test_incremental_cloud.CloudPublicationFixture):
    def publish_with_empty_middle_day(self):
        empty_day = date(2022, 12, 30)

        def without_middle_day(source, bbox, start, days):
            lines = self.csv_data(source, bbox, start, days).splitlines(keepends=True)
            return b"".join(
                line for line in lines if str(empty_day).encode() not in line
            )

        self.client.area.side_effect = without_middle_day
        return self.publish("2022-12-29", "2022-12-31")

    def assert_warned_about_date(self, run_id, missing_day):
        uri = f"{self.private}/runs/{run_id}/events.ndjson"
        events = [json.loads(line) for line in self.store.objects[uri][0].splitlines()]
        warnings = [event for event in events if event["level"] == "WARNING"]
        self.assertTrue(
            any(missing_day in json.dumps(event) for event in warnings),
            f"No warning identified {missing_day}. Warnings: {warnings}",
        )

    def test_date_without_detections_warns_and_finishes(self):
        run_id = self.publish_with_empty_middle_day()
        manifest_uri = f"{self.private}/runs/{run_id}/manifest.json"
        manifest = json.loads(self.store.objects[manifest_uri][0])
        self.assertEqual(manifest["cloud_publication"]["outcome"], "complete")
        self.assert_warned_about_date(run_id, "2022-12-30")

    def test_date_without_detections_is_in_parquet_and_excluded_from_h3(self):
        self.publish_with_empty_middle_day()
        assets = self.items()["2022"]["assets"]
        for resolution in RESOLUTIONS:
            (summary,) = self.rows(assets[f"aggregate-l{resolution}"])
            self.assertEqual(summary["detection_count"], 2)
        rows = {row["acq_date"]: row for row in self.rows(assets["data"])}
        self.assertEqual(set(rows), {date(2022, 12, day) for day in (29, 30, 31)})
        empty = rows[date(2022, 12, 30)]
        for column in ("latitude", "longitude", "geom", "frp"):
            with self.subTest(column=column):
                self.assertIsNone(empty[column])

    def test_header_only_response_is_publishable(self):
        self.client.area.side_effect = None
        self.client.area.return_value = self.csv_data(
            "MODIS_SP", "", date(2022, 12, 30), 0
        )
        run_id = self.publish("2022-12-30", "2022-12-30")
        assets = self.items()["2022"]["assets"]
        for resolution in RESOLUTIONS:
            self.assertEqual(self.rows(assets[f"aggregate-l{resolution}"]), [])
        with self.subTest(requirement="dated empty row"):
            self.assertEqual(
                [row["acq_date"] for row in self.rows(assets["data"])],
                [date(2022, 12, 30)],
            )
        with self.subTest(requirement="warning"):
            self.assert_warned_about_date(run_id, "2022-12-30")

    def test_request_before_product_availability_warns_and_finishes(self):
        self.client.availability.return_value = {
            "MODIS_SP": (date(2023, 1, 1), date(2026, 3, 1))
        }
        run_id = self.publish("2022-12-31", "2022-12-31")
        self.client.area.assert_not_called()
        self.assert_warned_about_date(run_id, "2022-12-31")
        data = self.items()["2022"]["assets"]["data"]
        self.assertEqual(
            [row["acq_date"] for row in self.rows(data)], [date(2022, 12, 31)]
        )

    def test_newly_available_date_replaces_its_date_only_row(self):
        day = date(2022, 12, 31)
        self.client.availability.return_value = {
            "MODIS_SP": (date(2023, 1, 1), date(2026, 3, 1))
        }
        self.publish(day.isoformat(), day.isoformat())
        self.client.area.assert_not_called()

        self.client.availability.return_value["MODIS_SP"] = (day, day)
        self.publish(day.isoformat(), day.isoformat())
        self.client.area.assert_called_once()
        assets = self.items()["2022"]["assets"]
        (row,) = self.rows(assets["data"])
        self.assertEqual(row["acq_date"], day)
        self.assertIsNotNone(row["geom"])
        for resolution in RESOLUTIONS:
            (summary,) = self.rows(assets[f"aggregate-l{resolution}"])
            self.assertEqual(summary["detection_count"], 1)

    def assert_existing_document_allows_backfill(self, name, digest):
        uri = f"{self.public}/{name}"
        prior = b"# Existing public documentation\n"
        self.store.objects[uri] = (prior, 1, digest)
        self.publish("2022-12-31", "2023-01-01")
        self.publish("2022-12-31", "2023-01-02")
        self.client.area.reset_mock()
        self.publish("2022-12-31", "2023-01-02")
        self.client.area.assert_not_called()
        self.assertEqual(set(self.items()), {"2022", "2023"})
        self.assertEqual(
            len(self.rows(self.items()["2023"]["assets"]["data"])), 2
        )
        content, _, checksum = self.store.objects[uri]
        if content != prior:
            self.assertEqual(checksum, hashlib.sha256(content).hexdigest())

    def test_existing_readme_without_checksum_allows_backfill_and_rerun(self):
        self.assert_existing_document_allows_backfill("README.md", "")

    def test_existing_agents_with_different_checksum_allows_backfill_and_rerun(self):
        prior = b"# Existing public documentation\n"
        self.assert_existing_document_allows_backfill(
            "AGENTS.md", hashlib.sha256(prior).hexdigest()
        )

    def test_existing_catalog_is_updated_and_rerun_is_idempotent(self):
        uri = f"{self.public}/catalog.json"
        legacy = json.dumps({
            "type": "Catalog",
            "id": "legacy",
            "stac_version": "1.1.0",
            "description": "Earlier catalog",
            "links": [{"rel": "child", "href": "./legacy/collection.json"}],
        }).encode()
        self.store.objects[uri] = (legacy, 1, hashlib.sha256(legacy).hexdigest())
        self.publish("2022-12-31", "2023-01-01")
        self.publish("2022-12-31", "2023-01-02")
        current = self.store.objects[uri]
        payload, generation, checksum = current
        self.assertGreater(generation, 1)
        self.assertEqual(checksum, hashlib.sha256(payload).hexdigest())
        links = json.loads(payload)["links"]
        (child,) = [link for link in links if link["rel"] == "child"]
        self.assertEqual(child["href"], "./occurrences/collection.json")
        self.assertEqual(set(self.items()), {"2022", "2023"})
        self.assertEqual(
            self.items()["2023"]["properties"]["firms:requested_end"], "2023-01-02"
        )

        self.client.area.reset_mock()
        self.publish("2022-12-31", "2023-01-02")
        self.client.area.assert_not_called()
        self.assertEqual(self.store.objects[uri], current)
