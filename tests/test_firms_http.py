import io
import unittest
import tempfile
from contextlib import redirect_stderr
from pathlib import Path
from datetime import date
from urllib.error import HTTPError
from unittest.mock import patch

from src.firms_http import FirmsClient, FirmsError
from src.logs import run_logging


class FirmsHttpTests(unittest.TestCase):
    def test_full_budget_waits_and_429_rechecks_budget_before_retry(self):
        status_full = b'{"current_transactions": 4999, "transaction_limit": 5000}'
        status_clear = b'{"current_transactions": 0, "transaction_limit": 5000}'
        rate_limit = HTTPError("test URL", 429, "", {"Retry-After": "17"}, io.BytesIO())
        with (
            tempfile.TemporaryDirectory() as work,
            run_logging(Path(work) / "log.ndjson"),
            redirect_stderr(io.StringIO()) as stderr,
            patch("src.firms_http.urlopen", side_effect=[
                io.BytesIO(status_full), io.BytesIO(status_clear), rate_limit,
                io.BytesIO(status_clear), io.BytesIO(b"payload")
            ]) as request,
            patch("src.firms_http.time.sleep") as sleep,
        ):
            client = FirmsClient("test-key", 5000)
            self.assertEqual(client.area("MODIS_SP", "bbox", date(2026, 1, 1), 5), b"payload")
        self.assertIn('"event": "firms_budget_wait"', stderr.getvalue())
        self.assertIn('"reason": "HTTP 429"', stderr.getvalue())
        self.assertIn('"level": "WARNING"', stderr.getvalue())
        self.assertNotIn("test-key", stderr.getvalue())
        self.assertEqual(request.call_count, 5)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [60, 17])

    def test_http_error_excludes_key_url(self):
        error = HTTPError("https://example.org/test-key", 403, "", {}, io.BytesIO())
        with patch("src.firms_http.urlopen", side_effect=error):
            with self.assertRaisesRegex(FirmsError, r"^FIRMS returned HTTP 403$"):
                FirmsClient("test-key", 5000).availability()

    def test_availability_accepts_data_id_header(self):
        body = b"data_id,min_date,max_date\nMODIS_SP,2000-11-01,2026-02-01\n"
        with patch.object(FirmsClient, "_request", return_value=body):
            self.assertEqual(FirmsClient("test-key", 5000).availability(), {
                "MODIS_SP": (date(2000, 11, 1), date(2026, 2, 1))
            })

    def test_availability_error_does_not_echo_response(self):
        body = b"Invalid MAP_KEY: test-key"
        with patch("src.firms_http.urlopen", return_value=io.BytesIO(body)):
            with self.assertRaisesRegex(FirmsError, r"^FIRMS rejected the MAP key$"):
                FirmsClient("test-key", 5000).availability()
