"""Credential-safe HTTP transport for the local FIRMS downloader."""

import csv
import io
import json
import random
import time
from collections.abc import Iterator
from datetime import date
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import urlopen

BASE_URL = "https://firms.modaps.eosdis.nasa.gov"
MAX_ATTEMPTS = 5


def csv_rows(body: bytes) -> Iterator[list[str]]:
    return csv.reader(io.StringIO(body.decode("utf-8-sig"), newline=""), strict=True)


class FirmsError(RuntimeError):
    pass


class FirmsClient:
    def __init__(self, map_key: str, transaction_limit: int) -> None:
        self._key = map_key
        self._path_key = quote(map_key, safe="")
        self._limit = transaction_limit

    def _request(self, path: str, *, reserve: int = 0) -> bytes:
        for attempt in range(MAX_ATTEMPTS):
            if reserve:
                self._wait_for_budget(reserve)

            retry_after = 0
            try:
                with urlopen(BASE_URL + path, timeout=60) as response:
                    body = response.read()
                message = body[:512].lower()
                key_message = message.replace(b"map_key", b"map key")
                if b"map key" in key_message and any(
                    word in key_message for word in (b"invalid", b"expired")
                ):
                    raise FirmsError("FIRMS rejected the MAP key")
                if b"transaction limit" not in message:
                    return body
                retry_after = 60
            except HTTPError as error:
                with error:
                    if error.code not in {429, 500, 502, 503, 504}:
                        raise FirmsError(f"FIRMS returned HTTP {error.code}") from None
                    if error.code == 429:
                        value = error.headers.get("Retry-After", "")
                        retry_after = int(value) if value.isascii() and value.isdecimal() else 60
            except (URLError, TimeoutError, OSError):
                pass

            if attempt + 1 < MAX_ATTEMPTS:
                if retry_after > 600:
                    raise FirmsError("FIRMS requested a retry beyond the local wait limit")
                time.sleep(max(retry_after, 2**attempt + random.random()))

        raise FirmsError("FIRMS request failed after bounded retries")

    def _wait_for_budget(self, reserve: int) -> None:
        query = urlencode({"MAP_KEY": self._key})
        for check in range(11):
            body = self._request(f"/mapserver/mapkey_status/?{query}")
            try:
                status = json.loads(body)
                used = status["current_transactions"]
                limit = min(self._limit, status["transaction_limit"])
                if not isinstance(used, int) or used < 0 or limit <= 0:
                    raise ValueError
            except (KeyError, TypeError, ValueError):
                raise FirmsError("FIRMS returned invalid transaction status") from None
            if used + reserve <= limit:
                return
            if check < 10:
                time.sleep(60)
        raise FirmsError("FIRMS transaction budget did not recover")

    def availability(self) -> dict[str, tuple[date, date]]:
        body = self._request(f"/api/data_availability/csv/{self._path_key}/ALL", reserve=36)
        try:
            rows = csv_rows(body)
            header = next(rows)
            if len(header) < 3 or header[0].strip().lower() not in {"source", "data_id"}:
                raise FirmsError("FIRMS availability response has an unexpected CSV header")
            availability = {}
            for row in rows:
                if not row:
                    continue
                source, first, last, *_ = (value.strip() for value in row)
                start, end = date.fromisoformat(first), date.fromisoformat(last)
                if source in availability or start > end:
                    raise ValueError
                availability[source] = (start, end)
            if availability:
                return availability
        except (StopIteration, ValueError, csv.Error):
            pass
        raise FirmsError("FIRMS availability response has missing or invalid product dates")

    def area(self, source: str, bbox: str, start: date, days: int) -> bytes:
        path = f"/api/area/csv/{self._path_key}/{source}/{bbox}/{days}/{start}"
        # The reference project measured 36 transactions per day for the world.
        return self._request(path, reserve=36 * days)
