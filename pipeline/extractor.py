"""
Pulls records from the Iowa Liquor Sales Socrata API.

Dataset: https://data.iowa.gov/resource/m3tr-qhgy
API docs: https://dev.socrata.com/foundry/data.iowa.gov/m3tr-qhgy

Uses offset-based pagination with a stable sort so we don't miss or
double-count rows when paginating over a bounded date window.
An app token is recommended — without one you hit the anonymous throttle
pretty quickly on larger date ranges.
"""

import logging
import time
from datetime import date
from typing import Generator, Optional

import requests

logger = logging.getLogger(__name__)

SOCRATA_URL = "https://data.iowa.gov/resource/m3tr-qhgy.json"


class ExtractionError(Exception):
    pass


class SocrataExtractor:
    def __init__(
        self,
        app_token: str = "",
        batch_size: int = 10_000,
        max_retries: int = 3,
        retry_delay: float = 5.0,
    ):
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.session = requests.Session()
        if app_token:
            self.session.headers["X-App-Token"] = app_token

    def fetch_incremental(
        self,
        since_date: date,
        until_date: Optional[date] = None,
    ) -> Generator[list[dict], None, None]:
        """
        Yields pages of records for the given date range.

        Each page is a list of raw API dicts. Yielding one page at a time
        keeps memory usage flat regardless of how many records we pull.
        """
        until_date = until_date or date.today()
        since_str = since_date.strftime("%Y-%m-%d")
        until_str = until_date.strftime("%Y-%m-%d")

        offset = 0
        total = 0

        logger.info("Extracting records from %s to %s", since_date, until_date)

        while True:
            params = {
                "$where": f"date >= '{since_str}' AND date <= '{until_str}'",
                "$limit": self.batch_size,
                "$offset": offset,
                "$order": "date ASC, invoice_line_no ASC",
            }

            batch = self._get_with_retry(params)

            if not batch:
                break

            total += len(batch)
            logger.info("Fetched %d records (offset=%d, running total=%d)", len(batch), offset, total)
            yield batch

            if len(batch) < self.batch_size:
                break  # reached the last page

            offset += self.batch_size

        logger.info("Extraction complete. Total records fetched: %d", total)

    def _get_with_retry(self, params: dict) -> list[dict]:
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(SOCRATA_URL, params=params, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as e:
                # 4xx errors (except 429 rate limit) are client mistakes — don't retry
                if e.response is not None and e.response.status_code < 500 and e.response.status_code != 429:
                    raise ExtractionError(f"Non-retryable HTTP error: {e}") from e
                self._wait(attempt, e)
            except requests.RequestException as e:
                self._wait(attempt, e)

        raise ExtractionError(f"Failed after {self.max_retries} attempts")

    def _wait(self, attempt: int, exc: Exception):
        if attempt == self.max_retries:
            raise ExtractionError("Max retries exceeded") from exc
        wait = self.retry_delay * (2 ** (attempt - 1))
        logger.warning("Attempt %d failed (%s). Retrying in %.1fs...", attempt, exc, wait)
        time.sleep(wait)
