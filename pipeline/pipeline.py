"""
Main pipeline logic — connects the extractor and loader together.

The watermark drives incremental loads: we read the last loaded date from
Snowflake, fetch everything since then, and only update the watermark once
all pages have been written successfully. If something fails halfway through,
the watermark stays where it was so the next run re-fetches cleanly.
"""

import logging
import time
from datetime import date, timedelta
from typing import Optional

import requests

from .config import PipelineConfig, load_config
from .extractor import SocrataExtractor
from .loader import SnowflakeLoader

logger = logging.getLogger(__name__)

PIPELINE_NAME = "iowa_liquor_sales_daily"

# Iowa's dataset goes back to January 2012; used as the start date on first run
DATASET_EPOCH = date(2012, 1, 1)


class IowaLiquorPipeline:
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or load_config()
        self.extractor = SocrataExtractor(
            app_token=self.config.socrata_app_token,
            batch_size=self.config.batch_size,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
        )
        self.loader = SnowflakeLoader(self.config)

    def run(
        self,
        since_date: Optional[date] = None,
        until_date: Optional[date] = None,
    ) -> dict:
        """
        Run an incremental load.

        If since_date is not provided, the watermark from the last successful
        run is used. If until_date is not provided, it defaults to yesterday
        (today's data may still be incomplete on the source).

        Returns a dict with status, date range, record count, and elapsed time.
        """
        t0 = time.monotonic()
        total_records = 0

        try:
            self.loader.setup()

            if since_date is None:
                watermark = self.loader.get_watermark(PIPELINE_NAME)
                since_date = (watermark + timedelta(days=1)) if watermark else DATASET_EPOCH
                logger.info("Watermark: %s → loading from %s", watermark, since_date)

            if until_date is None:
                until_date = date.today() - timedelta(days=1)

            if since_date > until_date:
                logger.info("Already up to date (since=%s, until=%s) — nothing to do.", since_date, until_date)
                return {
                    "status": "skipped",
                    "since_date": str(since_date),
                    "until_date": str(until_date),
                    "records_loaded": 0,
                    "elapsed_seconds": round(time.monotonic() - t0, 2),
                }

            logger.info("Pipeline run starting: %s → %s", since_date, until_date)

            for batch in self.extractor.fetch_incremental(since_date, until_date):
                self._check_batch(batch, since_date, until_date)
                count = self.loader.upsert_batch(batch)
                total_records += count

            # Only advance the watermark once everything has landed successfully
            self.loader.update_watermark(PIPELINE_NAME, until_date, total_records)

            summary = {
                "status": "success",
                "since_date": str(since_date),
                "until_date": str(until_date),
                "records_loaded": total_records,
                "elapsed_seconds": round(time.monotonic() - t0, 2),
            }
            logger.info("Pipeline complete: %s", summary)
            return summary

        except Exception as exc:
            elapsed = round(time.monotonic() - t0, 2)
            logger.error("Pipeline FAILED after %.1fs: %s", elapsed, exc, exc_info=True)
            self._alert(f"Iowa Liquor pipeline failed after {elapsed}s: {exc}")
            raise

        finally:
            self.loader.close()

    def _check_batch(self, batch: list[dict], since_date: date, until_date: date):
        # Reject the batch if any record is missing its primary key — a null
        # invoice number would cause the MERGE to fail or silently drop the row.
        missing_pk = [r for r in batch if not r.get("invoice_line_no")]
        if missing_pk:
            raise ValueError(
                f"{len(missing_pk)} of {len(batch)} records are missing invoice_line_no"
            )

        # Warn if the API returned records outside the date window we asked for.
        # This shouldn't happen with correct $where filtering but is worth knowing about.
        since_str = str(since_date)
        until_str = str(until_date)
        out_of_range = [
            r for r in batch
            if r.get("date", "")[:10] < since_str or r.get("date", "")[:10] > until_str
        ]
        if out_of_range:
            logger.warning(
                "%d records are outside the requested date window (%s → %s)",
                len(out_of_range), since_date, until_date,
            )

    def backfill(self, start: date, end: date):
        """Re-load a historical date range. Safe to re-run — MERGE prevents duplicates."""
        logger.info("Starting backfill: %s → %s", start, end)
        return self.run(since_date=start, until_date=end)

    def _alert(self, message: str):
        if not self.config.alert_webhook_url:
            return
        try:
            requests.post(
                self.config.alert_webhook_url,
                json={"text": message},
                timeout=10,
            )
        except Exception as exc:
            logger.warning("Failed to send Slack alert: %s", exc)
