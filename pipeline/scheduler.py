"""
Runs the pipeline on a daily schedule using APScheduler.

Fires at 06:00 UTC each day — Iowa's data portal typically has the previous
day's records finalised by ~3 AM CT, so 06:00 UTC gives a comfortable buffer.

Run with: python -m pipeline.scheduler
"""

import logging
import sys
from datetime import date

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .pipeline import IowaLiquorPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def _run_daily():
    logger.info("Scheduled trigger fired for %s", date.today())
    try:
        pipeline = IowaLiquorPipeline()
        summary = pipeline.run()
        logger.info("Scheduled run result: %s", summary)
    except Exception:
        # pipeline.run() already logged the error and sent an alert
        logger.error("Scheduled run failed — will retry tomorrow at 06:00 UTC")


def main():
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        _run_daily,
        trigger=CronTrigger(hour=6, minute=0),
        id="iowa_liquor_daily",
        name="Iowa Liquor Sales — Daily Incremental Load",
        misfire_grace_time=3_600,  # tolerate up to 1h late start
        coalesce=True,             # if we missed multiple runs, catch up with one
        max_instances=1,           # don't let a slow run overlap with the next trigger
    )
    logger.info("Scheduler started. Next run at 06:00 UTC daily. Ctrl+C to stop.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
