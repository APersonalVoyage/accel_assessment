"""
CLI for running the pipeline manually with a configurable date range.

Run with: python -m pipeline.run [--since YYYY-MM-DD] [--until YYYY-MM-DD] [--status]
"""

import argparse
import logging
import sys
from datetime import date, datetime

from dotenv import load_dotenv

load_dotenv("pipeline/.env")

from .config import load_config
from .loader import SnowflakeLoader
from .pipeline import IowaLiquorPipeline, PIPELINE_NAME

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date '{s}' — use YYYY-MM-DD format")


def main():
    parser = argparse.ArgumentParser(description="Iowa Liquor Sales Pipeline")
    parser.add_argument("--since", type=parse_date, metavar="YYYY-MM-DD",
                        help="Start date (inclusive). Defaults to watermark + 1 day.")
    parser.add_argument("--until", type=parse_date, metavar="YYYY-MM-DD",
                        help="End date (inclusive). Defaults to yesterday.")
    parser.add_argument("--status", action="store_true",
                        help="Show current watermark and exit without running.")
    args = parser.parse_args()

    config = load_config()

    if args.status:
        loader = SnowflakeLoader(config)
        watermark = loader.get_watermark(PIPELINE_NAME)
        loader.close()
        if watermark:
            print(f"Last loaded date : {watermark}")
            print(f"Next run will load from: {watermark} + 1 day onwards")
        else:
            print("No watermark found — pipeline has not run yet.")
        return

    pipeline = IowaLiquorPipeline(config)
    result = pipeline.run(since_date=args.since, until_date=args.until)
    print("\nResult:", result)


if __name__ == "__main__":
    main()
