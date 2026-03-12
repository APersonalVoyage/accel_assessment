"""
Handles all Snowflake operations for the pipeline: creating tables, loading data, and tracking watermarks.

The main pattern here is stage-then-merge: we bulk-insert each page into a
temporary table, then run a single MERGE statement against the target. This
keeps the upsert logic in one place, makes re-runs safe (same invoice number
won't be inserted twice), and is faster than doing row-by-row upserts.

The watermark table is just a small tracking table that stores the last date
we successfully loaded. That way the pipeline knows where to pick up from if
something fails or it hasn't run in a while.
"""

import logging
from datetime import date
from typing import Optional

import snowflake.connector
from snowflake.connector import DictCursor

from .config import PipelineConfig

logger = logging.getLogger(__name__)


_DDL_DATABASE = "CREATE DATABASE IF NOT EXISTS {db}"
_DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS {db}.{schema}"

_DDL_LIQUOR_SALES = """
CREATE TABLE IF NOT EXISTS {db}.{schema}.LIQUOR_SALES (
    invoice_item_number  VARCHAR(50)    NOT NULL,
    sale_date            DATE,
    store_number         VARCHAR(20),
    store_name           VARCHAR(255),
    address              VARCHAR(500),
    city                 VARCHAR(100),
    zip_code             VARCHAR(20),
    store_location       VARCHAR(255),
    county_number        VARCHAR(20),
    county               VARCHAR(100),
    category             VARCHAR(20),
    category_name        VARCHAR(255),
    vendor_number        VARCHAR(20),
    vendor_name          VARCHAR(255),
    item_number          VARCHAR(20),
    item_description     VARCHAR(500),
    pack                 NUMBER(10,0),
    bottle_volume_ml     NUMBER(10,0),
    state_bottle_cost    NUMBER(10,2),
    state_bottle_retail  NUMBER(10,2),
    bottles_sold         NUMBER(10,0),
    sale_dollars         NUMBER(12,2),
    volume_sold_liters   NUMBER(10,2),
    volume_sold_gallons  NUMBER(10,2),
    _loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_liquor_sales PRIMARY KEY (invoice_item_number)
)
"""

_DDL_WATERMARKS = """
CREATE TABLE IF NOT EXISTS {db}.{schema}.PIPELINE_WATERMARKS (
    pipeline_name    VARCHAR(100) NOT NULL,
    last_loaded_date DATE,
    last_run_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    records_loaded   NUMBER(10,0),
    PRIMARY KEY (pipeline_name)
)
"""

_DDL_VIEW_DAILY_SALES = """
CREATE OR REPLACE VIEW {db}.{schema}.V_DAILY_SALES AS
SELECT
    sale_date,
    SUM(sale_dollars)       AS total_sale_dollars,
    SUM(bottles_sold)       AS total_bottles_sold,
    SUM(volume_sold_liters) AS total_volume_liters,
    COUNT(*)                AS transaction_count
FROM {db}.{schema}.LIQUOR_SALES
GROUP BY 1
ORDER BY 1 DESC
"""

_DDL_VIEW_TOP_CATEGORIES = """
CREATE OR REPLACE VIEW {db}.{schema}.V_TOP_CATEGORIES AS
SELECT
    category_name,
    SUM(sale_dollars) AS total_sale_dollars,
    SUM(bottles_sold) AS total_bottles_sold
FROM {db}.{schema}.LIQUOR_SALES
GROUP BY 1
ORDER BY 2 DESC
"""

_MERGE_SQL = """
MERGE INTO {db}.{schema}.LIQUOR_SALES AS t
USING {db}.{schema}.LIQUOR_SALES_STAGE AS s
ON t.invoice_item_number = s.invoice_item_number
WHEN MATCHED THEN UPDATE SET
    t.sale_date           = s.sale_date,
    t.store_name          = s.store_name,
    t.sale_dollars        = s.sale_dollars,
    t.bottles_sold        = s.bottles_sold,
    t.volume_sold_liters  = s.volume_sold_liters,
    t.volume_sold_gallons = s.volume_sold_gallons,
    t._loaded_at          = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    invoice_item_number, sale_date, store_number, store_name, address, city,
    zip_code, store_location, county_number, county, category, category_name,
    vendor_number, vendor_name, item_number, item_description, pack,
    bottle_volume_ml, state_bottle_cost, state_bottle_retail, bottles_sold,
    sale_dollars, volume_sold_liters, volume_sold_gallons
) VALUES (
    s.invoice_item_number, s.sale_date, s.store_number, s.store_name, s.address, s.city,
    s.zip_code, s.store_location, s.county_number, s.county, s.category, s.category_name,
    s.vendor_number, s.vendor_name, s.item_number, s.item_description, s.pack,
    s.bottle_volume_ml, s.state_bottle_cost, s.state_bottle_retail, s.bottles_sold,
    s.sale_dollars, s.volume_sold_liters, s.volume_sold_gallons
)
"""

_INSERT_STAGE = """
INSERT INTO {db}.{schema}.LIQUOR_SALES_STAGE (
    invoice_item_number, sale_date, store_number, store_name, address, city,
    zip_code, store_location, county_number, county, category, category_name,
    vendor_number, vendor_name, item_number, item_description, pack,
    bottle_volume_ml, state_bottle_cost, state_bottle_retail, bottles_sold,
    sale_dollars, volume_sold_liters, volume_sold_gallons
) VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)
"""

_WATERMARK_MERGE = """
MERGE INTO {db}.{schema}.PIPELINE_WATERMARKS AS t
USING (SELECT %s AS pipeline_name) AS s ON t.pipeline_name = s.pipeline_name
WHEN MATCHED THEN UPDATE SET
    t.last_loaded_date = %s,
    t.last_run_at      = CURRENT_TIMESTAMP(),
    t.records_loaded   = %s
WHEN NOT MATCHED THEN INSERT (pipeline_name, last_loaded_date, records_loaded)
VALUES (%s, %s, %s)
"""


class SnowflakeLoader:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    @property
    def conn(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None or self._conn.is_closed():
            sf = self.config.snowflake
            self._conn = snowflake.connector.connect(
                account=sf.account,
                user=sf.user,
                password=sf.password,
                database=sf.database,
                schema=sf.schema,
                warehouse=sf.warehouse,
                role=sf.role,
            )
            # Service accounts don't always have a default warehouse set,
            # so we set it explicitly to avoid "no active warehouse" errors.
            with self._conn.cursor() as cur:
                cur.execute(f"USE WAREHOUSE {sf.warehouse}")
                cur.execute(f"USE ROLE {sf.role}")
            logger.info("Connected to Snowflake account=%s database=%s", sf.account, sf.database)
        return self._conn

    def close(self):
        if self._conn and not self._conn.is_closed():
            self._conn.close()

    def setup(self):
        """Create the database, schema, and tables on first run if they don't exist."""
        sf = self.config.snowflake
        with self.conn.cursor() as cur:
            cur.execute(_DDL_DATABASE.format(db=sf.database))
            cur.execute(_DDL_SCHEMA.format(db=sf.database, schema=sf.schema))
            cur.execute(_DDL_LIQUOR_SALES.format(db=sf.database, schema=sf.schema))
            cur.execute(_DDL_WATERMARKS.format(db=sf.database, schema=sf.schema))
            cur.execute(_DDL_VIEW_DAILY_SALES.format(db=sf.database, schema=sf.schema))
            cur.execute(_DDL_VIEW_TOP_CATEGORIES.format(db=sf.database, schema=sf.schema))
        logger.info("Snowflake tables and views are ready")

    def get_watermark(self, pipeline_name: str) -> Optional[date]:
        """Return the last successfully loaded date, or None if this is a first run."""
        sf = self.config.snowflake
        with self.conn.cursor(DictCursor) as cur:
            cur.execute(
                f"SELECT last_loaded_date FROM {sf.database}.{sf.schema}.PIPELINE_WATERMARKS "
                "WHERE pipeline_name = %s",
                (pipeline_name,),
            )
            row = cur.fetchone()
            return row["LAST_LOADED_DATE"] if row else None

    def update_watermark(self, pipeline_name: str, loaded_date: date, records_loaded: int):
        sf = self.config.snowflake
        with self.conn.cursor() as cur:
            cur.execute(
                _WATERMARK_MERGE.format(db=sf.database, schema=sf.schema),
                (pipeline_name, loaded_date, records_loaded, pipeline_name, loaded_date, records_loaded),
            )
        logger.info("Watermark updated: %s → %s (%d records)", pipeline_name, loaded_date, records_loaded)

    def upsert_batch(self, records: list[dict]) -> int:
        """
        Load a batch of records into Snowflake.

        Writes to a temp staging table first, then merges into the main table.
        Returns the number of records processed.
        """
        if not records:
            return 0

        sf = self.config.snowflake
        rows = [_transform(r) for r in records]

        with self.conn.cursor() as cur:
            # Temp table is session-scoped, so it gets cleaned up automatically
            cur.execute(
                f"CREATE OR REPLACE TEMPORARY TABLE {sf.database}.{sf.schema}.LIQUOR_SALES_STAGE "
                f"LIKE {sf.database}.{sf.schema}.LIQUOR_SALES"
            )

            cur.executemany(
                _INSERT_STAGE.format(db=sf.database, schema=sf.schema),
                rows,
            )

            cur.execute(_MERGE_SQL.format(db=sf.database, schema=sf.schema))
            result = cur.fetchone()
            rows_inserted = result[0] if result else 0
            rows_updated = result[1] if result and len(result) > 1 else 0

        logger.info(
            "Batch merged: %d inserted, %d updated (source rows: %d)",
            rows_inserted,
            rows_updated,
            len(rows),
        )
        return len(rows)


def _transform(r: dict) -> tuple:
    """Map a raw Socrata API record to the tuple expected by the INSERT statement."""
    def _float(v): return float(v) if v not in (None, "") else None
    def _int(v):   return int(v)   if v not in (None, "") else None

    raw_date = r.get("date", "")
    if raw_date:
        try:
            from datetime import datetime
            sale_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").date()
        except ValueError:
            sale_date = None
    else:
        sale_date = None

    # The API returns store_location as GeoJSON; convert to WKT for storage
    loc = r.get("store_location")
    if isinstance(loc, dict):
        coords = loc.get("coordinates", [])
        store_location = f"POINT ({coords[0]} {coords[1]})" if len(coords) == 2 else None
    else:
        store_location = loc

    # Note: the Socrata API field names differ from the CSV column names
    return (
        r.get("invoice_line_no"),
        sale_date,
        r.get("store"),
        r.get("name"),
        r.get("address"),
        r.get("city"),
        r.get("zipcode"),
        store_location,
        r.get("county_number"),
        r.get("county"),
        r.get("category"),
        r.get("category_name"),
        r.get("vendor_no"),
        r.get("vendor_name"),
        r.get("itemno"),
        r.get("im_desc"),
        _int(r.get("pack")),
        _int(r.get("bottle_volume_ml")),
        _float(r.get("state_bottle_cost")),
        _float(r.get("state_bottle_retail")),
        _int(r.get("sale_bottles")),
        _float(r.get("sale_dollars")),
        _float(r.get("sale_liters")),
        _float(r.get("sale_gallons")),
    )
