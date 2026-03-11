-- Iowa Liquor Sales Pipeline — Snowflake DDL
-- Applied automatically by loader.py on first run.
-- Kept here as a reference for manual inspection or CI checks.

CREATE DATABASE IF NOT EXISTS IOWA_LIQUOR;
CREATE SCHEMA   IF NOT EXISTS IOWA_LIQUOR.RAW;

CREATE TABLE IF NOT EXISTS IOWA_LIQUOR.RAW.LIQUOR_SALES (
    invoice_item_number  VARCHAR(50)    NOT NULL,
    sale_date            DATE,
    store_number         VARCHAR(20),
    store_name           VARCHAR(255),
    address              VARCHAR(500),
    city                 VARCHAR(100),
    zip_code             VARCHAR(20),
    store_location       VARCHAR(255),     -- WKT point e.g. "POINT (-93.57 41.60)"
    county_number        VARCHAR(20),
    county               VARCHAR(100),
    category             VARCHAR(20),
    category_name        VARCHAR(255),
    vendor_number        VARCHAR(20),
    vendor_name          VARCHAR(255),
    item_number          VARCHAR(20),
    item_description     VARCHAR(500),
    pack                 NUMBER(10,0),     -- bottles per case
    bottle_volume_ml     NUMBER(10,0),
    state_bottle_cost    NUMBER(10,2),
    state_bottle_retail  NUMBER(10,2),
    bottles_sold         NUMBER(10,0),
    sale_dollars         NUMBER(12,2),
    volume_sold_liters   NUMBER(10,2),
    volume_sold_gallons  NUMBER(10,2),
    _loaded_at           TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_liquor_sales PRIMARY KEY (invoice_item_number)
)
CLUSTER BY (sale_date);


CREATE TABLE IF NOT EXISTS IOWA_LIQUOR.RAW.PIPELINE_WATERMARKS (
    pipeline_name    VARCHAR(100) NOT NULL,
    last_loaded_date DATE,
    last_run_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    records_loaded   NUMBER(10,0),
    PRIMARY KEY (pipeline_name)
);



CREATE OR REPLACE VIEW IOWA_LIQUOR.RAW.V_DAILY_SALES AS
SELECT
    sale_date,
    SUM(sale_dollars)      AS total_sale_dollars,
    SUM(bottles_sold)      AS total_bottles_sold,
    SUM(volume_sold_liters) AS total_volume_liters,
    COUNT(*)               AS transaction_count
FROM IOWA_LIQUOR.RAW.LIQUOR_SALES
GROUP BY 1
ORDER BY 1 DESC;

CREATE OR REPLACE VIEW IOWA_LIQUOR.RAW.V_TOP_CATEGORIES AS
SELECT
    category_name,
    SUM(sale_dollars) AS total_sale_dollars,
    SUM(bottles_sold) AS total_bottles_sold
FROM IOWA_LIQUOR.RAW.LIQUOR_SALES
GROUP BY 1
ORDER BY 2 DESC;
