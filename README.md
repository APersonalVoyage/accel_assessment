# Iowa Liquor Sales Pipeline

Automated daily ingestion of Iowa Liquor Sales data from the Socrata public API into Snowflake.

**Stack**: Python · Snowflake · APScheduler

---

## Project Structure

```
.
├── pipeline/
│   ├── config.py       # Config dataclasses, loaded from env vars
│   ├── extractor.py    # Socrata SODA API client with pagination + retry
│   ├── loader.py       # Snowflake loader: stage-and-merge upsert pattern
│   ├── pipeline.py     # Orchestration: watermark, extract→load loop, alerting
│   ├── run.py          # CLI entry point (--since, --until, --status)
│   ├── scheduler.py    # APScheduler daily cron (06:00 UTC)
│   └── schema.sql      # Snowflake DDL reference (auto-applied on first run)
├── ARCHITECTURE.md     # Design document: decisions and trade-offs
├── iowa_liquor_sales.csv  # Sample data (~1000 rows, schema reference)
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## How to Run

### 1. Prerequisites

- Python 3.11+
- A Snowflake account with a warehouse and a user that has `CREATE DATABASE` privileges
- (Optional) A [Socrata app token](https://data.iowa.gov/profile/app_tokens) — strongly recommended to avoid throttling

### 2. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.example .env
# Edit .env — fill in SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
```

### 4. Run the pipeline (CLI)

```bash
# Explicit date range
python -m pipeline.run --since 2019-01-01 --until 2019-01-07

# Watermark-driven incremental (picks up from last loaded date automatically)
python -m pipeline.run

# Check current watermark without running
python -m pipeline.run --status

# Backfill a full month
python -m pipeline.run --since 2019-02-01 --until 2019-02-28
```

Expected output:
```
2026-03-11T10:00:01 INFO     pipeline.loader   Connected to Snowflake account=...
2026-03-11T10:00:02 INFO     pipeline.loader   Snowflake tables are ready
2026-03-11T10:00:02 INFO     pipeline.pipeline Pipeline run starting: 2019-01-01 → 2019-01-07
2026-03-11T10:00:20 INFO     pipeline.extractor Fetched 10000 records (offset=0, running total=10000)
2026-03-11T10:00:35 INFO     pipeline.loader   Batch merged: 10000 inserted, 0 updated ...
...
Result: {'status': 'success', 'since_date': '2019-01-01', 'until_date': '2019-01-07', 'records_loaded': 41980, 'elapsed_seconds': 91.78}
```

### 5. Prove incremental loading

```bash
# Step 1 — load week 1
python -m pipeline.run --since 2019-01-01 --until 2019-01-07

# Step 2 — check watermark
python -m pipeline.run --status
# → Last loaded date: 2019-01-07

# Step 3 — load week 2 (auto-detects watermark, no args needed)
python -m pipeline.run

# Step 4 — re-run week 1 (idempotency: 0 inserts, only updates)
python -m pipeline.run --since 2019-01-01 --until 2019-01-07
```

### 6. Start the daily scheduler

```bash
python -m pipeline.scheduler
```

Runs the pipeline daily at **06:00 UTC**, picking up the previous day's records via watermark.

---

## How Incremental Loads Work

The pipeline stores the last successfully loaded date in `IOWA_LIQUOR.RAW.PIPELINE_WATERMARKS`.

```
Run 1: loads 2019-01-01 → 2019-01-07   → watermark = 2019-01-07
Run 2: loads 2019-01-08 → yesterday    → watermark = yesterday
Run 3: since_date > until_date → skip (already up to date)
```

If a run fails halfway through, the watermark is **not** advanced. The next run re-fetches from the last known-good date and re-merges — no data loss, no manual cleanup.

---

## Verify in Snowflake

```sql
-- Row count
SELECT COUNT(*) FROM IOWA_LIQUOR.RAW.LIQUOR_SALES;

-- Pipeline run history
SELECT * FROM IOWA_LIQUOR.RAW.PIPELINE_WATERMARKS;

-- Daily sales trend
SELECT * FROM IOWA_LIQUOR.RAW.V_DAILY_SALES LIMIT 30;
```

---

## What I'd change in production

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full discussion. Short version:

| Area | Change |
|---|---|
| Orchestration | Airflow — adds a DAG UI, per-task logs, retry configuration, and easier backfills. `IowaLiquorPipeline.run()` drops straight into a `PythonOperator`. |
| Auth | Snowflake key-pair authentication + secrets in AWS SSM or Vault instead of a password in `.env`. |
| Bulk loading | For backfills >1M rows, use Snowflake internal stage + `COPY INTO` — significantly faster than `executemany`. |
| Schema evolution | Pre-run schema diff: auto-add nullable columns, alert and halt on renames or type changes. |
| Data quality | dbt tests on row counts, null rates, and value ranges after each load. |
| Observability | Structured JSON logs → Datadog; latency and row count tracked as metrics. |
| Testing | Unit tests for `_transform`, mocked HTTP tests for the extractor, integration tests against a Snowflake dev schema in CI. |

## Trade-offs

**Watermark vs. full reload** — A full reload is simpler but means downloading 16M rows
every day. Watermark-based incremental is faster but misses late-arriving corrections from
the source. I went with watermarks and noted that a rolling 7-day overlap window is a
reasonable mitigation if source corrections turn out to be common.

**MERGE vs. INSERT-only** — INSERT-only is faster (no lookup join) but creates duplicates
on retries. MERGE is slightly more expensive but means re-running any date range is always safe.

**APScheduler vs. Airflow** — APScheduler needs no extra infrastructure for a single daily
job. Airflow makes sense once you have multiple interdependent pipelines, need a backfill UI,
or want SLA alerts without writing custom code.

**`$offset` pagination** — Socrata's SODA 2.0 only supports offset-based pagination. This is
fine here because we sort deterministically over a bounded date window, so pages are stable.
For open-ended streaming a cursor on `updated_at` would be preferable.
