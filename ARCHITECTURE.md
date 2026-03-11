# Iowa Liquor Sales Pipeline

## Approach

Track the last date we loaded, fetch everything since then, and only update the tracker
once all the data is safely in Snowflake. If anything goes wrong mid-run, the tracker
stays where it was and the next run picks up from the same point.

The source is the Socrata SODA API at data.iowa.gov. It supports server-side date
filtering, so each run only pulls the new records rather than the full dataset (~2GB
as CSV) every time.

---

## Incremental loading

Each run does this:

1. Read last_loaded_date from PIPELINE_WATERMARKS in Snowflake
2. Fetch records where date > last_loaded_date AND date <= yesterday
3. Load them into Snowflake in pages of 10,000
4. Update last_loaded_date to yesterday

We stop at yesterday rather than today because Iowa's portal sometimes takes a few
hours to finalise the previous day's records. Pulling today mid-day risks loading
a partial window that we'd then need to correct.

On the first run (no watermark), the pipeline starts from 2012-01-01 — the beginning
of the dataset. That's a large one-off load, but after that each daily run is small.

The main trade-off is late-arriving corrections. Iowa occasionally goes back and amends
historical records; the watermark won't pick those up automatically. The fix is to
re-run with `--since <date>` for the affected window. A rolling 7-day overlap on every
run would also catch corrections but adds unnecessary load for how rarely this happens.

---

## Loading into Snowflake

Each page of records is written to a temporary staging table, then merged into
LIQUOR_SALES:

```sql
MERGE INTO LIQUOR_SALES
USING LIQUOR_SALES_STAGE ON invoice_item_number = invoice_item_number
WHEN MATCHED THEN UPDATE 
WHEN NOT MATCHED THEN INSERT 
```

MERGE rather than plain INSERT means the pipeline can be re-run for any date range
without creating duplicates. This matters for retries — if a run fails after writing
half the pages, the next run re-fetches the whole window and re-merges safely.

The staging table is session-scoped so it's dropped automatically when the connection
closes.

One limitation: executemany into the staging table is fine for the daily load
(a few thousand rows) but would be too slow for the initial full backfill of 16M rows.
For that I'd switch to writing to a Snowflake internal stage and running COPY INTO,
which is much faster for bulk loads. More on this in the production section.

---

## Pagination

The Socrata API paginates with `$limit` + `$offset`. Without a sort order, rows can
shift between pages if new records arrive mid-fetch — causing skips or duplicates.

We use ORDER BY date ASC, invoice_line_no ASC to keep pages stable across requests.

---

## Scheduling

The pipeline runs at 06:00 UTC daily. Iowa's portal typically finishes writing the
previous day's records by around 3 AM CT, so 06:00 UTC gives enough of a gap.

I used APScheduler rather than Airflow because there's only one job and no need for
a UI or task dependencies. `pipeline.run()` is a plain Python function, so moving it
to an Airflow DAG later would just be wrapping it in a `PythonOperator`.

Two scheduler settings:
- coalesce=True — if the server missed several triggers (e.g. was restarted), run
  once to catch up rather than firing back-to-back
- max_instances=1 — the watermark update at the end isn't safe across concurrent
  runs, so we enforce one run at a time

---

## Failure handling

Transient API errors retry with exponential backoff (5s → 10s → 20s). If Snowflake throws an error, a Slack alert is triggered and the watermark is left unchanged. The next scheduled run re-fetches from the same date.

The watermark update is deliberately the last thing in the run. If it happened any
earlier, a crash before all pages landed would advance the watermark past unloaded
data, creating a silent gap with no way to detect it.

---

## Production Changes

**Airflow for orchestration.** APScheduler has no visibility into past runs. With
Airflow you get a DAG UI, per-task logs, retry configuration, and the ability to
trigger backfills from the UI without touching the code or SSHing anywhere. The
pipeline's run() method drops straight into a PythonOperator.

**Key-pair auth for Snowflake.** Username and password in `.env` is fine locally,
but in a shared environment credentials should sit in AWS SSM Parameter Store and
the connection should use Snowflake's key-pair authentication.

**`COPY INTO` for backfills.** For loading a large historical window, write pages
to a Snowflake internal stage as Parquet files and use `COPY INTO` rather than
`executemany`. For the daily incremental what we have is fast enough; for a full
year's worth of data it's not.

**Row count checks after each load.** Iowa's dataset has occasional quality issues —
null store numbers, zero-dollar sales. Right now the pipeline loads whatever comes
back from the API. In production I'd add a check: if the row count for a day is
more than 3× the 30-day average, or if sale_dollars has an unexpected null rate,
fail the run and alert rather than silently loading bad data.
