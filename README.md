# Flightline

A modern-stack flight-data pipeline. Two ingestion paths converge in a Snowflake
warehouse, get modeled and tested with dbt, and are served on a Streamlit
dashboard. Orchestrated with Airflow, packaged with Docker, gated by CI.

This is a personal project on the modern data stack. It is inspired by, but
separate from, prior production ETL work; the data here is US BTS data, not any
single carrier's internal data.

## Architecture

Two paths, because the two data sources have different SLAs:

- Batch path (Phase 1, built): BTS On-Time Performance, monthly CSVs, pulled and
  validated by an Airflow DAG, loaded into Snowflake `RAW`, modeled by dbt into
  staging and marts, served on Streamlit.
- Streaming path (Phase 2, scaffolded): OpenSky live state vectors, polled with
  OAuth2, pushed to Kafka, consumed into Snowflake `RAW`. Kafka is here for
  decoupling and replay, not throughput; at this volume a direct write would
  also work, and the design is built so a second consumer (alerting) can be
  added without touching the producer.

```
OpenSky -> Kafka -> consumer -.
                               \
BTS -> Airflow DAG ------------ Snowflake (RAW -> dbt staging -> marts) -> Streamlit
```

## Why these choices (the interview answers)

- Idempotent loads: the loader deletes the (year, month) partition before COPY,
  so re-running a DAG run or a backfill never duplicates rows.
- MATCH_BY_COLUMN_NAME on COPY: BTS files have ~110 columns. The target table
  declares only the ~25 we use; COPY maps by header and ignores the rest. No
  brittle positional column lists.
- dbt `build` in the DAG: runs models and tests in dependency order. A failing
  test fails the pipeline, which is the behaviour you want.
- Surrogate key + dedup in staging: BTS occasionally re-publishes corrected
  rows; `row_number()` over the natural key keeps one row per flight leg.
- Snowflake trial reality: the trial gives $400 / 30 days, then suspends. So the
  dashboard backend is pluggable (`FLIGHTLINE_BACKEND`). Build on Snowflake for
  the real keyword, then mirror the marts to DuckDB/MotherDuck so the public
  link stays live for free after the trial ends.

## Quickstart (Phase 1, batch)

1. `cp .env.example .env` and fill in your Snowflake trial credentials.
   Leave `FLIGHTLINE_SAMPLE=1` to run with synthetic data first.
2. `cd dbt/flightline && cp profiles.yml.example profiles.yml && cd ../..`
3. `make up` then open http://localhost:8080 (admin / admin).
4. In the Airflow UI, unpause and trigger the `bts_batch` DAG. It extracts,
   validates, loads to Snowflake, and runs `dbt build`.
5. `make dashboard` to launch Streamlit.

To use real data: set `FLIGHTLINE_SAMPLE=0`. The extractor pulls from the BTS
TranStats PREZIP endpoint, which is occasionally down; if it fails, download the
month by hand from transtats.bts.gov and pass it via `local_path`.

## Layout

```
airflow/        Dockerfile + DAGs (bts_batch)
ingestion/      bts_extract, validate, snowflake_loader, opensky_poller
dbt/flightline/ sources, staging, marts, tests, seeds, macros
dashboard/      Streamlit app (Snowflake or DuckDB backend)
.github/        CI: sqlfluff lint, Python compile, dbt build on an ephemeral schema
```

## Roadmap

- [x] Phase 1: batch backbone (BTS -> Airflow -> Snowflake -> dbt -> Streamlit + CI)
- [ ] Phase 2: streaming (OpenSky -> Kafka -> consumer -> Snowflake live table)
- [ ] Phase 3: incremental models, snapshots, source freshness alerting, mirror
      to MotherDuck for a permanently-live public dashboard
