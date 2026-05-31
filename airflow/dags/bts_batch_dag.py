"""Batch path: extract -> validate -> load BTS into Snowflake -> dbt build.

Monthly schedule. Each task is idempotent so a re-run (or a backfill) is safe.
The dbt step runs `dbt build`, which runs models AND tests in dependency order;
a failing test fails the DAG, which is the behaviour you want.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# Make the mounted ingestion package importable inside the worker.
sys.path.insert(0, "/opt/airflow/project")

DATA_DIR = os.getenv("FLIGHTLINE_DATA_DIR", "/opt/airflow/project/data")
DBT_DIR = "/opt/airflow/project/dbt/flightline"

default_args = {"owner": "flightline", "retries": 2}


@dag(
    dag_id="bts_batch",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["flightline", "batch", "bts"],
)
def bts_batch():
    @task
    def extract(**ctx) -> str:
        from ingestion.bts_extract import extract_bts
        # BTS publishes with a lag; target the month two months back.
        logical = ctx["logical_date"]
        y, m = logical.year, logical.month
        m -= 2
        if m <= 0:
            m += 12
            y -= 1
        path = extract_bts(y, m, dest_dir=DATA_DIR)
        return f"{path}|{y}|{m}"

    @task
    def validate(token: str) -> str:
        from ingestion.validate import validate_bts_csv
        path, y, m = token.split("|")
        stats = validate_bts_csv(path, int(y), int(m))
        print(f"validation passed: {stats}")
        return token

    @task
    def load(token: str) -> str:
        from ingestion.snowflake_loader import load_bts_csv
        path, y, m = token.split("|")
        n = load_bts_csv(path, int(y), int(m))
        print(f"loaded {n} rows for {y}-{int(m):02d}")
        return token

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt deps && "
            "dbt build --profiles-dir . --target prod"
        ),
    )

    token = extract()
    dbt_build.set_upstream(load(validate(token)))


bts_batch()
