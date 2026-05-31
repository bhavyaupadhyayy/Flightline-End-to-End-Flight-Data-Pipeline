"""Load a BTS monthly CSV into Snowflake RAW, idempotently.

Pattern (the bits that signal production thinking):
  - Defined target table with a typed subset of columns.
  - PUT the file to the table's internal stage, then COPY INTO.
  - MATCH_BY_COLUMN_NAME so the ~110-column BTS file maps by header and the
    columns we did not declare are simply ignored. No brittle positional $1,$2.
  - DELETE the target partition (year, month) before COPY, so re-running the
    same DAG run does not duplicate rows. Re-runnability is the whole point.
"""
from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector

RAW_SCHEMA = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")
TARGET_TABLE = "BTS_ONTIME"

DDL = f"""
create schema if not exists {{db}}.{RAW_SCHEMA};
create table if not exists {{db}}.{RAW_SCHEMA}.{TARGET_TABLE} (
    YEAR                              integer,
    MONTH                            integer,
    DAYOFMONTH                       integer,
    FLIGHTDATE                       date,
    REPORTING_AIRLINE                varchar,
    FLIGHT_NUMBER_REPORTING_AIRLINE  varchar,
    ORIGIN                           varchar,
    ORIGINCITYNAME                   varchar,
    DEST                             varchar,
    DESTCITYNAME                     varchar,
    CRSDEPTIME                       varchar,
    DEPDELAY                         float,
    DEPDELAYMINUTES                  float,
    ARRDELAY                         float,
    ARRDELAYMINUTES                  float,
    CANCELLED                        float,
    CANCELLATIONCODE                 varchar,
    DIVERTED                         float,
    AIRTIME                          float,
    DISTANCE                         float,
    CARRIERDELAY                     float,
    WEATHERDELAY                     float,
    NASDELAY                         float,
    SECURITYDELAY                    float,
    LATEAIRCRAFTDELAY                float,
    _LOADED_AT                       timestamp_ntz default current_timestamp()
);
"""

FILE_FORMAT_SQL = """
create or replace file format {db}.{schema}.FF_BTS_CSV
    type = csv
    parse_header = true
    field_optionally_enclosed_by = '"'
    null_if = ('', 'NA')
    empty_field_as_null = true
    error_on_column_count_mismatch = false;
"""


def _load_private_key():
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if not key_path:
        return None
    from cryptography.hazmat.primitives import serialization
    with open(os.path.expanduser(key_path), "rb") as f:
        pkey = serialization.load_pem_private_key(f.read(), password=None)
    return pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _conn():
    kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )
    pkb = _load_private_key()
    if pkb is not None:
        kwargs["private_key"] = pkb
    else:
        kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return snowflake.connector.connect(**kwargs)


def load_bts_csv(csv_path: str, year: int, month: int) -> int:
    db = os.environ["SNOWFLAKE_DATABASE"]
    path = Path(csv_path).resolve()
    if not path.exists():
        raise FileNotFoundError(csv_path)

    fq = f"{db}.{RAW_SCHEMA}.{TARGET_TABLE}"
    ctx = _conn()
    try:
        cur = ctx.cursor()
        for stmt in DDL.format(db=db).strip().split(";"):
            if stmt.strip():
                cur.execute(stmt)
        cur.execute(FILE_FORMAT_SQL.format(db=db, schema=RAW_SCHEMA))

        # Idempotency: clear this month before reloading it.
        cur.execute(f"delete from {fq} where YEAR = %s and MONTH = %s", (year, month))

        # Stage the file on the table's internal stage, then COPY.
        stage_path = f"@{db}.{RAW_SCHEMA}.%{TARGET_TABLE}"
        cur.execute(
            f"put file://{path} {stage_path} overwrite=true auto_compress=true"
        )
        cur.execute(
            f"""
            copy into {fq}
            from {stage_path}
            file_format = (format_name = '{db}.{RAW_SCHEMA}.FF_BTS_CSV')
            match_by_column_name = case_insensitive
            on_error = abort_statement
            purge = true
            """
        )
        cur.execute(f"select count(*) from {fq} where YEAR = %s and MONTH = %s",
                    (year, month))
        loaded = cur.fetchone()[0]
        return int(loaded)
    finally:
        ctx.close()


if __name__ == "__main__":
    import sys
    p, y, m = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    print(f"loaded {load_bts_csv(p, y, m)} rows")
