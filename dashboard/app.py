"""Flightline dashboard. Reads the dbt marts and renders carrier and route views.

Data source is configurable. Point it at Snowflake during the 30-day trial; for
a permanently-live public link, point it at the mirrored marts (MotherDuck or
Neon) so the dashboard does not die when the Snowflake trial expires.
Set FLIGHTLINE_BACKEND=snowflake (default) or =duckdb.
"""
from __future__ import annotations

import os

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Flightline", page_icon=None, layout="wide")

BACKEND = os.getenv("FLIGHTLINE_BACKEND", "snowflake")


@st.cache_resource
def _snowflake_conn():
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    key_path = os.path.expanduser(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"])
    with open(key_path, "rb") as f:
        pkey = serialization.load_pem_private_key(f.read(), password=None)
    pkb = pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=pkb,
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="ANALYTICS_MARTS",
    )

@st.cache_data(ttl=600)
def query(sql: str) -> pd.DataFrame:
    if BACKEND == "duckdb":
        import duckdb
        con = duckdb.connect(os.getenv("DUCKDB_PATH", "flightline.duckdb"))
        return con.execute(sql).fetch_df()
    cur = _snowflake_conn().cursor()
    cur.execute(sql)
    cols = [c[0].lower() for c in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


st.title("Flightline")
st.caption("US flight on-time performance, modeled from BTS data with dbt.")

try:
    carriers = query("select * from agg_carrier_ontime")
    routes = query("select * from agg_route_delay limit 25")
except Exception as exc:  # noqa: BLE001
    st.error(f"Could not load data: {exc}")
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("Carriers", len(carriers))
col2.metric("Total flights", f"{int(carriers['total_flights'].sum()):,}")
best = carriers.sort_values("on_time_rate", ascending=False).iloc[0]
col3.metric("Most on-time", best["carrier_name"], f"{best['on_time_rate']:.0%}")

st.subheader("On-time rate by carrier")
chart_df = carriers.set_index("carrier_name")[["on_time_rate"]].sort_values(
    "on_time_rate", ascending=False
)
st.bar_chart(chart_df)

st.subheader("Average arrival delay by carrier (minutes)")
st.bar_chart(
    carriers.set_index("carrier_name")[["avg_arr_delay_min"]].sort_values(
        "avg_arr_delay_min"
    )
)

st.subheader("Worst routes by average arrival delay")
st.dataframe(
    routes[["origin_airport", "dest_airport", "total_flights",
            "avg_arr_delay_min", "avg_dep_delay_min"]],
    use_container_width=True,
    hide_index=True,
)
