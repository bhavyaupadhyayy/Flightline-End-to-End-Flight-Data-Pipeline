"""Build a local DuckDB file with mart tables from the sample BTS CSV.
Run this once to make the dashboard work without a live Snowflake connection.
"""
import duckdb
import pandas as pd

DB_PATH = "flightline.duckdb"
CSV_PATH = "data/bts_ontime_2024_03.csv"

CARRIERS = {
    "AA": "American Airlines", "DL": "Delta Air Lines", "UA": "United Airlines",
    "WN": "Southwest Airlines", "AS": "Alaska Airlines", "B6": "JetBlue Airways",
    "NK": "Spirit Airlines", "F9": "Frontier Airlines", "HA": "Hawaiian Airlines",
    "G4": "Allegiant Air",
}

raw = pd.read_csv(CSV_PATH, low_memory=False)
raw.columns = [c.lower() for c in raw.columns]

# stg_bts__ontime: rename + deduplicate
stg = raw.rename(columns={
    "flightdate": "flight_date",
    "year": "flight_year",
    "month": "flight_month",
    "reporting_airline": "carrier_code",
    "flight_number_reporting_airline": "flight_number",
    "origin": "origin_airport",
    "origincityname": "origin_city",
    "dest": "dest_airport",
    "destcityname": "dest_city",
    "depdelay": "dep_delay_min",
    "arrdelay": "arr_delay_min",
    "cancelled": "is_cancelled",
    "diverted": "is_diverted",
    "cancellationcode": "cancellation_code",
    "airtime": "air_time_min",
    "distance": "distance_mi",
    "carrierdelay": "carrier_delay_min",
    "weatherdelay": "weather_delay_min",
    "nasdelay": "nas_delay_min",
    "securitydelay": "security_delay_min",
    "lateaircraftdelay": "late_aircraft_delay_min",
    "crsdeptime": "crs_dep_time",
}).copy()
stg["is_cancelled"] = stg["is_cancelled"].fillna(0).astype(bool)
stg["is_diverted"] = stg["is_diverted"].fillna(0).astype(bool)
stg["flight_key"] = (
    stg["flight_date"].astype(str) + stg["carrier_code"] +
    stg["flight_number"].astype(str) + stg["origin_airport"] +
    stg["dest_airport"] + stg["crs_dep_time"].astype(str)
)
stg = stg.drop_duplicates(subset=["flight_key"])

# fct_flights
fct = stg.copy()
fct["is_on_time"] = None
mask_normal = ~fct["is_cancelled"] & ~fct["is_diverted"]
fct.loc[mask_normal & (fct["arr_delay_min"] < 15), "is_on_time"] = True
fct.loc[mask_normal & (fct["arr_delay_min"] >= 15), "is_on_time"] = False
for col in ["carrier_delay_min", "weather_delay_min", "nas_delay_min",
            "security_delay_min", "late_aircraft_delay_min"]:
    fct[col] = fct[col].fillna(0)

# dim_carrier
dim_carrier = pd.DataFrame([
    {"carrier_code": k, "carrier_name": v} for k, v in CARRIERS.items()
])
carriers_in_data = fct["carrier_code"].unique()
extra = pd.DataFrame([
    {"carrier_code": c, "carrier_name": c}
    for c in carriers_in_data if c not in CARRIERS
])
dim_carrier = pd.concat([dim_carrier, extra], ignore_index=True)

# agg_carrier_ontime
merged = fct.merge(dim_carrier, on="carrier_code", how="left")
agg_carrier = (
    merged.groupby(["carrier_code", "carrier_name"])
    .apply(lambda g: pd.Series({
        "total_flights": len(g),
        "cancelled_flights": g["is_cancelled"].sum(),
        "avg_arr_delay_min": g["arr_delay_min"].mean(),
        "on_time_rate": g["is_on_time"].mean(),
    }), include_groups=False)
    .reset_index()
    .sort_values("on_time_rate", ascending=False)
)

# agg_route_delay
agg_route = (
    fct.groupby(["origin_airport", "dest_airport"])
    .agg(
        total_flights=("flight_key", "count"),
        avg_arr_delay_min=("arr_delay_min", "mean"),
        avg_dep_delay_min=("dep_delay_min", "mean"),
        distance_mi=("distance_mi", "mean"),
    )
    .reset_index()
    .query("total_flights >= 5")
    .sort_values("avg_arr_delay_min", ascending=False)
    .head(25)
)

con = duckdb.connect(DB_PATH)
con.execute("CREATE OR REPLACE TABLE agg_carrier_ontime AS SELECT * FROM agg_carrier")
con.execute("CREATE OR REPLACE TABLE agg_route_delay AS SELECT * FROM agg_route")
con.close()

print(f"Written {DB_PATH}")
print(f"  agg_carrier_ontime: {len(agg_carrier)} rows")
print(f"  agg_route_delay:    {len(agg_route)} rows")
