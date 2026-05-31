"""Extract BTS Reporting Carrier On-Time Performance data for one month.

Resolution order:
  1. local_path, if provided (use a file you downloaded by hand).
  2. download from the BTS TranStats PREZIP endpoint.
  3. SAMPLE mode (env FLIGHTLINE_SAMPLE=1) -> generate a tiny synthetic file
     so the pipeline runs end to end with zero external dependencies.

The TranStats endpoint is form-driven and occasionally flaky, which is exactly
why the extractor is layered: the DAG must be demoable even when bts.gov is down.
"""
from __future__ import annotations

import csv
import io
import os
import random
import zipfile
from datetime import date, timedelta
from pathlib import Path

import requests

PREZIP_BASE = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)

# Columns we care about. dbt staging selects from these; extra BTS columns
# (there are ~110) are ignored downstream via MATCH_BY_COLUMN_NAME on load.
SAMPLE_COLUMNS = [
    "Year", "Month", "DayofMonth", "FlightDate", "Reporting_Airline",
    "Flight_Number_Reporting_Airline", "Origin", "OriginCityName", "Dest",
    "DestCityName", "CRSDepTime", "DepDelay", "DepDelayMinutes", "ArrDelay",
    "ArrDelayMinutes", "Cancelled", "CancellationCode", "Diverted", "AirTime",
    "Distance", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
    "LateAircraftDelay",
]
_CARRIERS = ["AA", "DL", "UA", "WN", "AS", "B6", "NK", "F9"]
_AIRPORTS = ["LAX", "SFO", "JFK", "ORD", "ATL", "DFW", "SEA", "DEN", "BOS"]


def _csv_path(dest_dir: Path, year: int, month: int) -> Path:
    return dest_dir / f"bts_ontime_{year}_{month:02d}.csv"


def _generate_sample(dest_dir: Path, year: int, month: int, n: int = 5000) -> Path:
    """Write a synthetic but schema-faithful BTS file for offline demos."""
    out = _csv_path(dest_dir, year, month)
    rng = random.Random(f"{year}-{month}")
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=SAMPLE_COLUMNS)
        w.writeheader()
        for _ in range(n):
            o, d = rng.sample(_AIRPORTS, 2)
            dep_delay = max(0, int(rng.gauss(8, 30)))
            arr_delay = max(-15, dep_delay + int(rng.gauss(0, 12)))
            cancelled = 1 if rng.random() < 0.02 else 0
            day = rng.randint(1, 28)
            w.writerow({
                "Year": year, "Month": month, "DayofMonth": day,
                "FlightDate": f"{year}-{month:02d}-{day:02d}",
                "Reporting_Airline": rng.choice(_CARRIERS),
                "Flight_Number_Reporting_Airline": rng.randint(1, 6000),
                "Origin": o, "OriginCityName": o, "Dest": d, "DestCityName": d,
                "CRSDepTime": f"{rng.randint(0,23):02d}{rng.randint(0,59):02d}",
                "DepDelay": dep_delay if not cancelled else "",
                "DepDelayMinutes": dep_delay if not cancelled else "",
                "ArrDelay": arr_delay if not cancelled else "",
                "ArrDelayMinutes": max(0, arr_delay) if not cancelled else "",
                "Cancelled": cancelled, "CancellationCode": "" if not cancelled else "B",
                "Diverted": 0, "AirTime": rng.randint(40, 360) if not cancelled else "",
                "Distance": rng.randint(150, 2800),
                "CarrierDelay": "", "WeatherDelay": "", "NASDelay": "",
                "SecurityDelay": "", "LateAircraftDelay": "",
            })
    return out


def _download_prezip(dest_dir: Path, year: int, month: int) -> Path:
    url = PREZIP_BASE.format(year=year, month=month)
    resp = requests.get(url, timeout=120, headers={"User-Agent": "flightline/0.1"})
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        # Filename casing has changed over the years; glob instead of hardcode.
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV inside archive from {url}")
        inner = csv_names[0]
        out = _csv_path(dest_dir, year, month)
        with zf.open(inner) as src, out.open("wb") as dst:
            dst.write(src.read())
    return out


def extract_bts(
    year: int,
    month: int,
    dest_dir: str | os.PathLike = "data",
    local_path: str | os.PathLike | None = None,
) -> str:
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)

    if local_path:
        src = Path(local_path)
        if not src.exists():
            raise FileNotFoundError(local_path)
        out = _csv_path(dest, year, month)
        out.write_bytes(src.read_bytes())
        return str(out)

    if os.getenv("FLIGHTLINE_SAMPLE") == "1":
        return str(_generate_sample(dest, year, month))

    return str(_download_prezip(dest, year, month))


if __name__ == "__main__":
    today = date.today().replace(day=1) - timedelta(days=1)
    os.environ.setdefault("FLIGHTLINE_SAMPLE", "1")
    path = extract_bts(today.year, today.month)
    print(f"wrote {path}")
