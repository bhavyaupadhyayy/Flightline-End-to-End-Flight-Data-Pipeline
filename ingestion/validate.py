"""Cheap, fast data-quality gate that runs before the load.

Catching a malformed extract here is far cheaper than catching it after it has
landed in the warehouse and broken every downstream dbt model.
"""
from __future__ import annotations

import csv
from pathlib import Path

REQUIRED = {"Year", "Month", "FlightDate", "Reporting_Airline", "Origin", "Dest"}
MIN_ROWS = 100


class ValidationError(Exception):
    pass


def validate_bts_csv(csv_path: str, expected_year: int, expected_month: int) -> dict:
    path = Path(csv_path)
    if not path.exists():
        raise ValidationError(f"file missing: {csv_path}")

    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        header = set(reader.fieldnames or [])
        missing = REQUIRED - header
        if missing:
            raise ValidationError(f"missing required columns: {sorted(missing)}")

        rows = 0
        bad_month = 0
        null_key = 0
        for r in reader:
            rows += 1
            if r.get("Origin") in (None, "") or r.get("Dest") in (None, ""):
                null_key += 1
            try:
                if int(r["Month"]) != expected_month or int(r["Year"]) != expected_year:
                    bad_month += 1
            except (ValueError, KeyError):
                bad_month += 1

    if rows < MIN_ROWS:
        raise ValidationError(f"only {rows} rows, expected >= {MIN_ROWS}")
    if null_key:
        raise ValidationError(f"{null_key} rows with null origin/dest")
    if bad_month > rows * 0.01:
        raise ValidationError(
            f"{bad_month} rows not in {expected_year}-{expected_month:02d}"
        )

    return {"rows": rows, "bad_month": bad_month, "null_key": null_key}


if __name__ == "__main__":
    import sys
    print(validate_bts_csv(sys.argv[1], int(sys.argv[2]), int(sys.argv[3])))
