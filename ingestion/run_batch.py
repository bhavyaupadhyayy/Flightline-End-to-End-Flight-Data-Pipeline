"""Run the batch path locally without Airflow: extract -> validate -> load."""
from dotenv import load_dotenv
load_dotenv()

from ingestion.bts_extract import extract_bts
from ingestion.validate import validate_bts_csv
from ingestion.snowflake_loader import load_bts_csv

YEAR, MONTH = 2024, 3

path = extract_bts(YEAR, MONTH, dest_dir="data")
print(f"extracted: {path}")

stats = validate_bts_csv(path, YEAR, MONTH)
print(f"validated: {stats}")

n = load_bts_csv(path, YEAR, MONTH)
print(f"loaded {n} rows into FLIGHTLINE.RAW.BTS_ONTIME")