"""Offline tests for the extract and validation steps. No network or Snowflake needed."""
from __future__ import annotations

import csv

import pytest

from ingestion.bts_extract import SAMPLE_COLUMNS, extract_bts
from ingestion.validate import MIN_ROWS, ValidationError, validate_bts_csv


@pytest.fixture
def sample_csv(tmp_path, monkeypatch):
    monkeypatch.setenv("FLIGHTLINE_SAMPLE", "1")
    return extract_bts(2024, 3, dest_dir=tmp_path)


def _rewrite(path, mutate):
    """Load a CSV, apply mutate(rows) -> rows, and write it back."""
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        fields, rows = reader.fieldnames, list(reader)
    rows = mutate(rows)
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_sample_extract_is_schema_faithful(sample_csv):
    with open(sample_csv, newline="") as fh:
        assert csv.DictReader(fh).fieldnames == SAMPLE_COLUMNS


def test_sample_extract_is_deterministic(tmp_path, monkeypatch):
    monkeypatch.setenv("FLIGHTLINE_SAMPLE", "1")
    first = open(extract_bts(2024, 3, dest_dir=tmp_path / "a")).read()
    second = open(extract_bts(2024, 3, dest_dir=tmp_path / "b")).read()
    assert first == second


def test_sample_extract_passes_validation(sample_csv):
    stats = validate_bts_csv(sample_csv, 2024, 3)
    assert stats["rows"] == 5000
    assert stats["null_key"] == 0


def test_local_path_is_copied(tmp_path, sample_csv):
    out = extract_bts(2024, 3, dest_dir=tmp_path / "copy", local_path=sample_csv)
    assert open(out).read() == open(sample_csv).read()


def test_missing_local_path_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        extract_bts(2024, 3, dest_dir=tmp_path, local_path=tmp_path / "nope.csv")


def test_missing_file_fails_validation(tmp_path):
    with pytest.raises(ValidationError, match="file missing"):
        validate_bts_csv(str(tmp_path / "nope.csv"), 2024, 3)


def test_missing_required_column_fails(sample_csv):
    with open(sample_csv, newline="") as fh:
        rows = list(csv.DictReader(fh))
    with open(sample_csv, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=[c for c in SAMPLE_COLUMNS if c != "Dest"],
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    with pytest.raises(ValidationError, match="Dest"):
        validate_bts_csv(sample_csv, 2024, 3)


def test_too_few_rows_fails(sample_csv):
    _rewrite(sample_csv, lambda rows: rows[: MIN_ROWS - 1])
    with pytest.raises(ValidationError, match="rows"):
        validate_bts_csv(sample_csv, 2024, 3)


def test_null_origin_fails(sample_csv):
    def blank_first_origin(rows):
        rows[0]["Origin"] = ""
        return rows

    _rewrite(sample_csv, blank_first_origin)
    with pytest.raises(ValidationError, match="null origin/dest"):
        validate_bts_csv(sample_csv, 2024, 3)


def test_wrong_month_fails(sample_csv):
    with pytest.raises(ValidationError, match="not in 2024-04"):
        validate_bts_csv(sample_csv, 2024, 4)


def test_small_share_of_wrong_month_rows_is_tolerated(sample_csv):
    def shift_one_row(rows):
        rows[0]["Month"] = "2"
        return rows

    _rewrite(sample_csv, shift_one_row)
    assert validate_bts_csv(sample_csv, 2024, 3)["bad_month"] == 1
