from __future__ import annotations

from pathlib import Path

import pytest

from ecommerce_etl.pipeline import extract_stage, run_pipeline


def test_dry_run_stops_before_the_warehouse(sample_file: Path, tmp_path) -> None:
    state = run_pipeline(sample_file, staging_dir=tmp_path, dry_run=True)

    assert state["rows_read"] == state["rows_valid"] + state["rows_rejected"]
    assert state["rows_rejected"] > 0
    assert state["revenue"] > 0
    assert Path(state["fact_path"]).is_file()
    assert Path(state["rejected_path"]).is_file()
    assert "quality_status" not in state


def test_run_id_is_derived_from_file_content(sample_file: Path, tmp_path) -> None:
    first = extract_stage(sample_file, tmp_path)
    second = extract_stage(sample_file, tmp_path)

    assert first["run_id"] == second["run_id"]
    assert first["run_id"].startswith("orders_2026_10-v1-")


def test_missing_source_raises_before_writing(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        run_pipeline(tmp_path / "missing.xlsx", staging_dir=tmp_path, dry_run=True)
