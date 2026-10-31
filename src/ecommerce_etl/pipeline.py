"""Staged pipeline functions shared by the CLI and the Airflow DAG.

Each stage exchanges small, JSON-serialisable dictionaries (paths and counts) via
XCom, while the bulk data lives in CSV files under a per-run staging directory.
That keeps task retries cheap and avoids putting DataFrames on Airflow's
metadata database.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ecommerce_etl.extract import extract_orders
from ecommerce_etl.transform import transform_orders
from ecommerce_etl.validation import validate_orders
from ecommerce_etl.warehouse import load_batch, run_quality_checks

# Bump this when validation/transform semantics change. It is part of the run ID,
# so stale quarantine rows from an older rule set are never reused.
RUN_VERSION = "1"

State = dict[str, Any]


def _run_id(source: Path) -> str:
    digest = hashlib.sha256(source.read_bytes()).hexdigest()[:12]
    return f"{source.stem[:24]}-v{RUN_VERSION}-{digest}"


def _read_csv(path: str | Path) -> pd.DataFrame:
    # Keep empty cells as empty strings instead of NaN so validation owns the
    # missing-value decisions in one place.
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def extract_stage(source_file: str | Path, staging_dir: str | Path) -> State:
    """Read the Excel file and persist the raw rows for the next stage."""

    source = Path(source_file)
    run_id = _run_id(source)
    run_dir = Path(staging_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    frame = extract_orders(source)
    raw_path = run_dir / "raw.csv"
    frame.to_csv(raw_path, index=False)

    return {
        "run_id": run_id,
        "source_file": source.name,
        "source_path": str(source),
        "started_at": datetime.now(UTC).isoformat(),
        "raw_path": str(raw_path),
        "rows_read": len(frame),
    }


def validate_stage(state: State) -> State:
    """Split raw rows into valid and quarantined sets."""

    raw = _read_csv(state["raw_path"])
    raw["source_row_number"] = raw["source_row_number"].astype(int)
    result = validate_orders(raw)

    raw_path = Path(state["raw_path"])
    valid_path = raw_path.with_name("valid.csv")
    rejected_path = raw_path.with_name("rejected.csv")
    result.valid.to_csv(valid_path, index=False)
    result.rejected.to_csv(rejected_path, index=False)

    return {
        **state,
        "valid_path": str(valid_path),
        "rejected_path": str(rejected_path),
        "rows_valid": len(result.valid),
        "rows_rejected": len(result.rejected),
    }


def transform_stage(state: State) -> State:
    """Derive the star-schema frames and persist them as CSV."""

    valid = _read_csv(state["valid_path"])
    valid["quantity"] = pd.to_numeric(valid["quantity"]).astype(int)
    valid["unit_price"] = pd.to_numeric(valid["unit_price"]).astype(float)
    valid["discount"] = pd.to_numeric(valid["discount"]).astype(float)

    result = transform_orders(
        valid,
        source_file=state["source_file"],
        run_id=state["run_id"],
    )

    valid_path = Path(state["valid_path"])
    fact_path = valid_path.with_name("fact_order_items.csv")
    customers_path = valid_path.with_name("dim_customers.csv")
    products_path = valid_path.with_name("dim_products.csv")
    result.fact.to_csv(fact_path, index=False)
    result.customers.to_csv(customers_path, index=False)
    result.products.to_csv(products_path, index=False)

    return {
        **state,
        "fact_path": str(fact_path),
        "customers_path": str(customers_path),
        "products_path": str(products_path),
        "rows_upserted": 0,
        "revenue": result.revenue,
    }


def load_stage(state: State, dsn: str) -> State:
    """Upsert the staged frames into PostgreSQL."""

    fact = _read_csv(state["fact_path"])
    fact["quantity"] = pd.to_numeric(fact["quantity"]).astype(int)
    for column in ("unit_price", "discount", "line_total"):
        fact[column] = pd.to_numeric(fact[column]).astype(float)

    customers = _read_csv(state["customers_path"])
    products = _read_csv(state["products_path"])
    products["last_unit_price"] = pd.to_numeric(products["last_unit_price"]).astype(float)

    rejected = _read_csv(state["rejected_path"])
    if not rejected.empty:
        rejected["source_row_number"] = rejected["source_row_number"].astype(int)

    metrics = load_batch(
        dsn=dsn,
        run_id=state["run_id"],
        source_file=state["source_file"],
        started_at=state["started_at"],
        rows_read=state["rows_read"],
        rows_valid=state["rows_valid"],
        rows_rejected=state["rows_rejected"],
        fact=fact,
        customers=customers,
        products=products,
        rejected=rejected,
        revenue=round(float(fact["line_total"].sum()), 2),
    )
    return {**state, **metrics}


def quality_stage(state: State, dsn: str, max_reject_rate: float) -> State:
    """Assert the loaded batch is complete, unique and within the reject limit."""

    result = run_quality_checks(
        dsn=dsn,
        run_id=state["run_id"],
        rows_valid=state["rows_valid"],
        rows_rejected=state["rows_rejected"],
        max_reject_rate=max_reject_rate,
    )
    return {**state, **result}


def run_pipeline(
    source_file: str | Path,
    *,
    staging_dir: str | Path,
    dsn: str | None = None,
    max_reject_rate: float = 0.2,
    dry_run: bool = False,
) -> State:
    """Run all stages locally, optionally stopping before the warehouse load."""

    state = extract_stage(source_file, staging_dir)
    state = validate_stage(state)
    state = transform_stage(state)
    if dry_run:
        return state
    if not dsn:
        raise ValueError("A warehouse DSN is required unless dry_run=True")
    state = load_stage(state, dsn)
    return quality_stage(state, dsn, max_reject_rate)
