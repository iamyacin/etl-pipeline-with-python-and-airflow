from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import psycopg
import pytest

from ecommerce_etl.exceptions import DataQualityError
from ecommerce_etl.pipeline import run_pipeline
from ecommerce_etl.transform import CUSTOMER_COLUMNS, FACT_COLUMNS, PRODUCT_COLUMNS
from ecommerce_etl.warehouse import load_batch, run_quality_checks, warehouse_summary


@pytest.mark.integration
def test_pipeline_load_is_idempotent(sample_file: Path, tmp_path, clean_db: str) -> None:
    first = run_pipeline(sample_file, staging_dir=tmp_path, dsn=clean_db)

    assert first["quality_status"] == "success"
    assert first["rows_upserted"] == first["rows_valid"]
    assert first["rows_rejected"] > 0

    second = run_pipeline(sample_file, staging_dir=tmp_path, dsn=clean_db)

    assert second["run_id"] == first["run_id"]
    assert second["quality_status"] == "success"

    with psycopg.connect(clean_db) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM fact_order_items")
        fact_rows = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM rejected_order_items")
        rejected_rows = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM etl_run_log")
        run_rows = cursor.fetchone()[0]

    assert fact_rows == first["rows_valid"]
    assert rejected_rows == first["rows_rejected"]
    assert run_rows == 1

    summary = warehouse_summary(clean_db)
    assert summary["totals"]["orders"] > 0
    assert summary["rejected_rows"] == first["rows_rejected"]
    assert summary["latest_run"]["status"] == "success"


@pytest.mark.integration
def test_fact_upsert_updates_an_existing_line(clean_db: str) -> None:
    def load(line_total: float, run_id: str) -> None:
        fact = pd.DataFrame(
            [
                {
                    "order_id": "ORD-UPSERT",
                    "customer_id": "CUST-UPSERT",
                    "product_id": "P-UPSERT",
                    "order_date": date(2026, 10, 1),
                    "order_month": date(2026, 10, 1),
                    "quantity": 1,
                    "unit_price": 10.0,
                    "discount": 0.0,
                    "line_total": line_total,
                    "status": "delivered",
                    "country": "US",
                    "source_file": "test.xlsx",
                    "ingest_run_id": run_id,
                }
            ],
            columns=FACT_COLUMNS,
        )
        customers = pd.DataFrame(
            [
                {
                    "customer_id": "CUST-UPSERT",
                    "country": "US",
                    "first_order_date": date(2026, 10, 1),
                }
            ],
            columns=CUSTOMER_COLUMNS,
        )
        products = pd.DataFrame(
            [
                {
                    "product_id": "P-UPSERT",
                    "product_name": "Test product",
                    "category": "Home",
                    "last_unit_price": 10.0,
                }
            ],
            columns=PRODUCT_COLUMNS,
        )
        rejected = pd.DataFrame(
            columns=[
                "ingest_run_id",
                "source_file",
                "source_row_number",
                "raw_row",
                "reject_reason",
            ]
        )
        load_batch(
            dsn=clean_db,
            run_id=run_id,
            source_file="test.xlsx",
            started_at=datetime.now(UTC).isoformat(),
            rows_read=1,
            rows_valid=1,
            rows_rejected=0,
            fact=fact,
            customers=customers,
            products=products,
            rejected=rejected,
            revenue=line_total,
        )

    load(10.0, "upsert-run-1")
    load(12.0, "upsert-run-2")

    with psycopg.connect(clean_db) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*), sum(line_total), max(ingest_run_id) FROM fact_order_items")
        count, total, latest_run = cursor.fetchone()

    assert count == 1
    assert float(total) == 12.0
    assert latest_run == "upsert-run-2"


@pytest.mark.integration
def test_quality_gate_fails_when_rows_are_missing(clean_db: str) -> None:
    with pytest.raises(DataQualityError, match="Quality gate failed"):
        run_quality_checks(
            dsn=clean_db,
            run_id="missing-run",
            rows_valid=1,
            rows_rejected=0,
            max_reject_rate=0.2,
        )
