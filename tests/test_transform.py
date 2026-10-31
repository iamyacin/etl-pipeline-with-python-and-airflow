from __future__ import annotations

from datetime import date

import pandas as pd

from ecommerce_etl.transform import (
    CUSTOMER_COLUMNS,
    FACT_COLUMNS,
    PRODUCT_COLUMNS,
    transform_orders,
)
from ecommerce_etl.validation import validate_orders


def test_totals_months_and_dimensions(order_frame) -> None:
    valid = validate_orders(order_frame).valid

    result = transform_orders(valid, source_file="orders.xlsx", run_id="run-1")

    assert list(result.fact.columns) == FACT_COLUMNS
    assert result.fact["line_total"].tolist() == [20.0, 90.0]
    assert result.fact["order_month"].tolist() == [date(2026, 10, 1), date(2026, 10, 1)]
    assert result.fact["ingest_run_id"].unique().tolist() == ["run-1"]
    assert set(result.customers["customer_id"]) == {"CUST-0001", "CUST-0002"}
    assert set(result.products["product_id"]) == {"P-1001", "P-1002"}
    assert result.revenue == 110.0


def test_dimensions_keep_the_latest_snapshot(order_frame) -> None:
    frame = order_frame.copy()
    frame.loc[1, "customer_id"] = "CUST-0001"
    frame.loc[1, "country"] = "FR"
    frame.loc[1, "order_date"] = "2026-10-05"
    valid = validate_orders(frame).valid

    result = transform_orders(valid, source_file="orders.xlsx", run_id="run-1")

    customers = result.customers.set_index("customer_id")
    assert len(customers) == 1
    assert customers.loc["CUST-0001", "country"] == "FR"
    assert customers.loc["CUST-0001", "first_order_date"] == date(2026, 10, 1)


def test_empty_input_returns_typed_empty_frames() -> None:
    result = transform_orders(
        pd.DataFrame(columns=["order_id", "source_row_number"]),
        source_file="orders.xlsx",
        run_id="run-1",
    )

    assert list(result.fact.columns) == FACT_COLUMNS
    assert list(result.customers.columns) == CUSTOMER_COLUMNS
    assert list(result.products.columns) == PRODUCT_COLUMNS
    assert result.revenue == 0.0
