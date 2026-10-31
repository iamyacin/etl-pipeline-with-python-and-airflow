from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ecommerce_etl.exceptions import SchemaError
from ecommerce_etl.extract import extract_orders


def test_reads_the_sample_export(sample_file: Path) -> None:
    frame = extract_orders(sample_file)

    assert len(frame) > 300
    assert frame["source_row_number"].iloc[0] == 2
    assert {"order_id", "order_date", "quantity", "discount"} <= set(frame.columns)
    assert frame["source_row_number"].is_monotonic_increasing


def test_missing_file_is_reported(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="Source file not found"):
        extract_orders(tmp_path / "nope.xlsx")


def test_missing_columns_fail_fast(tmp_path) -> None:
    path = tmp_path / "broken.xlsx"
    pd.DataFrame({"order_id": ["ORD-1"], "quantity": [1]}).to_excel(path, index=False)

    with pytest.raises(SchemaError, match="missing required columns"):
        extract_orders(path)


def test_optional_discount_defaults_to_zero(tmp_path) -> None:
    path = tmp_path / "no-discount.xlsx"
    pd.DataFrame(
        [
            {
                "order_id": "ORD-1",
                "customer_id": "CUST-1",
                "order_date": "2026-10-01",
                "product_id": "P-1",
                "product_name": "Item",
                "category": "Home",
                "quantity": 1,
                "unit_price": 5.0,
                "status": "delivered",
                "country": "US",
            }
        ]
    ).to_excel(path, index=False)

    frame = extract_orders(path)

    assert frame["discount"].tolist() == [0]
    assert frame["source_row_number"].tolist() == [2]


def test_headers_are_normalised(tmp_path) -> None:
    path = tmp_path / "headers.xlsx"
    pd.DataFrame(
        [
            {
                " Order_ID ": "ORD-1",
                "CUSTOMER_ID": "CUST-1",
                "Order_Date": "2026-10-01",
                "product_id": "P-1",
                "product_name": "Item",
                "category": "Home",
                "quantity": 1,
                "unit_price": 5.0,
                "status": "delivered",
                "country": "US",
            }
        ]
    ).to_excel(path, index=False)

    frame = extract_orders(path)

    assert frame["order_id"].tolist() == ["ORD-1"]
    assert "customer_id" in frame.columns
