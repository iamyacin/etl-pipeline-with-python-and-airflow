from __future__ import annotations

import pandas as pd
import pytest

from ecommerce_etl.validation import validate_orders


def test_valid_rows_are_normalised(order_row) -> None:
    frame = pd.DataFrame(
        [
            order_row(
                status=" Delivered ",
                country=" us ",
                quantity="2",
                unit_price="10.00",
                discount="0.1",
            )
        ]
    )

    result = validate_orders(frame)

    assert len(result.valid) == 1
    assert result.rejected.empty
    assert result.valid.loc[0, "status"] == "delivered"
    assert result.valid.loc[0, "country"] == "US"
    assert result.valid.loc[0, "quantity"] == 2
    assert result.valid.loc[0, "unit_price"] == 10.0


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"customer_id": None}, "missing customer_id"),
        ({"product_id": ""}, "missing product_id"),
        ({"quantity": 0}, "quantity must be positive"),
        ({"quantity": -2}, "quantity must be positive"),
        ({"quantity": 1.5}, "quantity must be a whole number"),
        ({"quantity": "n/a"}, "quantity is not numeric"),
        ({"unit_price": "N/A"}, "unit_price is not numeric"),
        ({"unit_price": -1}, "unit_price must not be negative"),
        ({"status": "refunded"}, "invalid status"),
        ({"order_date": "not-a-date"}, "invalid order_date"),
        ({"country": "USA"}, "country must be a 2-letter code"),
        ({"discount": 1.5}, "discount must be between 0 and 1"),
        ({"discount": "ten"}, "discount is not numeric"),
    ],
)
def test_bad_rows_are_quarantined_with_a_reason(order_row, overrides, reason) -> None:
    result = validate_orders(pd.DataFrame([order_row(**overrides)]))

    assert result.valid.empty
    assert len(result.rejected) == 1
    assert reason in result.rejected.loc[0, "reject_reason"]


def test_duplicate_order_lines_are_rejected(order_row) -> None:
    frame = pd.DataFrame([order_row(), order_row()])

    result = validate_orders(frame)

    assert len(result.valid) == 1
    assert len(result.rejected) == 1
    assert "duplicate order line" in result.rejected.loc[0, "reject_reason"]


def test_total_row_count_is_preserved(order_row) -> None:
    frame = pd.DataFrame(
        [
            order_row(),
            order_row(order_id="ORD-2", customer_id=None),
            order_row(order_id="ORD-3", quantity=0),
        ]
    )

    result = validate_orders(frame)

    assert result.rows_read == 3
    assert len(result.valid) + len(result.rejected) == 3
    assert result.rejected["source_row_number"].is_monotonic_increasing


def test_multiple_reasons_are_joined(order_row) -> None:
    result = validate_orders(
        pd.DataFrame([order_row(customer_id=None, status="unknown", quantity=0)])
    )

    reason = result.rejected.loc[0, "reject_reason"]
    assert "missing customer_id" in reason
    assert "invalid status" in reason
    assert "quantity must be positive" in reason
    assert reason.count(";") == 2


def test_empty_frame_is_returned_untouched() -> None:
    frame = pd.DataFrame(columns=["order_id", "source_row_number"])

    result = validate_orders(frame)

    assert result.valid.empty
    assert result.rejected.empty
    assert "reject_reason" in result.rejected.columns
