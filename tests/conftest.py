"""Shared fixtures.

Integration tests use a real PostgreSQL database and are skipped when it is not
reachable. Start one with ``docker compose up -d postgres`` and set
``WAREHOUSE_TEST_DSN`` if you use different credentials.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_FILE = ROOT / "data" / "raw" / "orders_2026_10.xlsx"
TEST_DSN = os.getenv(
    "WAREHOUSE_TEST_DSN",
    "postgresql://etl:etl@localhost:5434/warehouse_test",
)


@pytest.fixture(scope="session")
def sample_file() -> Path:
    if not SAMPLE_FILE.is_file():
        pytest.skip(f"Sample export not found: {SAMPLE_FILE}")
    return SAMPLE_FILE


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    psycopg = pytest.importorskip("psycopg")
    try:
        with psycopg.connect(TEST_DSN, connect_timeout=3):
            pass
    except Exception as exc:  # pragma: no cover - depends on local environment
        pytest.skip(f"PostgreSQL is not available at {TEST_DSN}: {exc}")

    from ecommerce_etl.warehouse import init_db

    init_db(TEST_DSN)
    return TEST_DSN


@pytest.fixture
def clean_db(pg_dsn: str) -> str:
    import psycopg

    with psycopg.connect(pg_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                TRUNCATE TABLE
                    rejected_order_items,
                    fact_order_items,
                    dim_customers,
                    dim_products,
                    etl_run_log
                RESTART IDENTITY CASCADE
                """
            )
        connection.commit()
    return pg_dsn


@pytest.fixture
def order_row():
    """Factory for a valid raw order row."""

    def make(**overrides: Any) -> dict[str, Any]:
        row = {
            "order_id": "ORD-0001",
            "customer_id": "CUST-0001",
            "order_date": "2026-10-01",
            "product_id": "P-1001",
            "product_name": "Wireless Headphones",
            "category": "Electronics",
            "quantity": 2,
            "unit_price": 10.0,
            "discount": 0.0,
            "status": "delivered",
            "country": "US",
            "source_row_number": 2,
        }
        row.update(overrides)
        return row

    return make


@pytest.fixture
def order_frame(order_row) -> pd.DataFrame:
    return pd.DataFrame(
        [
            order_row(),
            order_row(
                order_id="ORD-0002",
                customer_id="CUST-0002",
                product_id="P-1002",
                product_name="Mechanical Keyboard",
                category="Electronics",
                quantity=1,
                unit_price=100.0,
                discount=0.10,
                status="shipped",
                country="DE",
                source_row_number=3,
            ),
        ]
    )
