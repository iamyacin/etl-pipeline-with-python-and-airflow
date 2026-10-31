"""PostgreSQL warehouse access: schema, idempotent upserts, quality gates."""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import date, datetime
from importlib import resources
from typing import Any

import pandas as pd
import psycopg
from psycopg.types.json import Jsonb

from ecommerce_etl.exceptions import DataQualityError
from ecommerce_etl.transform import FACT_COLUMNS


def _schema_statements() -> list[str]:
    sql = resources.files("ecommerce_etl").joinpath("sql/schema.sql").read_text(encoding="utf-8")
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def apply_schema(connection: psycopg.Connection[Any]) -> None:
    """Create tables and indexes if they do not exist yet."""

    with connection.cursor() as cursor:
        for statement in _schema_statements():
            cursor.execute(statement)


def init_db(dsn: str) -> None:
    """Apply the warehouse schema to an existing database."""

    with psycopg.connect(dsn, connect_timeout=10) as connection:
        apply_schema(connection)
        connection.commit()


def _clean(value: Any) -> Any:
    """Convert pandas/NumPy scalars to values psycopg can adapt."""

    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _rows(frame: pd.DataFrame, columns: Iterable[str]) -> list[tuple[Any, ...]]:
    return [
        tuple(_clean(value) for value in row)
        for row in frame.loc[:, columns].itertuples(index=False, name=None)
    ]


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return value
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    return str(value)


def _rejected_rows(frame: pd.DataFrame, *, run_id: str, source_file: str) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for record in frame.to_dict(orient="records"):
        reason = str(record.pop("reject_reason"))
        row_number = int(record.pop("source_row_number"))
        raw_row = {key: _json_safe(value) for key, value in record.items()}
        rows.append((run_id, source_file, row_number, Jsonb(raw_row), reason))
    return rows


RUN_LOG_START = """
INSERT INTO etl_run_log (
    run_id, source_file, started_at, status, rows_read, rows_valid, rows_rejected
)
VALUES (%s, %s, %s, 'running', %s, %s, %s)
ON CONFLICT (run_id) DO UPDATE SET
    source_file = EXCLUDED.source_file,
    started_at = EXCLUDED.started_at,
    finished_at = NULL,
    status = 'running',
    rows_read = EXCLUDED.rows_read,
    rows_valid = EXCLUDED.rows_valid,
    rows_rejected = EXCLUDED.rows_rejected,
    rows_upserted = 0,
    revenue_loaded = 0
"""

CUSTOMER_UPSERT = """
INSERT INTO dim_customers (customer_id, country, first_order_date)
VALUES (%s, %s, %s)
ON CONFLICT (customer_id) DO UPDATE SET
    country = EXCLUDED.country,
    first_order_date = LEAST(dim_customers.first_order_date, EXCLUDED.first_order_date),
    updated_at = now()
"""

PRODUCT_UPSERT = """
INSERT INTO dim_products (product_id, product_name, category, last_unit_price)
VALUES (%s, %s, %s, %s)
ON CONFLICT (product_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    last_unit_price = EXCLUDED.last_unit_price,
    updated_at = now()
"""

FACT_UPSERT = """
INSERT INTO fact_order_items (
    order_id, customer_id, product_id, order_date, order_month, quantity,
    unit_price, discount, line_total, status, country, source_file, ingest_run_id
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (order_id, product_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    order_date = EXCLUDED.order_date,
    order_month = EXCLUDED.order_month,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    discount = EXCLUDED.discount,
    line_total = EXCLUDED.line_total,
    status = EXCLUDED.status,
    country = EXCLUDED.country,
    source_file = EXCLUDED.source_file,
    ingest_run_id = EXCLUDED.ingest_run_id,
    ingested_at = now()
"""

REJECTED_UPSERT = """
INSERT INTO rejected_order_items (
    ingest_run_id, source_file, source_row_number, raw_row, reject_reason
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (ingest_run_id, source_row_number) DO NOTHING
"""


def load_batch(
    *,
    dsn: str,
    run_id: str,
    source_file: str,
    started_at: str,
    rows_read: int,
    rows_valid: int,
    rows_rejected: int,
    fact: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    rejected: pd.DataFrame,
    revenue: float,
) -> dict[str, Any]:
    """Upsert dimensions first, then facts, then the quarantine rows."""

    started = datetime.fromisoformat(started_at)

    with psycopg.connect(dsn, connect_timeout=10) as connection:
        apply_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                RUN_LOG_START,
                (run_id, source_file, started, rows_read, rows_valid, rows_rejected),
            )
            cursor.executemany(
                CUSTOMER_UPSERT,
                _rows(customers, ["customer_id", "country", "first_order_date"]),
            )
            cursor.executemany(
                PRODUCT_UPSERT,
                _rows(products, ["product_id", "product_name", "category", "last_unit_price"]),
            )
            cursor.executemany(FACT_UPSERT, _rows(fact, FACT_COLUMNS))
            cursor.executemany(
                REJECTED_UPSERT,
                _rejected_rows(rejected, run_id=run_id, source_file=source_file),
            )
            cursor.execute(
                """
                UPDATE etl_run_log
                SET status = 'loaded', rows_upserted = %s, revenue_loaded = %s, finished_at = now()
                WHERE run_id = %s
                """,
                (len(fact), revenue, run_id),
            )
        connection.commit()

    return {
        "rows_upserted": len(fact),
        "revenue_loaded": round(float(revenue), 2),
        "customers_upserted": len(customers),
        "products_upserted": len(products),
        "rejected_stored": len(rejected),
    }


def run_quality_checks(
    *,
    dsn: str,
    run_id: str,
    rows_valid: int,
    rows_rejected: int,
    max_reject_rate: float,
) -> dict[str, Any]:
    """Run SQL assertions after the load and record the outcome."""

    total_rows = rows_valid + rows_rejected
    reject_rate = 1.0 if total_rows == 0 else rows_rejected / total_rows

    with psycopg.connect(dsn, connect_timeout=10) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM fact_order_items WHERE ingest_run_id = %s", (run_id,)
            )
            fact_rows = int(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT count(*) FROM (
                    SELECT order_id, product_id
                    FROM fact_order_items
                    GROUP BY order_id, product_id
                    HAVING count(*) > 1
                ) AS duplicates
                """
            )
            duplicate_keys = int(cursor.fetchone()[0])

            cursor.execute(
                "SELECT count(*) FROM fact_order_items WHERE line_total < 0 OR quantity <= 0"
            )
            invalid_amounts = int(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT count(*)
                FROM fact_order_items f
                LEFT JOIN dim_customers c ON c.customer_id = f.customer_id
                LEFT JOIN dim_products p ON p.product_id = f.product_id
                WHERE c.customer_id IS NULL OR p.product_id IS NULL
                """
            )
            orphan_dimensions = int(cursor.fetchone()[0])

            cursor.execute(
                "SELECT count(*) FROM rejected_order_items WHERE ingest_run_id = %s", (run_id,)
            )
            rejected_stored = int(cursor.fetchone()[0])

            checks = {
                "batch_not_empty": rows_valid > 0,
                "fact_rows_match_valid_rows": fact_rows == rows_valid,
                "no_duplicate_keys": duplicate_keys == 0,
                "no_invalid_amounts": invalid_amounts == 0,
                "no_orphan_dimensions": orphan_dimensions == 0,
                "rejected_rows_match": rejected_stored == rows_rejected,
                "reject_rate_within_threshold": reject_rate <= max_reject_rate,
            }
            failed = [name for name, passed in checks.items() if not passed]
            status = "success" if not failed else "failed"

            cursor.execute(
                "UPDATE etl_run_log SET status = %s, finished_at = now() WHERE run_id = %s",
                (status, run_id),
            )
        connection.commit()

    result = {
        "quality_status": status,
        "quality_checks": checks,
        "reject_rate": round(reject_rate, 4),
        "fact_rows": fact_rows,
    }
    if failed:
        raise DataQualityError(
            f"Quality gate failed for run {run_id}: {', '.join(failed)} "
            f"(reject rate {reject_rate:.1%}, limit {max_reject_rate:.1%})"
        )
    return result


def warehouse_summary(dsn: str) -> dict[str, Any]:
    """Small KPI snapshot used by the CLI and the README."""

    with psycopg.connect(dsn, connect_timeout=10) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
                SELECT
                    count(*) AS order_lines,
                    count(DISTINCT order_id) AS orders,
                    count(DISTINCT customer_id) AS customers,
                    coalesce(round(sum(line_total), 2), 0)::float8 AS gross_revenue
                FROM fact_order_items
                """
        )
        row = cursor.fetchone()
        totals = {
            "order_lines": int(row[0]),
            "orders": int(row[1]),
            "customers": int(row[2]),
            "gross_revenue": float(row[3]),
        }

        cursor.execute(
            """
                SELECT status, count(*) AS lines, round(sum(line_total), 2)::float8 AS revenue
                FROM fact_order_items
                GROUP BY status
                ORDER BY lines DESC
                """
        )
        by_status = [
            {"status": status, "lines": int(lines), "revenue": float(revenue)}
            for status, lines, revenue in cursor.fetchall()
        ]

        cursor.execute("SELECT count(*) FROM rejected_order_items")
        rejected = int(cursor.fetchone()[0])

        cursor.execute(
            """
                SELECT run_id, source_file, status, rows_read, rows_valid, rows_rejected,
                       rows_upserted, revenue_loaded::float8, started_at, finished_at
                FROM etl_run_log
                ORDER BY started_at DESC
                LIMIT 1
                """
        )
        latest = cursor.fetchone()
        latest_run = None
        if latest:
            columns = [description.name for description in cursor.description]
            latest_run = dict(zip(columns, latest, strict=True))
            latest_run["started_at"] = latest_run["started_at"].isoformat()
            if latest_run["finished_at"]:
                latest_run["finished_at"] = latest_run["finished_at"].isoformat()

    return {
        "totals": totals,
        "by_status": by_status,
        "rejected_rows": rejected,
        "latest_run": latest_run,
    }
