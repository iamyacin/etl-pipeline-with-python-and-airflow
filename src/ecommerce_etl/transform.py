"""Turn validated rows into a small star schema."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

FACT_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "order_month",
    "quantity",
    "unit_price",
    "discount",
    "line_total",
    "status",
    "country",
    "source_file",
    "ingest_run_id",
]

CUSTOMER_COLUMNS = ["customer_id", "country", "first_order_date"]
PRODUCT_COLUMNS = ["product_id", "product_name", "category", "last_unit_price"]


@dataclass(frozen=True)
class TransformResult:
    """Fact and dimension frames produced from one batch of valid rows."""

    fact: pd.DataFrame
    customers: pd.DataFrame
    products: pd.DataFrame

    @property
    def revenue(self) -> float:
        if self.fact.empty:
            return 0.0
        return round(float(self.fact["line_total"].sum()), 2)


def transform_orders(
    valid: pd.DataFrame,
    *,
    source_file: str,
    run_id: str,
) -> TransformResult:
    """Derive money columns and dimension snapshots.

    ``fact_order_items`` is keyed by ``(order_id, product_id)``, so the load can
    be retried safely with an upsert.
    """

    if valid.empty:
        return TransformResult(
            fact=pd.DataFrame(columns=FACT_COLUMNS),
            customers=pd.DataFrame(columns=CUSTOMER_COLUMNS),
            products=pd.DataFrame(columns=PRODUCT_COLUMNS),
        )

    frame = valid.copy()
    frame["quantity"] = frame["quantity"].astype(int)
    frame["unit_price"] = frame["unit_price"].astype(float).round(2)
    frame["discount"] = frame["discount"].astype(float).round(4)
    frame["line_total"] = (frame["quantity"] * frame["unit_price"] * (1 - frame["discount"])).round(
        2
    )

    order_dates = pd.to_datetime(frame["order_date"], errors="raise")
    frame["order_date"] = order_dates.dt.date
    frame["order_month"] = pd.to_datetime(order_dates.dt.strftime("%Y-%m-01")).dt.date
    frame["source_file"] = source_file
    frame["ingest_run_id"] = run_id

    fact = frame.loc[:, FACT_COLUMNS].reset_index(drop=True)

    customers = (
        frame.sort_values("order_date")
        .groupby("customer_id", as_index=False)
        .agg(
            country=("country", "last"),
            first_order_date=("order_date", "first"),
        )
        .loc[:, CUSTOMER_COLUMNS]
    )
    products = (
        frame.sort_values("order_date")
        .drop_duplicates(subset=["product_id"], keep="last")
        .loc[:, ["product_id", "product_name", "category", "unit_price"]]
        .rename(columns={"unit_price": "last_unit_price"})
        .reset_index(drop=True)
    )

    return TransformResult(fact=fact, customers=customers, products=products)
