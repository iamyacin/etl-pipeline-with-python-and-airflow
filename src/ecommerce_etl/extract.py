"""Read the raw Excel export into a permissive, string-friendly frame."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ecommerce_etl.exceptions import SchemaError

REQUIRED_COLUMNS = (
    "order_id",
    "customer_id",
    "order_date",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "status",
    "country",
)
OPTIONAL_COLUMNS = ("discount",)


def extract_orders(path: str | Path) -> pd.DataFrame:
    """Read an Excel file and return only the columns the pipeline understands.

    The export is intentionally read without type coercion: validation is a
    separate, testable stage. ``source_row_number`` keeps a link back to the
    spreadsheet row so rejected records can be traced to the source.
    """

    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Source file not found: {path}")

    frame = pd.read_excel(path, engine="openpyxl")
    frame.columns = [str(column).strip().lower() for column in frame.columns]

    duplicated = frame.columns[frame.columns.duplicated()].tolist()
    if duplicated:
        raise SchemaError(f"Duplicate columns after normalisation: {', '.join(duplicated)}")

    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise SchemaError(f"{path.name} is missing required columns: {', '.join(missing)}")

    for column in OPTIONAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = 0

    frame["source_row_number"] = frame.index + 2
    data_columns = [column for column in frame.columns if column != "source_row_number"]
    frame = frame.dropna(how="all", subset=data_columns).reset_index(drop=True)

    return frame[[*REQUIRED_COLUMNS, *OPTIONAL_COLUMNS, "source_row_number"]]
