"""Row-level validation with a quarantine instead of a hard failure.

Bad rows never stop the batch. Every rejected row keeps its original values and
one or more human-readable reasons, which are persisted for review.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ecommerce_etl.config import ALLOWED_STATUSES

TEXT_COLUMNS = (
    "order_id",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "status",
    "country",
)


@dataclass(frozen=True)
class ValidationResult:
    """Valid rows ready for transformation, plus rejected rows with reasons."""

    valid: pd.DataFrame
    rejected: pd.DataFrame

    @property
    def rows_read(self) -> int:
        return len(self.valid) + len(self.rejected)


def validate_orders(frame: pd.DataFrame) -> ValidationResult:
    """Validate and normalise an extracted order frame.

    Rules are deliberately simple and explicit so they are easy to audit and
    extend. A row is accepted only when every rule passes.
    """

    if frame.empty:
        return ValidationResult(
            valid=frame.copy(),
            rejected=frame.assign(reject_reason=pd.Series(dtype="string")),
        )

    work = frame.copy()
    for column in TEXT_COLUMNS:
        work[column] = work[column].astype("string").str.strip()
    work["status"] = work["status"].str.lower()
    work["country"] = work["country"].str.upper()

    order_dates = pd.to_datetime(work["order_date"], errors="coerce", format="mixed")
    quantities = pd.to_numeric(work["quantity"], errors="coerce")
    unit_prices = pd.to_numeric(work["unit_price"], errors="coerce")

    raw_discount = work["discount"].astype("string").str.strip()
    discounts = pd.to_numeric(raw_discount, errors="coerce")
    discount_is_blank = raw_discount.isna() | raw_discount.eq("")
    discounts = discounts.mask(discount_is_blank, 0.0)

    reasons: list[list[str]] = [[] for _ in range(len(work))]

    def flag(mask: pd.Series, message: str) -> None:
        for position, is_bad in enumerate(mask.fillna(False).tolist()):
            if is_bad:
                reasons[position].append(message)

    def missing(column: str) -> pd.Series:
        return work[column].isna() | work[column].str.strip().eq("")

    for column in TEXT_COLUMNS:
        flag(missing(column), f"missing {column}")

    flag(work["status"].notna() & ~work["status"].isin(ALLOWED_STATUSES), "invalid status")
    flag(
        work["country"].notna() & ~work["country"].str.fullmatch(r"[A-Za-z]{2}"),
        "country must be a 2-letter code",
    )

    flag(order_dates.isna(), "invalid order_date")
    flag(quantities.isna(), "quantity is not numeric")
    flag(quantities.notna() & (quantities <= 0), "quantity must be positive")
    flag(
        quantities.notna() & (quantities % 1 != 0),
        "quantity must be a whole number",
    )
    flag(unit_prices.isna(), "unit_price is not numeric")
    flag(unit_prices.notna() & (unit_prices < 0), "unit_price must not be negative")
    flag(discounts.isna() & ~discount_is_blank, "discount is not numeric")
    flag(
        discounts.notna() & ~discounts.between(0, 1),
        "discount must be between 0 and 1",
    )

    work["order_date"] = order_dates
    work["quantity"] = quantities
    work["unit_price"] = unit_prices
    work["discount"] = discounts.fillna(0.0)
    work["reject_reason"] = ["; ".join(row_reasons) for row_reasons in reasons]

    bad_mask = work["reject_reason"].ne("")
    valid = work.loc[~bad_mask].copy()
    rejected = frame.loc[work.index[bad_mask]].copy()
    rejected["reject_reason"] = work.loc[bad_mask, "reject_reason"].to_numpy()

    duplicate_mask = valid.duplicated(subset=["order_id", "product_id"], keep="first")
    if duplicate_mask.any():
        duplicate_index = valid.index[duplicate_mask]
        duplicates = frame.loc[duplicate_index].copy()
        duplicates["reject_reason"] = "duplicate order line (first occurrence kept)"
        rejected = pd.concat([rejected, duplicates], ignore_index=True)
        valid = valid.drop(index=duplicate_index)

    rejected = rejected.sort_values("source_row_number").reset_index(drop=True)
    valid = valid.drop(columns=["reject_reason"]).reset_index(drop=True)
    return ValidationResult(valid=valid, rejected=rejected)
