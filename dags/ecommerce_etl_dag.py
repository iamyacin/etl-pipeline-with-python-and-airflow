"""Daily ELT for the e-commerce Excel exports.

Pipeline stages:
1. Extract the spreadsheet into a per-run staging directory.
2. Validate rows and quarantine bad ones with reasons.
3. Transform valid rows into a star schema.
4. Upsert dimensions and facts into PostgreSQL.
5. Run SQL quality gates and record run metrics.

The source file can be changed with the ``SOURCE_FILE`` environment variable
(relative to ``DATA_DIR``). Runs are idempotent: the run id is derived from the
file content, and every warehouse write is an upsert.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from ecommerce_etl.config import Settings
from ecommerce_etl.pipeline import (
    extract_stage,
    load_stage,
    quality_stage,
    transform_stage,
    validate_stage,
)

SOURCE_FILE = os.getenv("SOURCE_FILE", "raw/orders_2026_10.xlsx")


@dag(
    dag_id="ecommerce_orders_etl",
    description="Excel orders -> validated star schema in PostgreSQL.",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["etl", "ecommerce", "postgres", "data-quality"],
    doc_md=__doc__,
)
def ecommerce_orders_etl():
    @task
    def extract() -> dict:
        settings = Settings.from_env()
        return extract_stage(settings.data_dir / SOURCE_FILE, settings.staging_dir)

    @task
    def validate(state: dict) -> dict:
        return validate_stage(state)

    @task
    def transform(state: dict) -> dict:
        return transform_stage(state)

    @task
    def load(state: dict) -> dict:
        settings = Settings.from_env()
        return load_stage(state, settings.warehouse_dsn)

    @task(retries=0)
    def quality(state: dict) -> dict:
        settings = Settings.from_env()
        return quality_stage(state, settings.warehouse_dsn, settings.max_reject_rate)

    quality(load(transform(validate(extract()))))


ecommerce_orders_etl()
