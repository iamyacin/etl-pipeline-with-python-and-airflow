from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def test_dag_imports_and_has_expected_tasks() -> None:
    bag = DagBag(dag_folder=str(ROOT / "dags"))

    assert bag.import_errors == {}
    assert "ecommerce_orders_etl" in bag.dags

    dag = bag.dags["ecommerce_orders_etl"]
    assert set(dag.task_ids) == {"extract", "validate", "transform", "load", "quality"}
    assert dag.catchup is False
    assert dag.max_active_runs == 1
