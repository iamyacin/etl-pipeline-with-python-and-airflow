# E-commerce ELT Pipeline with Python, Airflow & PostgreSQL

[![CI](https://github.com/iamyacin/etl-pipeline-with-python-and-airflow/actions/workflows/ci.yml/badge.svg)](https://github.com/iamyacin/etl-pipeline-with-python-and-airflow/actions/workflows/ci.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)
![Airflow 3.3.2](https://img.shields.io/badge/Airflow-3.3.2-017CEE?logo=apache-airflow&logoColor=white)
![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Ruff](https://img.shields.io/badge/lint-ruff-261230)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

A small, production-minded ELT pipeline that turns a messy Excel order export into a
queryable star schema in PostgreSQL. Bad rows are **quarantined with reasons**, every load
is **idempotent**, and a post-load **quality gate** fails the Airflow task if the batch
doesn't add up.

The repo ships with a deterministic synthetic dataset and a one-command Docker stack, so
you can clone it, run it, and see the Airflow DAG turn green without hunting for data.

## What it does

1. **Extract** — reads an `.xlsx` export with pandas/openpyxl into a per-run staging area.
2. **Validate** — normalises text and dates, enforces row-level rules, and writes rejected
   rows to a quarantine table with human-readable reasons.
3. **Transform** — builds a star schema (`dim_customers`, `dim_products`,
   `fact_order_items`) and derives revenue, discount, and month columns.
4. **Load** — upserts dimensions first, then facts, keyed on `(order_id, product_id)`.
5. **Check** — runs SQL assertions (counts, duplicates, orphans, rejects) and records
   metrics in `etl_run_log`.

Orchestration is an Airflow 3 DAG with retries, `catchup=False`, and one active run at a
time. The pipeline can also run from the CLI, which is handy for local development.

## Architecture

```mermaid
flowchart LR
    A["Excel exports<br/>data/raw/*.xlsx"] --> B[Extract]
    B --> C{Validate}
    C -- "valid rows" --> D[Transform<br/>star schema]
    C -- "bad rows" --> Q[("rejected_order_items<br/>+ reject_reason")]
    D --> E[Load<br/>idempotent upserts]
    E --> W[("PostgreSQL<br/>warehouse")]
    E --> F{Quality gate}
    F -- "pass" --> L[("etl_run_log")]
    F -- "fail" --> X[Fail the Airflow task]
```

Under the hood:

- **Airflow 3.3.2** (LocalExecutor) runs the five tasks; `dags/ecommerce_etl_dag.py` is thin
  and delegates to the importable `ecommerce_etl` package.
- **PostgreSQL 16** hosts both the Airflow metadata database and the `warehouse` database.
- **Staging CSVs** carry data between tasks. XCom only carries paths and counts, so retries
  are cheap and the metadata database never stores DataFrames.
- The Airflow image installs the package, while `src/` is bind-mounted for live editing.

## Data model

| Table | Grain / key | Purpose |
|---|---|---|
| `fact_order_items` | One row per `(order_id, product_id)` | Quantity, prices, discount, line total, status, country |
| `dim_customers` | `customer_id` | Country plus first order date |
| `dim_products` | `product_id` | Name, category, latest unit price |
| `rejected_order_items` | One row per `(run_id, source_row_number)` | Original row as JSONB + `reject_reason` |
| `etl_run_log` | `run_id` | Rows read/valid/rejected/upserted, revenue, status, timings |

The run ID is derived from the file content hash (`<file>-v1-<sha256[:12]>`), which makes
retries and re-runs of the same file safe. Facts are upserted, rejects are
`ON CONFLICT DO NOTHING`, and the run log is upserted as well.

## Data quality rules

A row is loaded only when **all** of these pass:

| Rule | Quarantine reason |
|---|---|
| Required text fields are present | `missing order_id`, `missing customer_id`, … |
| `order_date` is parseable | `invalid order_date` |
| `quantity` is a positive whole number | `quantity must be positive`, `quantity must be a whole number` |
| `unit_price` is numeric and non-negative | `unit_price is not numeric`, `unit_price must not be negative` |
| `discount` is between 0 and 1 | `discount must be between 0 and 1` |
| `status` is an allowed value | `invalid status` |
| `country` is a two-letter code | `country must be a 2-letter code` |
| `(order_id, product_id)` is unique in the batch | `duplicate order line (first occurrence kept)` |

The batch also fails the quality gate if more than 20% of rows are rejected
(`MAX_REJECT_RATE`), if the fact count doesn't match the number of valid rows, or if
duplicate keys, negative amounts, or orphan dimensions are found after the load.

## Quickstart (Docker)

Requirements: Docker with Compose v2, and about 4 GB of memory available to Docker.

```bash
git clone git@github.com:iamyacin/etl-pipeline-with-python-and-airflow.git
cd etl-pipeline-with-python-and-airflow
make up
```

`make up` creates `.env`, sets `AIRFLOW_UID` to your host user, builds the Airflow image,
and starts PostgreSQL + Airflow. The first build takes a few minutes.

| Service | URL / port | Credentials |
|---|---|---|
| Airflow UI | <http://localhost:8080> | `airflow` / `airflow` |
| Warehouse | `localhost:5434` → container `postgres:5432` | `etl` / `etl` |
| Airflow metadata | internal | `airflow` / `airflow` |

The DAG is created unpaused and scheduled daily. Trigger it immediately with:

```bash
make trigger
# or the Airflow UI: ecommerce_orders_etl -> Trigger DAG
```

Inspect the result:

```bash
docker compose exec postgres psql -U etl -d warehouse \
  -c "select count(*) as lines, round(sum(line_total), 2) as revenue from fact_order_items;"

docker compose exec postgres psql -U etl -d warehouse \
  -c "select reject_reason, count(*) from rejected_order_items group by 1 order by 2 desc;"
```

Stop the stack with `make down` (keeps data) or `make clean` (deletes the database volume).

## Run it locally (CLI)

Start only the database, then use the CLI. `uv` reads `.python-version` and uses Python 3.12.

```bash
make env
docker compose up -d postgres
uv sync

uv run python -m ecommerce_etl.cli run \
  --input data/raw/orders_2026_10.xlsx \
  --dsn postgresql://etl:etl@localhost:5434/warehouse

uv run python -m ecommerce_etl.cli summary \
  --dsn postgresql://etl:etl@localhost:5434/warehouse
```

Or use the shortcuts:

```bash
make run-local
make summary
```

A dry run validates and transforms without writing to PostgreSQL:

```bash
uv run python -m ecommerce_etl.cli run \
  --input data/raw/orders_2026_10.xlsx --dry-run
```

## Example output

The committed October sample has 408 rows: 385 valid and 23 deliberately bad or duplicate
rows (5.64% reject rate).

```json
{
  "run_id": "orders_2026_10-v1-dc1fcee6e9ed",
  "source_file": "orders_2026_10.xlsx",
  "rows_read": 408,
  "rows_valid": 385,
  "rows_rejected": 23,
  "revenue": 70175.88,
  "rows_upserted": 385,
  "quality_status": "success",
  "reject_rate": 0.0564
}
```

```json
{
  "totals": {
    "order_lines": 385,
    "orders": 260,
    "customers": 77,
    "gross_revenue": 70175.88
  },
  "by_status": [
    {"status": "delivered", "lines": 200, "revenue": 37144.78},
    {"status": "shipped", "lines": 82, "revenue": 14563.73},
    {"status": "pending", "lines": 52, "revenue": 9862.97},
    {"status": "cancelled", "lines": 38, "revenue": 6169.65},
    {"status": "returned", "lines": 13, "revenue": 2434.75}
  ],
  "rejected_rows": 23,
  "latest_run": {"status": "success", "rows_valid": 385, "rows_rejected": 23}
}
```

Rejected rows are fully traceable:

```sql
SELECT source_row_number, reject_reason, raw_row
FROM rejected_order_items
ORDER BY source_row_number
LIMIT 3;
```

## Analytics

`sql/analytics.sql` contains ready-to-run queries. For example, monthly net revenue
(excluding cancelled and returned orders):

```sql
SELECT
    order_month,
    round(sum(line_total) FILTER (WHERE status NOT IN ('cancelled', 'returned')), 2)
        AS net_revenue
FROM fact_order_items
GROUP BY order_month
ORDER BY order_month;
```

More in the file: average order value, top categories, customer lifetime value, and a
rejection-reason breakdown.

## Tests and CI

```bash
uv run ruff check .
uv run pytest
```

- Unit tests cover extraction, validation rules, transformations, and the CLI.
- Integration tests run against a real PostgreSQL and verify that loading the same file
  twice does not duplicate rows, that upserts update existing facts, and that the quality
  gate fails when it should.
- Integration tests are skipped automatically when the database isn't reachable.
- GitHub Actions runs three jobs: lint + tests with a PostgreSQL service, DAG import
  validation with Airflow installed, and a Docker image build.

The DAG test loads `dags/` with `DagBag` and asserts that there are no import errors and
that all five tasks exist.

## Project layout

```text
.
├── dags/
│   └── ecommerce_etl_dag.py       # Airflow 3 TaskFlow DAG
├── src/ecommerce_etl/
│   ├── cli.py                     # run / init-db / summary
│   ├── config.py                  # environment-backed settings
│   ├── extract.py                 # Excel reader + schema check
│   ├── validation.py              # row rules + quarantine
│   ├── transform.py               # star-schema construction
│   ├── warehouse.py               # upserts, schema, quality gates
│   ├── pipeline.py                # staged functions shared by CLI/DAG
│   └── sql/schema.sql             # idempotent warehouse DDL
├── scripts/generate_sample_data.py
├── docker/                        # Airflow image + database init
├── data/raw/                      # committed synthetic Excel samples
├── sql/analytics.sql
├── tests/                         # unit + integration
├── docker-compose.yml
└── Makefile
```

## Design decisions

**Quarantine instead of fail-fast.** One malformed cell shouldn't block a daily batch.
Bad rows are preserved verbatim in `rejected_order_items`, which makes the failure visible
and debuggable without losing the rest of the load. A global reject-rate threshold still
fails the run when data quality collapses.

**Content-hash run IDs + upserts.** Retrying a failed task or re-running the same file
produces the same run ID. Facts and rejects are written with `ON CONFLICT`, so the
warehouse converges to the same state. `ingested_at` is the only value that changes.

**Small XComs, staged files.** Airflow's metadata database is not a data warehouse.
Tasks exchange paths and counts, while the actual rows live in CSVs under
`data/staging/<run_id>/`. This keeps task retries cheap and keeps the DAG serialisable.

**Quality checks in SQL after the load.** Assertions run against the warehouse itself
(row counts, duplicate keys, orphan dimensions), not just the in-memory frames. The result
is written to `etl_run_log` before the task fails.

**Deterministic sample data.** `scripts/generate_sample_data.py` seeds `random`, injects
11 known bad rows and 12 exact duplicates per file, and styles the Excel output. The
rejected counts in the README are reproducible.

## Limitations

This is a portfolio project, not a production deployment:

- Batch, single-file ingestion. File discovery and incremental manifests are out of scope.
- LocalExecutor on one machine; no Celery/Kubernetes workers.
- Dev credentials live in `.env.example` and the committed `docker-compose.yml`. Use a
  secret manager and TLS for anything real.
- No dbt, cloud warehouse, dashboard, or alerting integration yet.
- The reject-rate gate is global; per-rule thresholds would be a natural next step.

## Next steps

- Land files in S3/GCS and trigger with an Airflow sensor or asset.
- Move transformations into dbt and port the quality checks to dbt tests.
- Add a Metabase/Grafana dashboard over the star schema.
- Add per-source ingest manifests and a backfill CLI.
- Deploy on Kubernetes with secrets from a vault.

## License

MIT — see [LICENSE](LICENSE).
