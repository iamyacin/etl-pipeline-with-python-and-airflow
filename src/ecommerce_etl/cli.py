"""Command line entry point for local runs and warehouse checks."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg

from ecommerce_etl.config import Settings
from ecommerce_etl.exceptions import ETLException
from ecommerce_etl.pipeline import run_pipeline
from ecommerce_etl.warehouse import init_db, warehouse_summary


def _dsn(args: argparse.Namespace) -> str:
    return args.dsn or Settings.from_env().warehouse_dsn


def _run(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    state = run_pipeline(
        args.input,
        staging_dir=Path(args.staging) if args.staging else settings.staging_dir,
        dsn=None if args.dry_run else _dsn(args),
        max_reject_rate=settings.max_reject_rate,
        dry_run=args.dry_run,
    )

    summary = {
        "run_id": state["run_id"],
        "source_file": state["source_file"],
        "rows_read": state["rows_read"],
        "rows_valid": state["rows_valid"],
        "rows_rejected": state["rows_rejected"],
        "revenue": state.get("revenue_loaded", state.get("revenue", 0.0)),
    }
    if not args.dry_run:
        summary["rows_upserted"] = state.get("rows_upserted")
        summary["quality_status"] = state.get("quality_status")
        summary["reject_rate"] = state.get("reject_rate")

    print(json.dumps(summary, indent=2))
    if args.dry_run:
        print("dry run: warehouse was not modified", file=sys.stderr)
    return 0


def _init_db(args: argparse.Namespace) -> int:
    init_db(_dsn(args))
    print("warehouse schema is ready")
    return 0


def _summary(args: argparse.Namespace) -> int:
    print(json.dumps(warehouse_summary(_dsn(args)), indent=2, default=str))
    return 0


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN (defaults to WAREHOUSE_DSN or the local Docker port 5434)",
    )

    parser = argparse.ArgumentParser(
        prog="ecommerce-etl",
        description="Excel to PostgreSQL ELT pipeline with a data-quality quarantine.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", parents=[common], help="run the pipeline")
    run_parser.add_argument("--input", required=True, help="path to the Excel export")
    run_parser.add_argument("--staging", default=None, help="staging directory")
    run_parser.add_argument("--dry-run", action="store_true", help="stop before loading")
    run_parser.set_defaults(func=_run)

    init_parser = subparsers.add_parser(
        "init-db", parents=[common], help="create the warehouse tables"
    )
    init_parser.set_defaults(func=_init_db)

    summary_parser = subparsers.add_parser("summary", parents=[common], help="print warehouse KPIs")
    summary_parser.set_defaults(func=_summary)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (ETLException, FileNotFoundError, ValueError, psycopg.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
