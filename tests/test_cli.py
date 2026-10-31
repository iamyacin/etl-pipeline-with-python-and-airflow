from __future__ import annotations

from pathlib import Path

from ecommerce_etl.cli import main


def test_cli_dry_run_prints_metrics(sample_file: Path, tmp_path, capsys) -> None:
    exit_code = main(
        [
            "run",
            "--input",
            str(sample_file),
            "--dry-run",
            "--staging",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert '"rows_rejected"' in captured.out
    assert "warehouse was not modified" in captured.err


def test_cli_reports_a_missing_file(tmp_path) -> None:
    exit_code = main(["run", "--input", str(tmp_path / "missing.xlsx"), "--dry-run"])

    assert exit_code == 1
