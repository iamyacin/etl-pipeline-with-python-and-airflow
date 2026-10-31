"""Runtime configuration, sourced from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_DSN = "postgresql://etl:etl@localhost:5434/warehouse"
ALLOWED_STATUSES = frozenset({"pending", "shipped", "delivered", "cancelled", "returned"})


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    """Configuration shared by the CLI and the Airflow tasks."""

    warehouse_dsn: str
    data_dir: Path
    staging_dir: Path
    max_reject_rate: float

    @classmethod
    def from_env(cls) -> Settings:
        data_dir = Path(os.getenv("DATA_DIR", "data"))
        return cls(
            warehouse_dsn=os.getenv("WAREHOUSE_DSN", DEFAULT_DSN),
            data_dir=data_dir,
            staging_dir=Path(os.getenv("STAGING_DIR", str(data_dir / "staging"))),
            max_reject_rate=_env_float("MAX_REJECT_RATE", 0.2),
        )
