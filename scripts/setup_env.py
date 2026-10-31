"""Create .env from the example and align AIRFLOW_UID with the host user."""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path


def main() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        shutil.copyfile(".env.example", env_path)

    text = env_path.read_text()
    uid = getattr(os, "getuid", lambda: 50000)()
    text = re.sub(r"^AIRFLOW_UID=.*$", f"AIRFLOW_UID={uid}", text, flags=re.MULTILINE)
    env_path.write_text(text)

    for directory in ("logs", "plugins", "data/staging"):
        Path(directory).mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    main()
