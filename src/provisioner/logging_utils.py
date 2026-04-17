"""Console and JSONL file logging for the provisioner CLI runs."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path


class JsonFormatter(logging.Formatter):
    """Serialize log records as single-line JSON including optional ``record.extras``."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        """Build a JSON object with timestamp, level, message, logger, and merged extras."""
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        extras = getattr(record, "extras", None)
        if isinstance(extras, dict):
            payload.update(extras)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(*, logs_dir: Path) -> tuple[logging.Logger, Path]:
    """Configure the ``provisioner`` logger with console + timestamped JSONL file handlers."""
    logs_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("provisioner")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    log_path = logs_dir / f"run-{datetime.now().strftime('%Y%m%d-%H%M%S')}.jsonl"
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(JsonFormatter())

    logger.addHandler(console)
    logger.addHandler(file_handler)

    logger.info("Logging initialized", extra={"extras": {"log_file": str(log_path)}})
    return logger, log_path


def log_row_issue(
    logger: logging.Logger,
    *,
    level: int,
    file: str,
    row: int,
    email: str | None,
    project_name: str | None,
    reason: str,
) -> None:
    """Emit a structured log entry for a CSV row validation or import skip."""
    logger.log(
        level,
        reason,
        extra={
            "extras": {
                "file": file,
                "row": row,
                "email": email,
                "project_name": project_name,
                "reason": reason,
            }
        },
    )
