from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass
class LogEvent:
    level: str
    message: str
    timestamp: str
    name: str
    extra: dict[str, Any]


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        extra: dict[str, Any] = {}
        for key, value in record.__dict__.items():
            if key.startswith("_"):
                continue
            if key in {
                "msg",
                "args",
                "levelname",
                "levelno",
                "name",
                "created",
                "msecs",
                "relativeCreated",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "thread",
                "threadName",
                "processName",
                "process",
            }:
                continue
            extra[key] = value

        evt = LogEvent(
            level=record.levelname,
            message=record.getMessage(),
            timestamp=datetime.utcnow().isoformat() + "Z",
            name=record.name,
            extra=extra,
        )
        return json.dumps(asdict(evt), ensure_ascii=False)


def setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.handlers = [handler]
