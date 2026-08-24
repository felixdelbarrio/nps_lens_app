from __future__ import annotations

import json
import platform
import resource
import sys
import threading
import time
from collections import Counter, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Deque

import numpy as np
from numpy.typing import NDArray

from nps_lens import PIPELINE_VERSION

TELEMETRY_SCHEMA_VERSION = "1.0"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _rss_mb() -> float:
    raw = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    divisor = 1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0
    return round(raw / divisor, 3)


@dataclass(frozen=True)
class TelemetryEvent:
    timestamp: str
    method: str
    route: str
    status: int
    duration_ms: float
    cpu_ms: float
    response_bytes: int
    rss_mb: float
    error_type: str = ""


class TelemetryCollector:
    """Bounded, thread-safe and PII-free application telemetry."""

    def __init__(self, max_events: int = 2000) -> None:
        self.started_at = _utc_now()
        self._events: Deque[TelemetryEvent] = deque(maxlen=max(int(max_events), 100))
        self._lock = threading.Lock()

    def record(
        self,
        *,
        method: str,
        route: str,
        status: int,
        duration_ms: float,
        cpu_ms: float,
        response_bytes: int,
        error_type: str = "",
    ) -> None:
        event = TelemetryEvent(
            timestamp=_utc_now(),
            method=str(method).upper(),
            route=str(route).split("?", 1)[0],
            status=int(status),
            duration_ms=round(max(float(duration_ms), 0.0), 3),
            cpu_ms=round(max(float(cpu_ms), 0.0), 3),
            response_bytes=max(int(response_bytes), 0),
            rss_mb=_rss_mb(),
            error_type=str(error_type),
        )
        with self._lock:
            self._events.append(event)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            events = list(self._events)
        durations = np.asarray([event.duration_ms for event in events], dtype=float)
        statuses = Counter(str(event.status) for event in events)
        routes: dict[str, list[float]] = {}
        for event in events:
            routes.setdefault(f"{event.method} {event.route}", []).append(event.duration_ms)

        def percentile(values: NDArray[np.float64], value: int) -> float:
            return round(float(np.percentile(values, value)), 3) if values.size else 0.0

        route_summary: list[dict[str, str | int | float]] = []
        for route, values in sorted(routes.items()):
            route_values = np.asarray(values, dtype=float)
            route_summary.append(
                {
                    "route": route,
                    "requests": int(route_values.size),
                    "p50_ms": percentile(route_values, 50),
                    "p95_ms": percentile(route_values, 95),
                    "max_ms": round(float(route_values.max()), 3),
                }
            )
        route_summary.sort(
            key=lambda item: (float(item["p95_ms"]), int(item["requests"])), reverse=True
        )
        return {
            "schema_version": TELEMETRY_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "app": {
                "pipeline_version": PIPELINE_VERSION,
                "python": platform.python_version(),
                "platform": platform.platform(),
                "started_at": self.started_at,
            },
            "privacy": {
                "query_parameters_recorded": False,
                "request_bodies_recorded": False,
                "response_bodies_recorded": False,
            },
            "summary": {
                "requests": len(events),
                "statuses": dict(sorted(statuses.items())),
                "p50_ms": percentile(durations, 50),
                "p95_ms": percentile(durations, 95),
                "p99_ms": percentile(durations, 99),
                "max_rss_mb": max((event.rss_mb for event in events), default=_rss_mb()),
                "response_bytes": sum(event.response_bytes for event in events),
            },
            "routes": route_summary,
            "events": [asdict(event) for event in events],
        }

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            self.snapshot(), ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")


class RequestTimer:
    def __enter__(self) -> "RequestTimer":
        usage = resource.getrusage(resource.RUSAGE_SELF)
        self.wall_start = time.perf_counter()
        self.cpu_start = float(usage.ru_utime + usage.ru_stime)
        return self

    def __exit__(self, *_args: object) -> None:
        self.duration_ms, self.cpu_ms = self.elapsed()

    def elapsed(self) -> tuple[float, float]:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return (
            (time.perf_counter() - self.wall_start) * 1000.0,
            (float(usage.ru_utime + usage.ru_stime) - self.cpu_start) * 1000.0,
        )
