from __future__ import annotations

import contextlib
import time
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional


@dataclass
class PerfEvent:
    name: str
    seconds: float


class PerfTracker:
    """Minimal timing collector (no heavy deps).

    Designed for Streamlit: create one per session_state and wrap expensive stages.
    """

    def __init__(self) -> None:
        self.events: List[PerfEvent] = []

    @contextlib.contextmanager
    def track(self, name: str) -> Iterator[None]:
        t0 = time.perf_counter()
        try:
            yield
        finally:
            dt = time.perf_counter() - t0
            self.events.append(PerfEvent(name=name, seconds=float(dt)))

    # Backward-compatible alias (older code used `timer`).
    def timer(self, name: str) -> contextlib.AbstractContextManager:
        return self.track(name)

    def summary(self) -> List[Dict[str, object]]:
        # last N only to keep UI light
        out: List[Dict[str, object]] = []
        for e in self.events[-80:]:
            out.append({"stage": e.name, "seconds": round(e.seconds, 4)})
        return out

    def totals(self) -> Dict[str, float]:
        acc: Dict[str, float] = {}
        for e in self.events:
            acc[e.name] = acc.get(e.name, 0.0) + float(e.seconds)
        return acc

    def reset(self) -> None:
        self.events = []

    def snapshot(self) -> Dict[str, object]:
        """Compact perf payload suitable for artifact manifests."""
        return {"events": self.summary(), "totals": self.totals()}
