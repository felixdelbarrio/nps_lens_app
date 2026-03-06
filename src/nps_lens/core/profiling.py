from __future__ import annotations

import cProfile
import os
import pstats
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, cast


@dataclass
class ProfileSummary:
    path: Path
    top: List[Dict[str, object]]


def profiling_enabled() -> bool:
    v = os.getenv("NPS_LENS_PROFILE", "")
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def load_profile_summary(path: Path, *, top_n: int = 15) -> ProfileSummary:
    stats = pstats.Stats(str(path))
    stats.strip_dirs().sort_stats("cumulative")
    top: List[Dict[str, object]] = []

    # pstats.Stats has a runtime attribute `stats`, but type stubs may miss it.
    stats_any = cast(Any, stats)

    # stats.stats: (filename, line, func) -> (cc, nc, tt, ct, callers)
    for (fn, line, func), (_cc, nc, tt, ct, _callers) in stats_any.stats.items():
        top.append(
            {
                "func": f"{func} ({Path(fn).name}:{line})",
                "calls": int(nc),
                "cum_s": float(ct),
                "self_s": float(tt),
            }
        )

    def _cum_key(r: Dict[str, object]) -> float:
        return float(cast(float, r["cum_s"]))

    top = sorted(top, key=_cum_key, reverse=True)[: int(top_n)]
    return ProfileSummary(path=path, top=top)


@contextmanager
def profile_if_enabled(out_dir: Path, *, tag: str = "run") -> Iterator[List[ProfileSummary]]:
    """Optional cProfile wrapper.

    Usage:
        summaries: List[ProfileSummary] = []
        with profile_if_enabled(dir, tag="ui") as summaries:
            ... work ...
        if summaries: ... summaries[0]

    Enabled when env var NPS_LENS_PROFILE=1.
    """
    summaries: List[ProfileSummary] = []

    if not profiling_enabled():
        yield summaries
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    pr = cProfile.Profile()
    pr.enable()
    try:
        yield summaries
    finally:
        pr.disable()
        with tempfile.NamedTemporaryFile(
            dir=str(out_dir), delete=False, suffix=f"_{tag}.prof"
        ) as tf:
            tmp_path = Path(tf.name)
        pr.dump_stats(str(tmp_path))
        import contextlib

        with contextlib.suppress(Exception):
            summaries.append(load_profile_summary(tmp_path))
