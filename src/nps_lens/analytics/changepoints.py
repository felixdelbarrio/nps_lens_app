from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Optional

import pandas as pd
import ruptures as rpt

from nps_lens.analytics.drivers import compute_nps_from_scores


@dataclass(frozen=True)
class ChangePoint:
    dimension: str
    value: str
    points: list[date]
    method: str
    note: str


@dataclass(frozen=True)
class ChangePointSignificance:
    """Changepoints with an empirical stability estimate.

    stability is in [0,1] and approximates the probability that a changepoint is
    recovered under resampling.
    """

    dimension: str
    value: str
    points: List[date]
    stability: List[float]
    level: List[str]
    method: str
    note: str


def detect_nps_changepoints(
    df: pd.DataFrame,
    date_col: str = "Fecha",
    dim_col: str = "Palanca",
    value: Optional[str] = None,
    freq: str = "D",
    model: str = "l2",
    pen: float = 8.0,
) -> Optional[ChangePoint]:
    if date_col not in df.columns or dim_col not in df.columns or "NPS" not in df.columns:
        return None

    data = df.copy()
    data[date_col] = pd.to_datetime(data[date_col], errors="coerce")
    data = data.dropna(subset=[date_col])
    if value is not None:
        data = data.loc[data[dim_col] == value]
    if data.empty:
        return None

    ts = (
        data.set_index(date_col)
        .resample(freq)["NPS"]
        .apply(compute_nps_from_scores)
        .dropna()
        .astype(float)
    )
    if len(ts) < 10:
        return None

    algo = rpt.Pelt(model=model).fit(ts.values.reshape(-1, 1))
    bkps = algo.predict(pen=pen)
    # bkps include last index
    pts: list[date] = []
    for idx in bkps[:-1]:
        pts.append(ts.index[idx - 1].date())
    return ChangePoint(
        dimension=dim_col,
        value=str(value) if value is not None else "*",
        points=pts,
        method=f"Pelt({model})",
        note=f"freq={freq} pen={pen}",
    )


def detect_nps_changepoints_with_bootstrap(
    df: pd.DataFrame,
    date_col: str = "Fecha",
    dim_col: str = "Palanca",
    value: Optional[str] = None,
    freq: str = "D",
    model: str = "l2",
    pen: float = 8.0,
    min_points: int = 12,
    n_boot: int = 200,
    block_size: int = 5,
    tol_periods: int = 1,
    random_state: int = 7,
) -> Optional[ChangePointSignificance]:
    """Detect changepoints and estimate their stability via moving-block bootstrap.

    Labels:
      - High: stability >= 0.70
      - Medium: stability >= 0.40
      - Low: otherwise
    """
    if date_col not in df.columns or dim_col not in df.columns or "NPS" not in df.columns:
        return None

    data = df.copy()
    data[date_col] = pd.to_datetime(data[date_col], errors="coerce")
    data = data.dropna(subset=[date_col])
    if value is not None:
        data = data.loc[data[dim_col] == value]
    if data.empty:
        return None

    ts = (
        data.set_index(date_col)
        .resample(freq)["NPS"]
        .apply(compute_nps_from_scores)
        .dropna()
        .astype(float)
    )
    if len(ts) < int(min_points):
        return None

    algo = rpt.Pelt(model=model).fit(ts.values.reshape(-1, 1))
    bkps = algo.predict(pen=pen)
    cp_pos = [int(i) for i in bkps[:-1] if int(i) > 0]
    if not cp_pos:
        return ChangePointSignificance(
            dimension=dim_col,
            value=str(value) if value is not None else "*",
            points=[],
            stability=[],
            level=[],
            method=f"Pelt({model})+MBB",
            note=f"freq={freq} pen={pen} n_boot={n_boot} block={block_size}",
        )

    pts = [ts.index[i - 1].date() for i in cp_pos]

    # Bootstrap stability (moving-block bootstrap)
    import numpy as np  # local import

    rng = np.random.RandomState(int(random_state))
    n = len(ts)
    b = max(1, int(block_size))
    counts = [0 for _ in cp_pos]

    starts = list(range(0, n, b))
    n_blocks = len(starts)
    for _ in range(int(n_boot)):
        sel = rng.randint(0, n_blocks, size=n_blocks)
        idx: List[int] = []
        for s_i in sel:
            s = starts[s_i]
            idx.extend(list(range(s, min(n, s + b))))
        if len(idx) < min_points:
            continue
        sample = ts.values[idx]
        try:
            algo_b = rpt.Pelt(model=model).fit(sample.reshape(-1, 1))
            bkps_b = algo_b.predict(pen=float(pen))
            cp_pos_b = [int(i) for i in bkps_b[:-1] if int(i) > 0]
        except Exception:
            continue

        for j, p in enumerate(cp_pos):
            for pb in cp_pos_b:
                if abs(int(pb) - int(p)) <= int(tol_periods):
                    counts[j] += 1
                    break

    stabs = [float(c) / float(n_boot) for c in counts]

    def _label(s: float) -> str:
        if s >= 0.70:
            return "High"
        if s >= 0.40:
            return "Medium"
        return "Low"

    levels = [_label(s) for s in stabs]

    return ChangePointSignificance(
        dimension=dim_col,
        value=str(value) if value is not None else "*",
        points=pts,
        stability=stabs,
        level=levels,
        method=f"Pelt({model})+MBB",
        note=f"freq={freq} pen={pen} n_boot={n_boot} block={block_size}",
    )
