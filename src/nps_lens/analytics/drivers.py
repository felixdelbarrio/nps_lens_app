from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DriverStat:
    dimension: str
    value: str
    n: int
    nps: float
    detractor_rate: float
    promoter_rate: float
    gap_vs_overall: float

def compute_nps_from_scores(scores: pd.Series) -> float:
    s = pd.to_numeric(scores, errors="coerce").dropna()
    if s.empty:
        return float("nan")
    promoters = (s >= 9).mean()
    detractors = (s <= 6).mean()
    return float((promoters - detractors) * 100.0)


def driver_table(df: pd.DataFrame, dimension: str, score_col: str = "NPS") -> list[DriverStat]:
    if dimension not in df.columns:
        return []
    overall = compute_nps_from_scores(df[score_col])
    out: list[DriverStat] = []
    # Pandas groupby observed default is changing; be explicit and keep output small.
    for value, g in df.groupby(dimension, dropna=False, observed=True):
        n = int(len(g))
        nps = compute_nps_from_scores(g[score_col])
        s = pd.to_numeric(g[score_col], errors="coerce").dropna()
        detr = float((s <= 6).mean()) if not s.empty else float("nan")
        prom = float((s >= 9).mean()) if not s.empty else float("nan")
        out.append(
            DriverStat(
                dimension=dimension,
                value=str(value),
                n=n,
                nps=nps,
                detractor_rate=detr,
                promoter_rate=prom,
                gap_vs_overall=float(nps - overall) if not np.isnan(nps) else float("nan"),
            )
        )
    out.sort(key=lambda x: (np.nan_to_num(x.gap_vs_overall, nan=-1e9), x.n), reverse=True)
    return out
