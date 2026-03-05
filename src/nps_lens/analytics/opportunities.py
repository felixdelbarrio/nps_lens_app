from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd

from nps_lens.analytics.drivers import compute_nps_from_scores


@dataclass(frozen=True)
class Opportunity:
    dimension: str
    value: str
    n: int
    current_nps: float
    potential_uplift: float
    confidence: float
    why: str


def rank_opportunities(
    df: pd.DataFrame,
    dimensions: Sequence[str],
    score_col: str = "NPS",
    min_n: int = 200,
) -> list[Opportunity]:
    overall = compute_nps_from_scores(df[score_col])
    out: list[Opportunity] = []
    for dim in dimensions:
        if dim not in df.columns:
            continue
        # Pandas groupby observed default is changing; be explicit and keep output small.
        for val, g in df.groupby(dim, dropna=False, observed=True):
            n = int(len(g))
            if n < min_n:
                continue
            nps = compute_nps_from_scores(g[score_col])
            delta = overall - nps  # how far below overall
            if np.isnan(delta) or delta <= 0:
                continue
            # naive uplift: close 60% of the gap
            uplift = float(delta * 0.6)
            # confidence proxy: more data -> higher confidence (cap at 1)
            conf = float(min(1.0, np.log10(max(n, 10)) / 5.0))
            out.append(
                Opportunity(
                    dimension=dim,
                    value=str(val),
                    n=n,
                    current_nps=float(nps),
                    potential_uplift=uplift,
                    confidence=conf,
                    why=f"'{dim}={val}' está {delta:.1f} pts por debajo del NPS global",
                )
            )
    out.sort(key=lambda o: (o.potential_uplift * o.confidence, o.n), reverse=True)
    return out
