from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd

from nps_lens.analytics.drivers import compute_nps_from_scores, grouped_driver_stats


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
    survey_score_col: str = "NPS",
    min_n: int = 200,
) -> list[Opportunity]:
    overall = compute_nps_from_scores(df[survey_score_col])
    out: list[Opportunity] = []
    for dim in dimensions:
        if dim not in df.columns:
            continue
        grouped = grouped_driver_stats(df, dim, survey_score_col=survey_score_col)
        grouped = grouped.loc[grouped["n"] >= int(min_n)].copy()
        if grouped.empty:
            continue
        for _, row in grouped.iterrows():
            n = int(row["n"])
            nps = float(row["nps"]) if pd.notna(row["nps"]) else float("nan")
            delta = overall - nps  # how far below overall
            if np.isnan(delta) or delta <= 0:
                continue
            # naive uplift: close 60% of the gap
            uplift = float(delta * 0.6)
            # confidence proxy: more data -> higher confidence (cap at 1)
            conf = float(min(1.0, np.log10(max(n, 10)) / 5.0))
            value = str(row[dim])
            out.append(
                Opportunity(
                    dimension=dim,
                    value=value,
                    n=n,
                    current_nps=float(nps),
                    potential_uplift=uplift,
                    confidence=conf,
                    why=f"'{dim}={value}' está {delta:.2f} pts por debajo del NPS global",
                )
            )
    out.sort(key=lambda o: (o.potential_uplift * o.confidence, o.n), reverse=True)
    return out
