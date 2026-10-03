from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from nps_lens.core.nps_math import valid_nps_scores


@dataclass(frozen=True)
class NpsSummary:
    n: int
    nps_avg: float
    promoter_rate: float
    neutral_rate: float
    detractor_rate: float

    @property
    def nps_classic_pp(self) -> float:
        # classic NPS in percentage points
        return (self.promoter_rate - self.detractor_rate) * 100.0


def summarize(df: pd.DataFrame, score_col: str = "NPS") -> NpsSummary:
    if df is None or df.empty or score_col not in df.columns:
        return NpsSummary(
            n=0,
            nps_avg=float("nan"),
            promoter_rate=0.0,
            neutral_rate=0.0,
            detractor_rate=0.0,
        )

    s = valid_nps_scores(df[score_col]).dropna()
    n = int(len(s))
    if n == 0:
        return NpsSummary(
            n=0,
            nps_avg=float("nan"),
            promoter_rate=0.0,
            neutral_rate=0.0,
            detractor_rate=0.0,
        )

    # Standard groups
    promoters = (s >= 9).mean()
    neutrals = ((s >= 7) & (s <= 8)).mean()
    detractors = (s <= 6).mean()
    return NpsSummary(
        n=n,
        nps_avg=float(s.mean()),
        promoter_rate=float(promoters),
        neutral_rate=float(neutrals),
        detractor_rate=float(detractors),
    )
