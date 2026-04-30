from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from nps_lens.core.nps_math import daily_metrics as shared_daily_metrics


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

    s = pd.to_numeric(df[score_col], errors="coerce").dropna()
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


def daily_mix(df: pd.DataFrame, date_col: str = "Fecha", score_col: str = "NPS") -> pd.DataFrame:
    """Daily detractor/passive/promoter shares + n."""
    metrics = shared_daily_metrics(df, days=None, date_col=date_col, score_col=score_col)
    if metrics.empty:
        return pd.DataFrame()
    out = metrics.rename(
        columns={
            "detractor_rate": "detractor",
            "passive_rate": "passive",
            "promoter_rate": "promoter",
        }
    )[["day", "n", "detractor", "passive", "promoter"]]
    return out.copy()


def daily_kpis(df: pd.DataFrame, date_col: str = "Fecha", score_col: str = "NPS") -> pd.DataFrame:
    """Daily N, %detractors and classic NPS (pp)."""
    metrics = shared_daily_metrics(df, days=None, date_col=date_col, score_col=score_col)
    if metrics.empty:
        return pd.DataFrame()
    out = metrics.rename(columns={"classic_nps": "nps_classic_pp"})[
        ["day", "n", "nps_avg", "detractor_rate", "promoter_rate", "nps_classic_pp"]
    ]
    return out.copy()
