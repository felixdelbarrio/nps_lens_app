from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from nps_lens.analytics.drivers import compute_nps_from_scores


@dataclass(frozen=True)
class NpsSummary:
    n: int
    nps_avg: float
    promoter_rate: float
    detractor_rate: float

    @property
    def nps_classic_pp(self) -> float:
        # classic NPS in percentage points
        return (self.promoter_rate - self.detractor_rate) * 100.0


def summarize(df: pd.DataFrame, score_col: str = "NPS") -> NpsSummary:
    if df is None or df.empty or score_col not in df.columns:
        return NpsSummary(n=0, nps_avg=float("nan"), promoter_rate=0.0, detractor_rate=0.0)

    s = pd.to_numeric(df[score_col], errors="coerce").dropna()
    n = int(len(s))
    if n == 0:
        return NpsSummary(n=0, nps_avg=float("nan"), promoter_rate=0.0, detractor_rate=0.0)

    # Standard groups
    promoters = (s >= 9).mean()
    detractors = (s <= 6).mean()
    return NpsSummary(
        n=n,
        nps_avg=float(s.mean()),
        promoter_rate=float(promoters),
        detractor_rate=float(detractors),
    )


def daily_mix(df: pd.DataFrame, date_col: str = "Fecha", score_col: str = "NPS") -> pd.DataFrame:
    """Daily detractor/passive/promoter shares + n."""
    if df.empty or date_col not in df.columns or score_col not in df.columns:
        return pd.DataFrame()

    d = df[[date_col, score_col]].copy()
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce").dt.date
    d[score_col] = pd.to_numeric(d[score_col], errors="coerce")
    d = d.dropna(subset=[date_col, score_col])
    if d.empty:
        return pd.DataFrame()

    def bucket(x: float) -> str:
        if x <= 6:
            return "detractor"
        if x <= 8:
            return "passive"
        return "promoter"

    d["bucket"] = d[score_col].map(bucket)
    pivot = (
        d.pivot_table(index=date_col, columns="bucket", values=score_col, aggfunc="size", fill_value=0)
        .sort_index()
        .rename_axis(None, axis=0)
        .reset_index()
        .rename(columns={date_col: "day"})
    )
    pivot["n"] = pivot[["detractor", "passive", "promoter"]].sum(axis=1)
    for c in ["detractor", "passive", "promoter"]:
        pivot[c] = (pivot[c] / pivot["n"]).astype(float)
    return pivot


def daily_kpis(df: pd.DataFrame, date_col: str = "Fecha", score_col: str = "NPS") -> pd.DataFrame:
    """Daily N, %detractors and classic NPS (pp)."""
    if df.empty or date_col not in df.columns or score_col not in df.columns:
        return pd.DataFrame()

    d = df[[date_col, score_col]].copy()
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce").dt.date
    d[score_col] = pd.to_numeric(d[score_col], errors="coerce")
    d = d.dropna(subset=[date_col, score_col])
    if d.empty:
        return pd.DataFrame()

    g = d.groupby(date_col, dropna=False)
    out = pd.DataFrame(
        {
            "day": g.size().index.astype(object),
            "n": g.size().values.astype(int),
            "nps_avg": g[score_col].mean().values.astype(float),
            "detractor_rate": g[score_col].apply(lambda x: (x <= 6).mean()).values.astype(float),
            "promoter_rate": g[score_col].apply(lambda x: (x >= 9).mean()).values.astype(float),
        }
    )
    out["nps_classic_pp"] = (out["promoter_rate"] - out["detractor_rate"]) * 100.0
    return out.sort_values("day")
