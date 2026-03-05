from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PeriodWindow:
    start: date
    end: date


def default_windows(
    df: pd.DataFrame, days: int = 14
) -> tuple[Optional[PeriodWindow], Optional[PeriodWindow]]:
    """Return default (current, baseline) windows.

    - current: last `days` days available
    - baseline: previous `days` days
    """
    if "Fecha" not in df.columns:
        return None, None
    dts = pd.to_datetime(df["Fecha"], errors="coerce")
    dts = dts.dropna()
    if dts.empty:
        return None, None
    max_d = dts.max().date()
    cur_end = max_d
    cur_start = max_d - timedelta(days=days - 1)
    base_end = cur_start - timedelta(days=1)
    base_start = base_end - timedelta(days=days - 1)
    return PeriodWindow(cur_start, cur_end), PeriodWindow(base_start, base_end)


def slice_by_window(df: pd.DataFrame, w: PeriodWindow) -> pd.DataFrame:
    if "Fecha" not in df.columns:
        return df.iloc[0:0].copy()
    tmp = df.copy()
    tmp["Fecha"] = pd.to_datetime(tmp["Fecha"], errors="coerce")
    mask = (tmp["Fecha"].dt.date >= w.start) & (tmp["Fecha"].dt.date <= w.end)
    return tmp.loc[mask].copy()


def driver_delta_table(
    current_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    dimension: str,
    score_col: str = "NPS",
    min_n: int = 50,
) -> pd.DataFrame:
    """Compute delta NPS per driver value between two periods."""
    if dimension not in current_df.columns or dimension not in baseline_df.columns:
        return pd.DataFrame()
    if score_col not in current_df.columns or score_col not in baseline_df.columns:
        return pd.DataFrame()

    cur = current_df.dropna(subset=[dimension, score_col]).copy()
    base = baseline_df.dropna(subset=[dimension, score_col]).copy()
    if cur.empty or base.empty:
        return pd.DataFrame()

    cur[dimension] = cur[dimension].astype(str)
    base[dimension] = base[dimension].astype(str)

    cur_agg = cur.groupby(dimension, as_index=False).agg(
        n_current=(score_col, "size"),
        nps_current=(score_col, "mean"),
    )
    base_agg = base.groupby(dimension, as_index=False).agg(
        n_baseline=(score_col, "size"),
        nps_baseline=(score_col, "mean"),
    )
    merged = cur_agg.merge(base_agg, on=dimension, how="inner")
    merged = merged.loc[
        (merged["n_current"] >= int(min_n)) & (merged["n_baseline"] >= int(min_n))
    ].copy()
    if merged.empty:
        return pd.DataFrame()

    merged["delta_nps"] = merged["nps_current"] - merged["nps_baseline"]
    merged["value"] = merged[dimension]
    # Sort by deterioration first (most negative)
    merged = merged.sort_values("delta_nps", ascending=True)

    # Ensure float stability
    merged["nps_current"] = merged["nps_current"].astype(float)
    merged["nps_baseline"] = merged["nps_baseline"].astype(float)
    merged["delta_nps"] = merged["delta_nps"].astype(float)
    merged["n_current"] = merged["n_current"].astype(int)
    merged["n_baseline"] = merged["n_baseline"].astype(int)
    return merged[["value", "delta_nps", "nps_current", "nps_baseline", "n_current", "n_baseline"]]


def safe_mean(series: pd.Series) -> float:
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().empty:
        return float("nan")
    return float(np.nanmean(s))
