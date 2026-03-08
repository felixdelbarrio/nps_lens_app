from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import calendar
from typing import Optional

import numpy as np
import pandas as pd

from nps_lens.ui.population import POP_ALL


@dataclass(frozen=True)
class PeriodWindow:
    start: date
    end: date


def default_windows(
    df: pd.DataFrame,
    days: int = 14,
    *,
    pop_year: str = "",
    pop_month: str = "",
) -> tuple[Optional[PeriodWindow], Optional[PeriodWindow]]:
    """Return default (current, baseline) windows aligned to the selected context month.

    - current: selected natural month in context; if no explicit month is selected,
      the latest month available in the dataframe
    - baseline: all historical data available before that current month
    """
    del days
    if "Fecha" not in df.columns:
        return None, None
    dts = pd.to_datetime(df["Fecha"], errors="coerce")
    dts = dts.dropna()
    if dts.empty:
        return None, None

    anchor_year: Optional[int] = None
    anchor_month: Optional[int] = None
    if str(pop_year or "").strip() != POP_ALL and str(pop_month or "").strip() != POP_ALL:
        try:
            anchor_year = int(str(pop_year).strip())
            anchor_month = int(str(pop_month).strip())
        except Exception:
            anchor_year = None
            anchor_month = None

    if anchor_year is None or anchor_month is None or not (1 <= anchor_month <= 12):
        max_d = dts.max().date()
        anchor_year = int(max_d.year)
        anchor_month = int(max_d.month)

    current_start = date(anchor_year, anchor_month, 1)
    current_end_natural = date(
        anchor_year,
        anchor_month,
        calendar.monthrange(anchor_year, anchor_month)[1],
    )

    dates_only = dts.dt.date
    current_month_mask = (dates_only >= current_start) & (dates_only <= current_end_natural)
    current_month_dates = dts.loc[current_month_mask]
    current_end = (
        current_month_dates.max().date() if not current_month_dates.empty else current_end_natural
    )
    current_window = PeriodWindow(current_start, current_end)

    baseline_end = current_start - timedelta(days=1)
    baseline_dates = dts.loc[dates_only <= baseline_end]
    if baseline_dates.empty:
        baseline_window = None
    else:
        baseline_window = PeriodWindow(baseline_dates.min().date(), baseline_end)

    return current_window, baseline_window


def context_period_days(df: pd.DataFrame, *, minimum: int = 1) -> int:
    """Return the full active context span in days.

    The app already scopes data by the selected context/time period, so charts that
    should follow that context must use this full span rather than an extra UI slider.
    """
    if "Fecha" not in df.columns:
        return int(max(1, minimum))
    dts = pd.to_datetime(df["Fecha"], errors="coerce").dropna()
    if dts.empty:
        return int(max(1, minimum))
    span = int((dts.max().date() - dts.min().date()).days) + 1
    return int(max(int(minimum), span))


def slice_by_window(df: pd.DataFrame, w: PeriodWindow) -> pd.DataFrame:
    if "Fecha" not in df.columns:
        return df.iloc[0:0].copy()
    tmp = df.copy()
    tmp["Fecha"] = pd.to_datetime(tmp["Fecha"], errors="coerce")
    mask = (tmp["Fecha"].dt.date >= w.start) & (tmp["Fecha"].dt.date <= w.end)
    out = tmp.loc[mask].copy()
    out.attrs["period_window"] = w
    return out


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
