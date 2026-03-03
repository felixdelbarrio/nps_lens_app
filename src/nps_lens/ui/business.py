from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PeriodWindow:
    start: date
    end: date


@dataclass(frozen=True)
class PeriodWindows:
    """Convenience bundle: (current, baseline) + human labels."""

    current: PeriodWindow
    baseline: PeriodWindow
    label_current: str
    label_baseline: str
    periodicity: str


PERIODICITIES = [
    "Semanal (ISO)",
    "Quincenal (14d)",
    "Mensual",
    "Manual",
]


def default_windows(df: pd.DataFrame, days: int = 14) -> Tuple[Optional[PeriodWindow], Optional[PeriodWindow]]:
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
    cur_start = (max_d - timedelta(days=days - 1))
    base_end = cur_start - timedelta(days=1)
    base_start = base_end - timedelta(days=days - 1)
    return PeriodWindow(cur_start, cur_end), PeriodWindow(base_start, base_end)


def _date_bounds(df: pd.DataFrame) -> Optional[Tuple[date, date]]:
    if "Fecha" not in df.columns:
        return None
    dts = pd.to_datetime(df["Fecha"], errors="coerce").dropna()
    if dts.empty:
        return None
    return dts.min().date(), dts.max().date()


def pandas_freq_for_periodicity(periodicity: str) -> str:
    """Best-effort pandas resample frequency for trend charts."""
    if periodicity == "Semanal (ISO)":
        # Weeks ending Sunday; stable for business trend charts
        return "W-SUN"
    if periodicity == "Quincenal (14d)":
        return "2W-SUN"
    if periodicity == "Mensual":
        return "M"
    return "D"


def period_windows(df: pd.DataFrame, periodicity: str) -> Optional[PeriodWindows]:
    """Return (current, baseline) windows aligned to a business periodicity.

    Rules (best-effort):
    - Weekly: ISO-style weeks, current = week containing max(date), baseline = previous week.
    - Biweekly: 14-day blocks aligned to Mondays, based on max(date).
    - Monthly: calendar month of max(date), baseline = previous month.
    - Manual: falls back to last 14 days vs previous 14 days.
    """
    bounds = _date_bounds(df)
    if bounds is None:
        return None
    _, max_d = bounds

    if periodicity == "Semanal (ISO)":
        # Monday..Sunday
        weekday = max_d.weekday()  # Mon=0
        start = max_d - timedelta(days=weekday)
        end = start + timedelta(days=6)
        cur = PeriodWindow(start, end)
        base_end = start - timedelta(days=1)
        base_start = base_end - timedelta(days=6)
        base = PeriodWindow(base_start, base_end)
        return PeriodWindows(
            current=cur,
            baseline=base,
            label_current=f"Semana {start.isoformat()} → {end.isoformat()}",
            label_baseline=f"Semana {base_start.isoformat()} → {base_end.isoformat()}",
            periodicity=periodicity,
        )

    if periodicity == "Quincenal (14d)":
        # Align blocks to Monday starts for legibility
        end = max_d
        start = end - timedelta(days=13)
        # Snap start to Monday by moving backwards within the block.
        start = start - timedelta(days=start.weekday())
        end = start + timedelta(days=13)
        cur = PeriodWindow(start, end)
        base_end = start - timedelta(days=1)
        base_start = base_end - timedelta(days=13)
        base = PeriodWindow(base_start, base_end)
        return PeriodWindows(
            current=cur,
            baseline=base,
            label_current=f"Quincena {start.isoformat()} → {end.isoformat()}",
            label_baseline=f"Quincena {base_start.isoformat()} → {base_end.isoformat()}",
            periodicity=periodicity,
        )

    if periodicity == "Mensual":
        # Calendar month boundaries
        start = max_d.replace(day=1)
        # Next month start
        if start.month == 12:
            next_month = date(start.year + 1, 1, 1)
        else:
            next_month = date(start.year, start.month + 1, 1)
        end = next_month - timedelta(days=1)
        cur = PeriodWindow(start, end)

        # Previous month
        prev_end = start - timedelta(days=1)
        prev_start = prev_end.replace(day=1)
        base = PeriodWindow(prev_start, prev_end)
        label_cur = start.strftime("%B %Y")
        label_base = prev_start.strftime("%B %Y")
        return PeriodWindows(
            current=cur,
            baseline=base,
            label_current=f"Mes {label_cur}",
            label_baseline=f"Mes {label_base}",
            periodicity=periodicity,
        )

    # Manual / fallback
    w_cur, w_base = default_windows(df, days=14)
    if w_cur is None or w_base is None:
        return None
    return PeriodWindows(
        current=w_cur,
        baseline=w_base,
        label_current=f"Últimos 14 días ({w_cur.start.isoformat()} → {w_cur.end.isoformat()})",
        label_baseline=f"14 días previos ({w_base.start.isoformat()} → {w_base.end.isoformat()})",
        periodicity="Manual",
    )


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
    merged = merged.loc[(merged["n_current"] >= int(min_n)) & (merged["n_baseline"] >= int(min_n))].copy()
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
