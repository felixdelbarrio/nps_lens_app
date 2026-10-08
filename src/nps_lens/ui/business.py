from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from nps_lens.ui.population import POP_ALL


@dataclass(frozen=True)
class PeriodWindow:
    start: date
    end: date


def selected_month_label(
    *,
    pop_year: str = "",
    pop_month: str = "",
    df: Optional[pd.DataFrame] = None,
) -> str:
    """Return a business-friendly label for the selected context month."""
    months = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre",
    }

    anchor_year: Optional[int] = None
    anchor_month: Optional[int] = None
    if str(pop_year or "").strip() != POP_ALL and str(pop_month or "").strip() != POP_ALL:
        try:
            anchor_year = int(str(pop_year).strip())
            anchor_month = int(str(pop_month).strip())
        except Exception:
            anchor_year = None
            anchor_month = None

    if (anchor_year is None or anchor_month is None) and df is not None and "Fecha" in df.columns:
        dts = pd.to_datetime(df["Fecha"], errors="coerce").dropna()
        if not dts.empty:
            max_d = dts.max().date()
            anchor_year = int(max_d.year)
            anchor_month = int(max_d.month)

    if anchor_year is None or anchor_month is None or not (1 <= anchor_month <= 12):
        return "periodo seleccionado"

    return f"{months.get(anchor_month, 'Mes')} {anchor_year}"


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
    baseline_window = (
        None if baseline_dates.empty else PeriodWindow(baseline_dates.min().date(), baseline_end)
    )

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


def safe_mean(series: pd.Series) -> float:
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().empty:
        return float("nan")
    return float(np.nanmean(s))
