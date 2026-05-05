from __future__ import annotations

import calendar
from dataclasses import dataclass
from typing import Any, Optional

import numpy as np
import pandas as pd

from nps_lens.ui.population import POP_ALL


@dataclass(frozen=True)
class ScoreKpis:
    samples: int
    nps_average: Optional[float]
    detractor_rate: Optional[float]
    neutral_rate: Optional[float]
    promoter_rate: Optional[float]

    def to_dict(self) -> dict[str, object]:
        return {
            "samples": self.samples,
            "nps_average": self.nps_average,
            "detractor_rate": self.detractor_rate,
            "neutral_rate": self.neutral_rate,
            "promoter_rate": self.promoter_rate,
        }


def _score_series(frame: pd.DataFrame) -> pd.Series[Any]:
    if frame is None or frame.empty or "NPS" not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame["NPS"], errors="coerce").dropna()


def compute_score_kpis(frame: pd.DataFrame) -> ScoreKpis:
    scores = _score_series(frame)
    if scores.empty:
        return ScoreKpis(
            samples=0,
            nps_average=None,
            detractor_rate=None,
            neutral_rate=None,
            promoter_rate=None,
        )
    total = int(len(scores))
    return ScoreKpis(
        samples=total,
        nps_average=float(scores.mean()),
        detractor_rate=float((scores <= 6.0).mean()),
        neutral_rate=float(((scores >= 7.0) & (scores <= 8.0)).mean()),
        promoter_rate=float((scores >= 9.0).mean()),
    )


def _coerce_dates(frame: pd.DataFrame) -> pd.Series[Any]:
    if frame is None or frame.empty or "Fecha" not in frame.columns:
        return pd.Series(dtype="datetime64[ns]")
    return pd.to_datetime(frame["Fecha"], errors="coerce")


def _selection_end_date(
    frame: pd.DataFrame, pop_year: str, pop_month: str
) -> Optional[pd.Timestamp]:
    dates = _coerce_dates(frame).dropna()
    if dates.empty:
        return None
    year_value = str(pop_year or POP_ALL).strip()
    month_value = str(pop_month or POP_ALL).strip()
    if year_value == POP_ALL:
        return pd.Timestamp(dates.max()).normalize()
    try:
        year = int(year_value)
    except ValueError:
        return pd.Timestamp(dates.max()).normalize()
    if month_value == POP_ALL:
        return pd.Timestamp(year=year, month=12, day=31)
    try:
        month = int(month_value)
    except ValueError:
        return pd.Timestamp(year=year, month=12, day=31)
    return pd.Timestamp(year=year, month=month, day=calendar.monthrange(year, month)[1])


def _selection_start_date(
    frame: pd.DataFrame, pop_year: str, pop_month: str
) -> Optional[pd.Timestamp]:
    dates = _coerce_dates(frame).dropna()
    if dates.empty:
        return None
    year_value = str(pop_year or POP_ALL).strip()
    month_value = str(pop_month or POP_ALL).strip()
    if year_value == POP_ALL:
        return pd.Timestamp(dates.min()).normalize()
    try:
        year = int(year_value)
    except ValueError:
        return pd.Timestamp(dates.min()).normalize()
    if month_value == POP_ALL:
        return pd.Timestamp(year=year, month=1, day=1)
    try:
        month = int(month_value)
    except ValueError:
        return pd.Timestamp(year=year, month=1, day=1)
    return pd.Timestamp(year=year, month=month, day=1)


def cumulative_until_period(frame: pd.DataFrame, pop_year: str, pop_month: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(frame.columns) if frame is not None else [])
    end_date = _selection_end_date(frame, pop_year, pop_month)
    if end_date is None:
        return frame.copy()
    dates = _coerce_dates(frame)
    return frame.loc[dates.notna() & (dates.dt.normalize() <= end_date)].copy()


def history_before_period(frame: pd.DataFrame, pop_year: str, pop_month: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(frame.columns) if frame is not None else [])
    start_date = _selection_start_date(frame, pop_year, pop_month)
    if start_date is None:
        return pd.DataFrame(columns=list(frame.columns))
    dates = _coerce_dates(frame)
    return frame.loc[dates.notna() & (dates.dt.normalize() < start_date)].copy()


def _delta_payload(
    current: Optional[float],
    baseline: Optional[float],
    *,
    lower_is_better: bool = False,
) -> dict[str, object]:
    if current is None or baseline is None:
        return {"value": None, "direction": "flat", "favorable": None}
    delta = float(current) - float(baseline)
    if not np.isfinite(delta) or abs(delta) < 1e-9:
        return {"value": 0.0, "direction": "flat", "favorable": None}
    favorable = delta < 0 if lower_is_better else delta > 0
    return {
        "value": delta,
        "direction": "up" if delta > 0 else "down",
        "favorable": favorable,
    }


def build_scope_kpis(
    *,
    history_df: pd.DataFrame,
    current_df: pd.DataFrame,
    pop_year: str,
    pop_month: str,
    context_label: str,
) -> dict[str, object]:
    cumulative_df = cumulative_until_period(history_df, pop_year, pop_month)
    baseline_df = history_before_period(history_df, pop_year, pop_month)
    current = compute_score_kpis(current_df)
    cumulative = compute_score_kpis(cumulative_df)
    baseline = compute_score_kpis(baseline_df)
    return {
        "cumulative": {
            "label": f"Datos acumulados hasta {context_label}",
            "kpis": cumulative.to_dict(),
            "note": "KPIs calculados solo con Service Container y Period Container.",
        },
        "period": {
            "label": context_label,
            "kpis": current.to_dict(),
            "deltas": {
                "nps_average": _delta_payload(current.nps_average, baseline.nps_average),
                "detractor_rate": _delta_payload(
                    current.detractor_rate,
                    baseline.detractor_rate,
                    lower_is_better=True,
                ),
                "neutral_rate": _delta_payload(current.neutral_rate, baseline.neutral_rate),
                "promoter_rate": _delta_payload(current.promoter_rate, baseline.promoter_rate),
            },
        },
    }
