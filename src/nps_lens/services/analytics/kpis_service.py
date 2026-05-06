from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional, cast

import numpy as np
import pandas as pd

from nps_lens.ui.population import POP_ALL

_KPI_ORDER = ("nps_average", "classic_nps", "detractor_rate", "promoter_rate", "comments")
_PERCENTAGE_KPIS = {"detractor_rate", "neutral_rate", "promoter_rate"}
_VOLUME_KPIS = {"samples", "comments"}
_DELTA_UNITS = {
    "nps_average": "pts",
    "classic_nps": "pts",
    "detractor_rate": "pp",
    "neutral_rate": "pp",
    "promoter_rate": "pp",
}
_COMMENT_COLUMNS = ("comment_txt", "Comment", "Comentario", "Comentarios", "comentario")
_DATE_COLUMNS = ("Fecha", "date")


@dataclass(frozen=True)
class ScoreKpis:
    samples: int
    nps_average: Optional[float]
    classic_nps: Optional[float]
    detractor_rate: Optional[float]
    neutral_rate: Optional[float]
    promoter_rate: Optional[float]
    comments: int

    def to_dict(self) -> dict[str, object]:
        return {
            "samples": self.samples,
            "nps_average": self.nps_average,
            "classic_nps": self.classic_nps,
            "detractor_rate": self.detractor_rate,
            "neutral_rate": self.neutral_rate,
            "promoter_rate": self.promoter_rate,
            "comments": self.comments,
        }


def _score_series(frame: pd.DataFrame) -> pd.Series[Any]:
    if frame is None or frame.empty or "NPS" not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame["NPS"], errors="coerce").dropna()


def _useful_comment_count(frame: pd.DataFrame, *, fallback: int) -> int:
    if frame is None or frame.empty:
        return 0
    for column in _COMMENT_COLUMNS:
        if column not in frame.columns:
            continue
        raw = frame[column]
        if not isinstance(raw, pd.Series):
            continue
        text = raw.where(raw.notna(), "").astype(str).str.strip()
        text = text[~text.str.casefold().isin({"", "nan", "none", "null"})]
        return int(text.size)
    return int(fallback)


def compute_score_kpis(frame: pd.DataFrame) -> ScoreKpis:
    scores = _score_series(frame)
    comment_count = _useful_comment_count(frame, fallback=int(len(scores)))
    if scores.empty:
        return ScoreKpis(
            samples=0,
            nps_average=None,
            classic_nps=None,
            detractor_rate=None,
            neutral_rate=None,
            promoter_rate=None,
            comments=comment_count,
        )
    total = int(len(scores))
    detractor_rate = float((scores <= 6.0).mean())
    promoter_rate = float((scores >= 9.0).mean())
    return ScoreKpis(
        samples=total,
        nps_average=float(scores.mean()),
        classic_nps=float((promoter_rate - detractor_rate) * 100.0),
        detractor_rate=detractor_rate,
        neutral_rate=float(((scores >= 7.0) & (scores <= 8.0)).mean()),
        promoter_rate=promoter_rate,
        comments=comment_count,
    )


def _coerce_dates(frame: pd.DataFrame) -> pd.Series[Any]:
    if frame is None or frame.empty:
        return pd.Series(dtype="datetime64[ns]")
    for column in _DATE_COLUMNS:
        if column in frame.columns:
            return pd.to_datetime(frame[column], errors="coerce")
    return pd.Series(dtype="datetime64[ns]")


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


def _explicit_or_selection_start_date(
    frame: pd.DataFrame,
    pop_year: str,
    pop_month: str,
    period_start: Optional[date],
) -> Optional[pd.Timestamp]:
    if period_start is not None:
        return pd.Timestamp(period_start).normalize()
    return _selection_start_date(frame, pop_year, pop_month)


def _explicit_or_selection_end_date(
    frame: pd.DataFrame,
    pop_year: str,
    pop_month: str,
    period_end: Optional[date],
) -> Optional[pd.Timestamp]:
    if period_end is not None:
        return pd.Timestamp(period_end).normalize()
    return _selection_end_date(frame, pop_year, pop_month)


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


def _slice_until(frame: pd.DataFrame, end_date: Optional[pd.Timestamp]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(frame.columns) if frame is not None else [])
    if end_date is None:
        return frame.copy()
    dates = _coerce_dates(frame)
    return frame.loc[dates.notna() & (dates.dt.normalize() <= end_date)].copy()


def _slice_before(frame: pd.DataFrame, start_date: Optional[pd.Timestamp]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(frame.columns) if frame is not None else [])
    if start_date is None:
        return pd.DataFrame(columns=list(frame.columns))
    dates = _coerce_dates(frame)
    return frame.loc[dates.notna() & (dates.dt.normalize() < start_date)].copy()


def _period_boundary_frames(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame is None or frame.empty:
        columns = list(frame.columns) if frame is not None else []
        empty = pd.DataFrame(columns=columns)
        return empty, empty
    dates = _coerce_dates(frame)
    valid_dates = dates.dropna()
    if valid_dates.empty:
        empty = pd.DataFrame(columns=list(frame.columns))
        return empty, empty
    normalized = dates.dt.normalize()
    start_date = normalized.loc[valid_dates.index].min()
    end_date = normalized.loc[valid_dates.index].max()
    start_frame = frame.loc[normalized.eq(start_date)].copy()
    end_frame = frame.loc[normalized.eq(end_date)].copy()
    return start_frame, end_frame


def _date_label(frame: pd.DataFrame, *, default: str) -> str:
    dates = _coerce_dates(frame).dropna()
    if dates.empty:
        return default
    return pd.Timestamp(dates.min()).date().isoformat()


def _delta_payload(
    current: Optional[float],
    baseline: Optional[float],
    *,
    lower_is_better: bool = False,
    neutral: bool = False,
) -> dict[str, object]:
    if current is None or baseline is None:
        return {"value": None, "direction": "flat", "favorable": None}
    delta = float(current) - float(baseline)
    if not np.isfinite(delta) or abs(delta) < 1e-9:
        return {"value": 0.0, "direction": "flat", "favorable": None}
    favorable = None if neutral else (delta < 0 if lower_is_better else delta > 0)
    return {
        "value": delta,
        "direction": "up" if delta > 0 else "down",
        "favorable": favorable,
    }


def _finite_float(value: object) -> Optional[float]:
    try:
        numeric = float(cast(Any, value))
    except Exception:
        return None
    if not np.isfinite(numeric):
        return None
    return float(numeric)


def _format_locale_number(
    value: object,
    *,
    decimals: int,
    signed: bool = False,
    default: str = "n/d",
) -> str:
    numeric = _finite_float(value)
    if numeric is None:
        return default
    precision = max(int(decimals), 0)
    threshold = 0.5 * (10**-precision) if precision > 0 else 0.5
    if abs(numeric) < threshold:
        numeric = 0.0
    rendered = f"{numeric:+,.{precision}f}" if signed else f"{numeric:,.{precision}f}"
    return rendered.replace(",", "_").replace(".", ",").replace("_", ".")


def format_metric(value: object, *, signed: bool = False, default: str = "n/d") -> str:
    return _format_locale_number(value, decimals=2, signed=signed, default=default)


def format_percentage(value: object, *, signed: bool = False, default: str = "n/d") -> str:
    numeric = _finite_float(value)
    if numeric is None:
        return default
    return f"{_format_locale_number(numeric * 100.0, decimals=2, signed=signed, default=default)}%"


def format_volume(value: object, *, signed: bool = False, default: str = "0") -> str:
    return _format_locale_number(value, decimals=0, signed=signed, default=default)


def format_kpi_value(kpi_key: str, value: object) -> str:
    if kpi_key in _PERCENTAGE_KPIS:
        return format_percentage(value)
    if kpi_key in _VOLUME_KPIS:
        return format_volume(value)
    return format_metric(value)


def format_delta(value: object, *, kpi_key: str) -> str:
    numeric = _finite_float(value)
    if numeric is None:
        return "sin histórico"
    direction = "↑" if numeric > 0 else "↓" if numeric < 0 else "→"
    if kpi_key in _PERCENTAGE_KPIS:
        amount = _format_locale_number(numeric * 100.0, decimals=2, signed=True)
    elif kpi_key in _VOLUME_KPIS:
        amount = format_volume(numeric, signed=True)
    else:
        amount = format_metric(numeric, signed=True)
    unit = _DELTA_UNITS.get(kpi_key, "")
    return f"{direction} {amount}{f' {unit}' if unit else ''}".strip()


def _score_value(kpis: ScoreKpis, kpi_key: str) -> Optional[float]:
    value = getattr(kpis, kpi_key)
    if value is None:
        return None
    return float(value)


def _delta_payloads(actual: ScoreKpis, baseline: ScoreKpis) -> dict[str, object]:
    deltas: dict[str, object] = {}
    for kpi_key in _KPI_ORDER:
        payload = _delta_payload(
            _score_value(actual, kpi_key),
            _score_value(baseline, kpi_key),
            lower_is_better=kpi_key == "detractor_rate",
            neutral=kpi_key == "comments",
        )
        payload["display"] = format_delta(payload.get("value"), kpi_key=kpi_key)
        deltas[kpi_key] = payload
    return deltas


def _display_payload(kpis: ScoreKpis) -> dict[str, str]:
    return {key: format_kpi_value(key, getattr(kpis, key)) for key in _KPI_ORDER}


def _comparison_payload(
    *,
    label: str,
    actual: ScoreKpis,
    baseline: ScoreKpis,
    base_label: str,
    actual_label: str,
    note: str = "",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "label": label,
        "base_label": base_label,
        "actual_label": actual_label,
        "kpis": actual.to_dict(),
        "base_kpis": baseline.to_dict(),
        "deltas": _delta_payloads(actual, baseline),
        "display": _display_payload(actual),
        "base_display": _display_payload(baseline),
    }
    if note:
        payload["note"] = note
    return payload


def build_period_boundary_kpis(current_df: pd.DataFrame) -> dict[str, object]:
    start_frame, end_frame = _period_boundary_frames(current_df)
    baseline = compute_score_kpis(start_frame)
    actual = compute_score_kpis(end_frame)
    return _comparison_payload(
        label="Evolución temporal del periodo",
        actual=actual,
        baseline=baseline,
        base_label=_date_label(start_frame, default="Inicio periodo"),
        actual_label=_date_label(end_frame, default="Fin periodo"),
    )


def build_period_kpis(
    *,
    history_df: pd.DataFrame,
    current_df: pd.DataFrame,
    pop_year: str,
    pop_month: str,
    context_label: str,
    period_start: Optional[date] = None,
    period_end: Optional[date] = None,
) -> dict[str, object]:
    start_date = _explicit_or_selection_start_date(history_df, pop_year, pop_month, period_start)
    end_date = _explicit_or_selection_end_date(history_df, pop_year, pop_month, period_end)
    cumulative_df = _slice_until(history_df, end_date)
    baseline_df = _slice_before(history_df, start_date)
    current = compute_score_kpis(current_df)
    cumulative = compute_score_kpis(cumulative_df)
    baseline = compute_score_kpis(baseline_df)
    return {
        "cumulative": _comparison_payload(
            label=f"Datos acumulados hasta {context_label}",
            actual=cumulative,
            baseline=baseline,
            base_label="Inicio periodo",
            actual_label="Fin periodo",
            note="KPIs calculados solo con Service Container y Period Container.",
        ),
        "period": {
            **_comparison_payload(
                label=context_label,
                actual=current,
                baseline=baseline,
                base_label="Base histórica anterior",
                actual_label=context_label,
            ),
            "temporal": build_period_boundary_kpis(current_df),
        },
    }


def build_scope_kpis(
    *,
    history_df: pd.DataFrame,
    current_df: pd.DataFrame,
    pop_year: str,
    pop_month: str,
    context_label: str,
) -> dict[str, object]:
    return build_period_kpis(
        history_df=history_df,
        current_df=current_df,
        pop_year=pop_year,
        pop_month=pop_month,
        context_label=context_label,
    )
