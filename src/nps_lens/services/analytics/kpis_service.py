from __future__ import annotations

import calendar
import contextlib
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional, cast

import numpy as np
import pandas as pd

from nps_lens.ui.population import MONTH_LABELS_ES, POP_ALL

_KPI_ORDER = ("comments", "nps_average", "classic_nps", "detractor_rate", "promoter_rate")
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


def _period_container_bounds(
    pop_year: str,
    pop_month: str,
) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    year_value = str(pop_year or POP_ALL).strip()
    month_value = str(pop_month or POP_ALL).strip()
    if year_value == POP_ALL:
        return None, None
    try:
        year = int(year_value)
    except ValueError:
        return None, None
    if month_value == POP_ALL:
        return pd.Timestamp(year=year, month=1, day=1), pd.Timestamp(year=year, month=12, day=31)
    try:
        month = int(month_value)
    except ValueError:
        return pd.Timestamp(year=year, month=1, day=1), pd.Timestamp(year=year, month=12, day=31)
    return (
        pd.Timestamp(year=year, month=month, day=1),
        pd.Timestamp(year=year, month=month, day=calendar.monthrange(year, month)[1]),
    )


def _period_container_date_mask(
    dates: pd.Series[Any],
    pop_year: str,
    pop_month: str,
) -> pd.Series[Any]:
    normalized = dates.dt.normalize()
    valid = dates.notna()
    start, end = _period_container_bounds(pop_year, pop_month)
    if start is not None:
        valid = valid & normalized.ge(start)
    if end is not None:
        valid = valid & normalized.le(end)
    year_value = str(pop_year or POP_ALL).strip()
    month_value = str(pop_month or POP_ALL).strip()
    if year_value == POP_ALL and month_value != POP_ALL:
        with contextlib.suppress(ValueError):
            valid = valid & dates.dt.month.eq(int(month_value))
    return valid


def _selection_end_date(
    frame: pd.DataFrame, pop_year: str, pop_month: str
) -> Optional[pd.Timestamp]:
    dates = _coerce_dates(frame).dropna()
    if dates.empty:
        return None
    all_dates = _coerce_dates(frame)
    in_selection = all_dates.loc[_period_container_date_mask(all_dates, pop_year, pop_month)]
    if in_selection.dropna().empty:
        _, container_end = _period_container_bounds(pop_year, pop_month)
        return (
            container_end.normalize()
            if container_end is not None
            else pd.Timestamp(dates.max()).normalize()
        )
    return pd.Timestamp(in_selection.dropna().max()).normalize()


def _selection_start_date(
    frame: pd.DataFrame, pop_year: str, pop_month: str
) -> Optional[pd.Timestamp]:
    dates = _coerce_dates(frame).dropna()
    if dates.empty:
        return None
    del pop_year, pop_month
    return pd.Timestamp(dates.min()).normalize()


def _current_period_start_date(
    frame: pd.DataFrame,
    pop_year: str,
    pop_month: str,
) -> Optional[pd.Timestamp]:
    dates = _coerce_dates(frame).dropna()
    if dates.empty:
        return None
    start, _ = _period_container_bounds(pop_year, pop_month)
    if start is not None:
        return start.normalize()
    all_dates = _coerce_dates(frame)
    in_selection = all_dates.loc[_period_container_date_mask(all_dates, pop_year, pop_month)]
    if in_selection.dropna().empty:
        return pd.Timestamp(dates.min()).normalize()
    return pd.Timestamp(in_selection.dropna().min()).normalize()


def _previous_period_bounds(
    pop_year: str,
    pop_month: str,
    current_start: Optional[pd.Timestamp],
) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    if current_start is None:
        return None, None
    year_value = str(pop_year or POP_ALL).strip()
    month_value = str(pop_month or POP_ALL).strip()
    if year_value != POP_ALL and month_value != POP_ALL:
        previous_start = pd.Timestamp(current_start).normalize() - pd.offsets.MonthBegin(1)
        previous_end = pd.Timestamp(current_start).normalize() - pd.Timedelta(days=1)
        return previous_start.normalize(), previous_end.normalize()
    if year_value != POP_ALL and month_value == POP_ALL:
        try:
            year = int(year_value)
        except ValueError:
            return None, None
        return pd.Timestamp(year=year - 1, month=1, day=1), pd.Timestamp(
            year=year - 1,
            month=12,
            day=31,
        )
    return None, pd.Timestamp(current_start).normalize() - pd.Timedelta(days=1)


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
    start_date = _current_period_start_date(frame, pop_year, pop_month)
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


def _slice_between(
    frame: pd.DataFrame,
    start_date: Optional[pd.Timestamp],
    end_date: Optional[pd.Timestamp],
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(frame.columns) if frame is not None else [])
    dates = _coerce_dates(frame)
    mask = dates.notna()
    if start_date is not None:
        mask = mask & dates.dt.normalize().ge(pd.Timestamp(start_date).normalize())
    if end_date is not None:
        mask = mask & dates.dt.normalize().le(pd.Timestamp(end_date).normalize())
    return frame.loc[mask].copy()


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
    if kpi_key in _PERCENTAGE_KPIS:
        amount = _format_locale_number(numeric * 100.0, decimals=2, signed=True)
    elif kpi_key in _VOLUME_KPIS:
        amount = format_volume(numeric, signed=True)
    else:
        amount = format_metric(numeric, signed=True)
    unit = _DELTA_UNITS.get(kpi_key, "")
    return f"{amount}{f' {unit}' if unit else ''}".strip()


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


def _kpi_payload(
    *,
    label: str,
    period_type: str,
    kpis: ScoreKpis,
    note: str = "",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "label": label,
        "period_type": period_type,
        "kpis": kpis.to_dict(),
        "display": _display_payload(kpis),
        "deltas": None,
        "show_deltas": False,
    }
    if note:
        payload["note"] = note
    return payload


def _comparison_payload(
    *,
    label: str,
    period_type: str,
    actual: ScoreKpis,
    baseline: ScoreKpis,
    base_label: str,
    actual_label: str,
    note: str = "",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "label": label,
        "period_type": period_type,
        "base_label": base_label,
        "actual_label": actual_label,
        "kpis": actual.to_dict(),
        "base_kpis": baseline.to_dict(),
        "deltas": _delta_payloads(actual, baseline),
        "display": _display_payload(actual),
        "base_display": _display_payload(baseline),
        "show_deltas": True,
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
        period_type="internal_period_evolution",
        actual=actual,
        baseline=baseline,
        base_label=_date_label(start_frame, default="Inicio periodo"),
        actual_label=_date_label(end_frame, default="Fin periodo"),
    )


def _month_label(ts: pd.Timestamp) -> str:
    month = str(int(ts.month)).zfill(2)
    return f"{MONTH_LABELS_ES.get(month, month)} {int(ts.year)}"


def _period_aggregate_payload(label: str, frame: pd.DataFrame) -> dict[str, object]:
    dates = _coerce_dates(frame).dropna()
    kpis = compute_score_kpis(frame)
    start = pd.Timestamp(dates.min()).date().isoformat() if not dates.empty else None
    end = pd.Timestamp(dates.max()).date().isoformat() if not dates.empty else None
    payload = kpis.to_dict()
    return {
        "label": label,
        "start_date": start,
        "end_date": end,
        "samples": kpis.samples,
        "comments": kpis.comments,
        "nps_average": payload["nps_average"],
        "classic_nps": payload["classic_nps"],
        "detractor_rate": payload["detractor_rate"],
        "promoter_rate": payload["promoter_rate"],
        "display": _display_payload(kpis),
    }


def build_period_aggregates(
    frame: pd.DataFrame,
    pop_year: str,
    pop_month: str,
) -> list[dict[str, object]]:
    if frame is None or frame.empty:
        return []
    dates = _coerce_dates(frame)
    end_date = _selection_end_date(frame, pop_year, pop_month)
    mask = dates.notna()
    if end_date is not None:
        mask = mask & dates.dt.normalize().le(end_date)
    scoped = frame.loc[mask].copy()
    if scoped.empty:
        return []
    scoped_dates = _coerce_dates(scoped)
    scoped = scoped.assign(_period_key=scoped_dates.dt.to_period("M").astype(str))
    aggregates: list[dict[str, object]] = []
    for period_key, group in scoped.groupby("_period_key", sort=True):
        if pd.isna(period_key):
            continue
        period_start = pd.Timestamp(pd.Period(str(period_key), freq="M").start_time)
        aggregates.append(_period_aggregate_payload(_month_label(period_start), group))
    return aggregates


def _previous_period_label(
    *,
    pop_year: str,
    pop_month: str,
    start_date: Optional[pd.Timestamp],
) -> str:
    year_value = str(pop_year or POP_ALL).strip()
    month_value = str(pop_month or POP_ALL).strip()
    if year_value != POP_ALL:
        try:
            year = int(year_value)
        except ValueError:
            year = 0
        if year and month_value != POP_ALL:
            try:
                month = int(month_value)
            except ValueError:
                month = 0
            if 1 <= month <= 12:
                previous = pd.Timestamp(year=year, month=month, day=1) - pd.offsets.MonthBegin(1)
                previous_month = str(int(previous.month)).zfill(2)
                return f"{MONTH_LABELS_ES.get(previous_month, previous_month)} {int(previous.year)}"
        if year:
            return str(year - 1)
    if start_date is not None:
        previous_day = pd.Timestamp(start_date).normalize() - pd.Timedelta(days=1)
        return previous_day.date().isoformat()
    return "periodo anterior"


def _available_date_range_note(frame: pd.DataFrame, *, prefix: str) -> str:
    dates = _coerce_dates(frame).dropna()
    if dates.empty:
        return f"{prefix}; no hay fechas suficientes para mostrar el rango."
    return f"{prefix} ({dates.min():%d/%m/%Y} a {dates.max():%d/%m/%Y})"


def _bounded_date_range_note(
    frame: pd.DataFrame,
    *,
    prefix: str,
    start: Optional[pd.Timestamp],
    end: Optional[pd.Timestamp],
) -> str:
    if frame.empty or start is None or end is None:
        return _available_date_range_note(frame, prefix=prefix)
    return f"{prefix} ({pd.Timestamp(start):%d/%m/%Y} a {pd.Timestamp(end):%d/%m/%Y})"


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
    cumulative_start = _selection_start_date(history_df, pop_year, pop_month)
    end_date = _explicit_or_selection_end_date(history_df, pop_year, pop_month, period_end)
    current_start = (
        pd.Timestamp(period_start).normalize()
        if period_start is not None
        else _current_period_start_date(history_df, pop_year, pop_month)
    )
    previous_start, previous_end = _previous_period_bounds(pop_year, pop_month, current_start)
    cumulative_df = _slice_between(history_df, cumulative_start, end_date)
    baseline_df = _slice_between(history_df, previous_start, previous_end)
    current = compute_score_kpis(current_df)
    cumulative = compute_score_kpis(cumulative_df)
    baseline = compute_score_kpis(baseline_df)
    previous_label = _previous_period_label(
        pop_year=pop_year,
        pop_month=pop_month,
        start_date=current_start,
    )
    temporal = build_period_boundary_kpis(current_df)
    if cumulative_start is not None and end_date is not None:
        cumulative_note = (
            "KPIs agregados para el periodo del "
            f"{pd.Timestamp(cumulative_start).date().isoformat()} al "
            f"{pd.Timestamp(end_date).date().isoformat()}."
        )
    else:
        cumulative_note = "KPIs agregados para el periodo disponible; no hay fechas suficientes para mostrar el rango."
    period_payload = _comparison_payload(
        label=context_label,
        period_type="current_period",
        actual=current,
        baseline=baseline,
        base_label=previous_label,
        actual_label=context_label,
        note=_available_date_range_note(current_df, prefix="KPIs agregados del período"),
    )
    period_payload["temporal"] = temporal
    return {
        "historical": _kpi_payload(
            label=previous_label,
            period_type="historical_previous",
            kpis=baseline,
            note=_bounded_date_range_note(
                baseline_df,
                prefix="KPIs agregados del período anterior",
                start=previous_start,
                end=previous_end,
            ),
        ),
        "period": period_payload,
        "cumulative": _kpi_payload(
            label=f"Datos acumulados hasta {context_label}",
            period_type="cumulative_to_current",
            kpis=cumulative,
            note=cumulative_note,
        ),
        "period_aggregates": build_period_aggregates(history_df, pop_year, pop_month),
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
