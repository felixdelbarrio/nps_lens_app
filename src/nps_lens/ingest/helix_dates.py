from __future__ import annotations

import contextlib
import re
import warnings
from typing import Any

import pandas as pd

_EMPTY_MARKERS = {"", "nan", "none", "null", "nat"}
_NUMERIC_TEXT_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")
_LATIN_AND_EXPLICIT_FORMATS = (
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M",
    "%d-%m-%Y",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y/%m/%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
)


def looks_like_helix_datetime_column(column: object) -> bool:
    lc = str(column or "").lower()
    return (
        "fecha" in lc
        or "date" in lc
        or "datetime" in lc
        or "timestamp" in lc
        or "datt" in lc
        or lc.endswith("_date")
        or lc.endswith("_datetime")
    )


def _empty_datetime(index: Any) -> pd.Series:
    return pd.Series(pd.NaT, index=index, dtype="datetime64[ns]")


def _to_utc_naive(values: Any, *, index: Any, **kwargs: Any) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce", utc=True, **kwargs)
    out = pd.Series(parsed, index=index)
    out = (
        out.dt.tz_convert("UTC").dt.tz_localize(None)
        if isinstance(out.dtype, pd.DatetimeTZDtype)
        else pd.to_datetime(out, errors="coerce")
    )
    return out.astype("datetime64[ns]")


def _coerce_numeric_datetime(values: pd.Series) -> pd.Series:
    numbers = pd.to_numeric(values, errors="coerce")
    out = _empty_datetime(numbers.index)
    if numbers.empty:
        return out

    valid = numbers.notna()
    abs_values = numbers.abs()
    # Helix exports mostly use 13-digit epoch milliseconds. We also handle
    # seconds/us/ns and Excel serial days, then normalize to timezone-naive UTC
    # because the rest of the app stores and compares naive datetimes.
    masks = (
        (valid & abs_values.ge(1e17), "ns"),
        (valid & abs_values.ge(1e14) & abs_values.lt(1e17), "us"),
        (valid & abs_values.ge(1e11) & abs_values.lt(1e14), "ms"),
        (valid & abs_values.ge(1e9) & abs_values.lt(1e11), "s"),
    )
    for mask, unit in masks:
        if not bool(mask.any()):
            continue
        out.loc[mask] = _to_utc_naive(numbers.loc[mask], index=numbers.loc[mask].index, unit=unit)

    excel_mask = valid & out.isna() & numbers.between(20_000, 80_000)
    if bool(excel_mask.any()):
        parsed = pd.to_datetime(
            numbers.loc[excel_mask],
            unit="D",
            origin="1899-12-30",
            errors="coerce",
        )
        out.loc[excel_mask] = pd.Series(parsed, index=numbers.loc[excel_mask].index).astype(
            "datetime64[ns]"
        )

    return out.astype("datetime64[ns]")


def _assign_missing(target: pd.Series, parsed: pd.Series) -> pd.Series:
    if parsed.empty:
        return target
    mask = target.loc[parsed.index].isna() & parsed.notna()
    if bool(mask.any()):
        target.loc[parsed.index[mask]] = parsed.loc[mask]
    return target


def _parse_text_datetimes(text: pd.Series) -> pd.Series:
    out = _empty_datetime(text.index)
    if text.empty:
        return out

    pending = text
    with contextlib.suppress(Exception):
        out = _assign_missing(
            out,
            _to_utc_naive(pending, index=pending.index, format="ISO8601"),
        )

    for fmt in _LATIN_AND_EXPLICIT_FORMATS:
        pending = text.loc[out.isna()]
        if pending.empty:
            break
        with contextlib.suppress(Exception):
            out = _assign_missing(out, _to_utc_naive(pending, index=pending.index, format=fmt))

    pending = text.loc[out.isna()]
    if not pending.empty:
        try:
            out = _assign_missing(
                out,
                _to_utc_naive(pending, index=pending.index, format="mixed", dayfirst=True),
            )
        except (TypeError, ValueError):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                with contextlib.suppress(Exception):
                    out = _assign_missing(
                        out,
                        _to_utc_naive(pending, index=pending.index, dayfirst=True),
                    )

    return out.astype("datetime64[ns]")


def coerce_helix_datetime_series(series: pd.Series) -> pd.Series:
    """Return a homogeneous timezone-naive datetime64[ns] Series for Helix dates.

    Helix extracts mix ISO strings with timezone, epoch ms/s, Excel serial dates,
    localized date strings and null markers. Parsing is centralized here so
    ingestion, storage recovery and operational metrics compare compatible
    naive UTC timestamps and avoid pandas' per-element inference warning.
    """

    raw = pd.Series(series, copy=False)
    out = _empty_datetime(raw.index)
    if raw.empty:
        return out

    if pd.api.types.is_datetime64_any_dtype(raw) or isinstance(raw.dtype, pd.DatetimeTZDtype):
        return _to_utc_naive(raw, index=raw.index)

    if pd.api.types.is_numeric_dtype(raw):
        return _coerce_numeric_datetime(raw)

    text = raw.astype("string").str.strip()
    present = raw.notna() & ~text.str.casefold().isin(_EMPTY_MARKERS)
    if not bool(present.any()):
        return out

    cleaned_numeric = text.loc[present].str.replace(r"[\s,]", "", regex=True)
    numeric_mask = cleaned_numeric.str.fullmatch(_NUMERIC_TEXT_RE.pattern).fillna(False)
    if bool(numeric_mask.any()):
        numeric_index = cleaned_numeric.index[numeric_mask]
        out.loc[numeric_index] = _coerce_numeric_datetime(
            pd.to_numeric(cleaned_numeric.loc[numeric_index], errors="coerce")
        )

    date_index = text.index[present & out.isna()]
    if len(date_index):
        out.loc[date_index] = _parse_text_datetimes(text.loc[date_index])

    return out.astype("datetime64[ns]")


# Actual occurrence/start precedes detection, legacy canonical dates and registration.
INCIDENT_DATE_PRIORITY = (
    "BBVA_StartDateTime",
    "BBVA_IncidentStartDate",
    "Incident Start Date",
    "Start Date",
    "Occurred Date",
    "Occurrence Date",
    "Fecha inicio",
    "Fecha ocurrencia",
    "BBVA_DetectionDate",
    "Reported Date",
    "Fecha",
    "Fecha apertura",
    "Open Date",
    "Submit Date",
    "SubmitDate",
    "Submitted Date",
    "SubmittedDate",
    "CreatedDate",
    "Created Date",
    "Fecha creación",
    "Fecha creacion",
    "Date",
)


def incident_occurrence_dates(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Resolve dates per row and retain the source field, including partial fallbacks."""
    dates = _empty_datetime(frame.index)
    sources = pd.Series("", index=frame.index, dtype="string")
    if "incident_occurred_at" in frame:
        dates = coerce_helix_datetime_series(frame["incident_occurred_at"])
        sources = frame.get("incident_occurred_at_source", sources).astype("string").fillna("")
        sources = sources.mask(dates.notna() & sources.eq(""), "incident_occurred_at")
    for candidate in INCIDENT_DATE_PRIORITY:
        for column in frame.columns:
            if str(column).casefold() != candidate.casefold():
                continue
            missing = dates.isna()
            if not missing.any():
                return dates, sources
            parsed = coerce_helix_datetime_series(frame.loc[missing, column])
            valid = parsed.notna()
            indices = parsed.index[valid]
            dates.loc[indices] = parsed.loc[indices]
            sources.loc[indices] = str(column)
    return dates, sources
