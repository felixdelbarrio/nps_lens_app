from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

INCIDENT_ID_COLUMNS: tuple[str, ...] = (
    "Incident Number",
    "ID de la Incidencia",
    "incident_id",
    "id",
)
RECORD_ID_COLUMNS: tuple[str, ...] = (
    "Record ID",
    "RecordID",
    "Record Id",
    "workItemId",
    "WorkItemId",
    "InstanceId",
    "Instance ID",
    "instance_id",
)
EXPLICIT_URL_COLUMNS: tuple[str, ...] = (
    "Incident URL",
    "Incident Link",
    "Record URL",
    "Record Link",
    "Document URL",
    "Document Link",
    "URL",
    "Link",
    "Href",
    "url",
)


def normalize_helix_incident_base_url(base_url: object) -> str:
    raw = str(base_url or "").strip()
    if not raw:
        return ""
    return raw.rstrip("/") + "/"


def _text_series(frame: pd.DataFrame, column: str, *, default: str = "") -> pd.Series[Any]:
    series = frame.get(column)
    if isinstance(series, pd.Series):
        return series.fillna("").astype(str).str.strip()
    return pd.Series([default] * len(frame), index=frame.index, dtype=object)


def _coalesce_text_columns(
    frame: pd.DataFrame,
    columns: Sequence[str],
    *,
    default: str = "",
) -> pd.Series[Any]:
    output = pd.Series([default] * len(frame), index=frame.index, dtype=object)
    lower_map = {str(column).strip().lower(): str(column) for column in frame.columns}
    for column in columns:
        actual_column = lower_map.get(str(column).strip().lower(), column)
        if actual_column not in frame.columns:
            continue
        candidate = _text_series(frame, actual_column, default=default)
        output = output.where(output.astype(str).str.strip().ne(""), candidate)
    return output.astype(str).fillna("").str.strip()


def _http_url_series(values: pd.Series[Any], *, base_url: str = "") -> pd.Series[Any]:
    series = values.fillna("").astype(str).str.strip()
    http = series.where(series.str.match(r"^https?://", case=False, na=False), "")
    normalized_base = normalize_helix_incident_base_url(base_url)
    if normalized_base:
        base_without_slash = normalized_base.rstrip("/")
        http = http.where(
            http.map(lambda value: str(value).strip().rstrip("/") != base_without_slash),
            "",
        )
    return http


def _incident_href_columns(frame: pd.DataFrame) -> list[str]:
    columns: list[str] = []
    lower_columns = {str(column).strip().lower(): str(column) for column in frame.columns}
    for incident_column in INCIDENT_ID_COLUMNS:
        for suffix in ("__href", "__hyperlink"):
            key = f"{incident_column}{suffix}".strip().lower()
            if key in lower_columns:
                columns.append(lower_columns[key])
    return columns


def _explicit_url_series(frame: pd.DataFrame, *, base_url: str) -> pd.Series[Any]:
    candidates = list(EXPLICIT_URL_COLUMNS) + _incident_href_columns(frame)
    return _http_url_series(_coalesce_text_columns(frame, candidates), base_url=base_url)


def _constructed_url_series(frame: pd.DataFrame, *, base_url: str) -> pd.Series[Any]:
    normalized_base = normalize_helix_incident_base_url(base_url)
    if not normalized_base:
        return pd.Series([""] * len(frame), index=frame.index, dtype=object)
    record_id = _coalesce_text_columns(frame, RECORD_ID_COLUMNS)
    return (
        record_id.map(lambda value: f"{normalized_base}{value}" if str(value or "").strip() else "")
        .astype(str)
        .fillna("")
        .str.strip()
    )


def resolve_helix_incident_url(
    incident_id: object,
    lookup: dict[str, str],
    *,
    current_url: object = "",
    base_url: object = "",
) -> str:
    current = str(current_url or "").strip()
    normalized_base = normalize_helix_incident_base_url(base_url)
    if current.lower().startswith(("http://", "https://")) and (
        not normalized_base or current.rstrip("/") != normalized_base.rstrip("/")
    ):
        return current
    key = str(incident_id or "").strip()
    return lookup.get(key, "")


def build_helix_incident_url_lookup(
    helix_df: pd.DataFrame,
    *,
    base_url: object = "",
) -> dict[str, str]:
    if helix_df is None or helix_df.empty:
        return {}

    normalized_base = normalize_helix_incident_base_url(base_url)
    explicit_url = _explicit_url_series(helix_df, base_url=normalized_base)
    constructed_url = _constructed_url_series(helix_df, base_url=normalized_base)
    resolved_url = explicit_url.where(explicit_url.ne(""), constructed_url)

    lookup: dict[str, str] = {}
    lower_map = {str(column).strip().lower(): str(column) for column in helix_df.columns}
    for incident_column in INCIDENT_ID_COLUMNS:
        actual_column = lower_map.get(incident_column.strip().lower(), incident_column)
        if actual_column not in helix_df.columns:
            continue
        incident_id = _text_series(helix_df, actual_column)
        for inc_id, url in zip(incident_id.tolist(), resolved_url.tolist()):
            key = str(inc_id or "").strip()
            href = str(url or "").strip()
            if not key or not href or key in lookup:
                continue
            lookup[key] = href
    return lookup


def enrich_helix_incident_links(
    helix_df: pd.DataFrame,
    *,
    base_url: object = "",
) -> pd.DataFrame:
    if helix_df is None:
        return pd.DataFrame()
    if helix_df.empty:
        return pd.DataFrame(columns=list(helix_df.columns))

    out = helix_df.copy()
    normalized_base = normalize_helix_incident_base_url(base_url)
    explicit_url = _explicit_url_series(out, base_url=normalized_base)
    constructed_url = _constructed_url_series(out, base_url=normalized_base)
    resolved_url = explicit_url.where(explicit_url.ne(""), constructed_url)
    lower_map = {str(column).strip().lower(): str(column) for column in out.columns}
    for incident_column in INCIDENT_ID_COLUMNS:
        actual_column = lower_map.get(incident_column.strip().lower(), incident_column)
        if actual_column not in out.columns:
            continue
        out[f"{actual_column}__href"] = resolved_url.astype(str).fillna("").str.strip()
    return out
