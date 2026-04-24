from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Sequence

import numpy as np
import pandas as pd

_SUPPORT_ORG_SPLIT_RE = re.compile(r"[\n,;|]+")
_MAX_REASONABLE_RESOLUTION_WEEKS = 104.0

_INCIDENT_ID_CANDIDATES = (
    "Incident Number",
    "ID de la Incidencia",
    "incident_id",
    "id",
)
_SUPPORT_ORG_CANDIDATES = (
    "Assigned Support Organization",
    "Assigned Support Organisation",
    "Assigned Support Group",
    "Support Organization",
    "Support Organisation",
)
_OPENED_AT_CANDIDATES = (
    "Submit Date",
    "SubmitDate",
    "Submitted Date",
    "CreatedDate",
    "Created Date",
    "Open Date",
    "Fecha apertura",
    "Fecha Apertura",
    "Fecha creación",
    "Fecha creacion",
    "Fecha",
    "bbva_startdatetime",
)
_RESOLVED_AT_CANDIDATES = (
    "Closed Date",
    "ClosedDate",
    "Resolved Date",
    "ResolvedDate",
    "Resolution Date",
    "Last Resolved Date",
    "Fecha cierre",
    "Fecha Cierre",
    "Fecha resolución",
    "Fecha resolucion",
    "bbva_closeddate",
    "closed_at",
)


@dataclass(frozen=True)
class HelixOperationalBenchmark:
    incident_to_support_orgs: dict[str, tuple[str, ...]]
    support_org_eta_weeks: dict[str, float]
    overall_eta_weeks: Optional[float]


@dataclass(frozen=True)
class HelixOperationalMetrics:
    owner_role: str
    eta_weeks: float


def _normalize_name(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _resolve_columns(frame: pd.DataFrame, candidates: Sequence[str]) -> list[str]:
    if frame is None or frame.empty:
        return []

    normalized_to_column = {_normalize_name(column): str(column) for column in frame.columns}
    matched: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        candidate_key = _normalize_name(candidate)
        direct = normalized_to_column.get(candidate_key)
        if direct and direct not in seen:
            matched.append(direct)
            seen.add(direct)
            continue
        for column in frame.columns:
            column_name = str(column)
            normalized_column = _normalize_name(column_name)
            if (
                candidate_key
                and normalized_column
                and (candidate_key in normalized_column or normalized_column in candidate_key)
                and column_name not in seen
            ):
                matched.append(column_name)
                seen.add(column_name)
                break
    return matched


def _coalesce_text_columns(
    frame: pd.DataFrame,
    candidates: Sequence[str],
    *,
    default: str = "",
) -> pd.Series:
    output = pd.Series([default] * len(frame), index=frame.index, dtype=object)
    for column in _resolve_columns(frame, candidates):
        candidate = frame[column].where(frame[column].notna(), "").astype(str).str.strip()
        output = output.where(output.astype(str).str.strip().ne(""), candidate)
    return output.astype(str).fillna("").str.strip()


def _coalesce_datetime_columns(frame: pd.DataFrame, candidates: Sequence[str]) -> pd.Series:
    output = pd.Series([pd.NaT] * len(frame), index=frame.index, dtype="datetime64[ns]")
    for column in _resolve_columns(frame, candidates):
        candidate = pd.to_datetime(frame[column], errors="coerce")
        output = output.where(output.notna(), candidate)
    return output


def _unique_preserve_order(values: Sequence[object]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        out.append(normalized)
        seen.add(normalized)
    return tuple(out)


def _split_support_orgs(value: object) -> tuple[str, ...]:
    if isinstance(value, list):
        return _unique_preserve_order(value)
    text = str(value or "").strip()
    if not text:
        return tuple()
    if not _SUPPORT_ORG_SPLIT_RE.search(text):
        return (text,)
    return _unique_preserve_order(_SUPPORT_ORG_SPLIT_RE.split(text))


def _existing_eta(value: object) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(parsed) if pd.notna(parsed) else float("nan")


def build_helix_operational_benchmark(helix_df: pd.DataFrame) -> HelixOperationalBenchmark:
    if helix_df is None or helix_df.empty:
        return HelixOperationalBenchmark({}, {}, None)

    base = pd.DataFrame(index=helix_df.index)
    base["incident_id"] = _coalesce_text_columns(helix_df, _INCIDENT_ID_CANDIDATES)
    base["support_orgs"] = _coalesce_text_columns(helix_df, _SUPPORT_ORG_CANDIDATES).map(
        _split_support_orgs
    )
    base["opened_at"] = _coalesce_datetime_columns(helix_df, _OPENED_AT_CANDIDATES)
    base["resolved_at"] = _coalesce_datetime_columns(helix_df, _RESOLVED_AT_CANDIDATES)
    base = base[base["incident_id"].astype(str).str.strip().ne("")].copy()
    if base.empty:
        return HelixOperationalBenchmark({}, {}, None)

    resolution_weeks = (base["resolved_at"] - base["opened_at"]).dt.total_seconds() / (
        86400.0 * 7.0
    )
    base["resolution_weeks"] = resolution_weeks.where(
        resolution_weeks.notna()
        & resolution_weeks.ge(0.0)
        & resolution_weeks.le(_MAX_REASONABLE_RESOLUTION_WEEKS)
    )

    incident_to_support_orgs: dict[str, tuple[str, ...]] = {}
    for incident_id, group in base.groupby("incident_id", dropna=False, observed=True):
        collected: list[str] = []
        for support_orgs in group["support_orgs"].tolist():
            if isinstance(support_orgs, tuple):
                collected.extend(list(support_orgs))
        incident_to_support_orgs[str(incident_id).strip()] = _unique_preserve_order(collected)

    exploded = base.explode("support_orgs").copy()
    exploded["support_orgs"] = (
        exploded["support_orgs"].where(exploded["support_orgs"].notna(), "").astype(str).str.strip()
    )
    exploded = exploded[
        exploded["support_orgs"].ne("")
        & pd.to_numeric(exploded["resolution_weeks"], errors="coerce").notna()
    ].copy()

    support_org_eta_weeks: dict[str, float] = {}
    if not exploded.empty:
        support_org_eta_weeks = {
            str(support_org).strip(): float(
                pd.to_numeric(group["resolution_weeks"], errors="coerce").mean()
            )
            for support_org, group in exploded.groupby("support_orgs", observed=True)
            if str(support_org).strip()
        }

    overall_values = pd.to_numeric(base["resolution_weeks"], errors="coerce").dropna()
    overall_eta_weeks = float(overall_values.mean()) if not overall_values.empty else None
    return HelixOperationalBenchmark(
        incident_to_support_orgs=incident_to_support_orgs,
        support_org_eta_weeks=support_org_eta_weeks,
        overall_eta_weeks=overall_eta_weeks,
    )


def summarize_operational_metrics_for_incidents(
    incident_ids: Sequence[object],
    benchmark: HelixOperationalBenchmark,
) -> HelixOperationalMetrics:
    unique_incident_ids = _unique_preserve_order(incident_ids)
    if not unique_incident_ids:
        return HelixOperationalMetrics("", float("nan"))

    support_orgs: list[str] = []
    eta_samples: list[float] = []
    seen_orgs: set[str] = set()
    for incident_id in unique_incident_ids:
        incident_support_orgs = benchmark.incident_to_support_orgs.get(
            str(incident_id).strip(), tuple()
        )
        for support_org in incident_support_orgs:
            normalized_support_org = str(support_org).strip()
            if not normalized_support_org:
                continue
            if normalized_support_org not in seen_orgs:
                support_orgs.append(normalized_support_org)
                seen_orgs.add(normalized_support_org)
            eta_value = benchmark.support_org_eta_weeks.get(normalized_support_org)
            if eta_value is not None and np.isfinite(float(eta_value)):
                eta_samples.append(float(eta_value))

    if not eta_samples and benchmark.overall_eta_weeks is not None:
        eta_samples = [float(benchmark.overall_eta_weeks)]

    eta_weeks = float(np.mean(eta_samples)) if eta_samples else float("nan")
    return HelixOperationalMetrics(
        owner_role=" · ".join(support_orgs),
        eta_weeks=eta_weeks,
    )


def enrich_rationale_with_operational_metrics(
    rationale_df: pd.DataFrame,
    *,
    links_df: pd.DataFrame,
    benchmark: HelixOperationalBenchmark,
) -> pd.DataFrame:
    if rationale_df is None:
        return pd.DataFrame()
    if rationale_df.empty or links_df is None or links_df.empty:
        return rationale_df.copy()

    topic_to_incidents: dict[str, tuple[str, ...]] = {}
    if {"nps_topic", "incident_id"}.issubset(links_df.columns):
        topic_links = links_df.loc[:, ["nps_topic", "incident_id"]].copy()
        topic_links["nps_topic"] = topic_links["nps_topic"].astype(str).str.strip()
        topic_links["incident_id"] = topic_links["incident_id"].astype(str).str.strip()
        topic_links = topic_links[
            topic_links["nps_topic"].ne("") & topic_links["incident_id"].ne("")
        ]
        topic_to_incidents = {
            str(topic).strip(): _unique_preserve_order(group["incident_id"].tolist())
            for topic, group in topic_links.groupby("nps_topic", observed=True)
        }

    out = rationale_df.copy()
    if "owner_role" not in out.columns:
        out["owner_role"] = ""
    if "eta_weeks" not in out.columns:
        out["eta_weeks"] = np.nan

    owner_roles: list[str] = []
    eta_weeks_values: list[float] = []
    for _, row in out.iterrows():
        topic = str(row.get("nps_topic", "") or "").strip()
        metrics = summarize_operational_metrics_for_incidents(
            topic_to_incidents.get(topic, tuple()),
            benchmark,
        )
        owner_roles.append(metrics.owner_role or str(row.get("owner_role", "") or "").strip())
        eta_weeks_values.append(
            metrics.eta_weeks
            if np.isfinite(metrics.eta_weeks)
            else _existing_eta(row.get("eta_weeks"))
        )

    out["owner_role"] = owner_roles
    out["eta_weeks"] = eta_weeks_values
    return out


def enrich_chain_with_operational_metrics(
    chain_df: pd.DataFrame,
    *,
    benchmark: HelixOperationalBenchmark,
) -> pd.DataFrame:
    if chain_df is None:
        return pd.DataFrame()
    if chain_df.empty:
        return chain_df.copy()

    out = chain_df.copy()
    if "owner_role" not in out.columns:
        out["owner_role"] = ""
    if "eta_weeks" not in out.columns:
        out["eta_weeks"] = np.nan

    owner_roles: list[str] = []
    eta_weeks_values: list[float] = []
    for _, row in out.iterrows():
        incident_records = row.get("incident_records")
        source_records = incident_records if isinstance(incident_records, list) else []
        incident_ids = [
            str(entry.get("incident_id", "") or "").strip()
            for entry in source_records
            if isinstance(entry, dict)
        ]
        metrics = summarize_operational_metrics_for_incidents(incident_ids, benchmark)
        owner_roles.append(metrics.owner_role or str(row.get("owner_role", "") or "").strip())
        eta_weeks_values.append(
            metrics.eta_weeks
            if np.isfinite(metrics.eta_weeks)
            else _existing_eta(row.get("eta_weeks"))
        )

    out["owner_role"] = owner_roles
    out["eta_weeks"] = eta_weeks_values
    return out
