from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Sequence

import pandas as pd

_SUPPORT_ORG_SPLIT_RE = re.compile(r"[\n,;|]+")
_INCIDENT_ID_CANDIDATES = ("Incident Number", "ID de la Incidencia", "incident_id", "id")
_SUPPORT_ORG_CANDIDATES = (
    "Assigned Support Organization",
    "Assigned Support Organisation",
    "Assigned Support Group",
    "Support Organization",
    "Support Organisation",
)


@dataclass(frozen=True)
class HelixOperationalBenchmark:
    incident_to_support_orgs: dict[str, tuple[str, ...]]


@dataclass(frozen=True)
class HelixOperationalMetrics:
    support_organizations: str


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
    frame: pd.DataFrame, candidates: Sequence[str], *, default: str = ""
) -> pd.Series:
    output = pd.Series([default] * len(frame), index=frame.index, dtype=object)
    for column in _resolve_columns(frame, candidates):
        candidate = frame[column].where(frame[column].notna(), "").astype(str).str.strip()
        output = output.where(output.astype(str).str.strip().ne(""), candidate)
    return output.astype(str).fillna("").str.strip()


def _unique_preserve_order(values: Sequence[object]) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(str(value or "").strip() for value in values if str(value or "").strip())
    )


def _split_support_orgs(value: object) -> tuple[str, ...]:
    text = str(value or "").strip()
    if not text:
        return tuple()
    return _unique_preserve_order(_SUPPORT_ORG_SPLIT_RE.split(text))


def build_helix_operational_benchmark(helix_df: pd.DataFrame) -> HelixOperationalBenchmark:
    if helix_df is None or helix_df.empty:
        return HelixOperationalBenchmark({})
    incident_ids = _coalesce_text_columns(helix_df, _INCIDENT_ID_CANDIDATES)
    support_orgs = _coalesce_text_columns(helix_df, _SUPPORT_ORG_CANDIDATES).map(
        _split_support_orgs
    )
    mapping: dict[str, tuple[str, ...]] = {}
    for incident_id, organizations in zip(incident_ids, support_orgs):
        if incident_id:
            mapping[incident_id] = _unique_preserve_order(
                (*mapping.get(incident_id, ()), *organizations)
            )
    return HelixOperationalBenchmark(mapping)


def summarize_operational_metrics_for_incidents(
    incident_ids: Sequence[object], benchmark: HelixOperationalBenchmark
) -> HelixOperationalMetrics:
    organizations = _unique_preserve_order(
        organization
        for incident_id in _unique_preserve_order(incident_ids)
        for organization in benchmark.incident_to_support_orgs.get(incident_id, ())
    )
    return HelixOperationalMetrics(" · ".join(organizations))


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
    topic_links = links_df.loc[:, ["nps_topic", "incident_id"]].copy()
    topic_links = topic_links.assign(
        nps_topic=topic_links["nps_topic"].astype(str).str.strip(),
        incident_id=topic_links["incident_id"].astype(str).str.strip(),
    )
    topic_links = topic_links[topic_links["nps_topic"].ne("") & topic_links["incident_id"].ne("")]
    topic_to_incidents = {
        str(topic): _unique_preserve_order(group["incident_id"].tolist())
        for topic, group in topic_links.groupby("nps_topic", observed=True)
    }
    out = rationale_df.copy()
    out["support_organizations"] = [
        summarize_operational_metrics_for_incidents(
            topic_to_incidents.get(str(topic or "").strip(), ()), benchmark
        ).support_organizations
        for topic in out.get("nps_topic", pd.Series("", index=out.index))
    ]
    return out


def enrich_chain_with_operational_metrics(
    chain_df: pd.DataFrame, *, benchmark: HelixOperationalBenchmark
) -> pd.DataFrame:
    if chain_df is None:
        return pd.DataFrame()
    if chain_df.empty:
        return chain_df.copy()
    out = chain_df.copy()
    out["support_organizations"] = [
        summarize_operational_metrics_for_incidents(
            tuple(dict.fromkeys(pair[0] for pair in pairs)), benchmark
        ).support_organizations
        for pairs in out["evidence_pairs"]
    ]
    return out
