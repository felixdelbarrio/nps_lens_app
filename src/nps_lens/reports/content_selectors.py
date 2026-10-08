from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from nps_lens.analytics.causal_evidence import scenario_impact_score
from nps_lens.analytics.signal_quality import actionable_rows
from nps_lens.reports.coherence import validate_delta_rows
from nps_lens.reports.narrative import text


@dataclass(frozen=True)
class MarkdownSegment:
    text: str
    bold: bool = False


def causal_scenario_title(row: Any, *, rank: int, include_topic: bool) -> str:
    task = text(row.get("affected_task"))
    symptom = text(row.get("observed_symptom"))
    topic = text(row.get("anchor_topic")) or text(row.get("nps_topic"))
    if task and symptom and task != "Tarea pendiente de validación":
        case = f"{task} → {symptom}"
        return f"{topic}: {case}" if include_topic and topic else case
    return topic if include_topic and topic else f"Escenario de evidencia {rank}"


def _series(df: pd.DataFrame, column: str, default: object = 0.0) -> pd.Series[Any]:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index)


def _numeric_series(df: pd.DataFrame, column: str, default: object = 0.0) -> pd.Series[Any]:
    return pd.to_numeric(_series(df, column, default=default), errors="coerce")


def select_text_clusters(text_topics_df: pd.DataFrame, *, max_clusters: int) -> pd.DataFrame:
    """Select the highest-volume clusters that can fit legibly on the slide."""

    if text_topics_df is None or text_topics_df.empty:
        return pd.DataFrame(columns=getattr(text_topics_df, "columns", []))
    work = text_topics_df.copy()
    work["n"] = _numeric_series(work, "n").fillna(0.0)
    return work.sort_values(["n", "cluster_id"], ascending=[False, True]).head(max_clusters).copy()


def select_negative_delta_rows(delta_df: pd.DataFrame, *, max_rows: int) -> pd.DataFrame:
    """Select real deteriorations only, strongest first, then highest current volume."""

    if delta_df is None or delta_df.empty:
        return pd.DataFrame(columns=getattr(delta_df, "columns", []))
    validate_delta_rows(delta_df.to_dict("records"))
    work = actionable_rows(delta_df)
    work["delta_nps"] = _numeric_series(work, "delta_nps")
    work["n_current"] = _numeric_series(work, "n_current").fillna(0.0)
    work = work.dropna(subset=["delta_nps"])
    if work.empty:
        return work
    deteriorations = work[work["delta_nps"] < 0].copy()
    if deteriorations.empty:
        return deteriorations
    return (
        deteriorations.sort_values(
            ["delta_nps", "n_current", "value"], ascending=[True, False, True]
        )
        .head(max_rows)
        .copy()
    )


def select_causal_scenarios(chain_df: pd.DataFrame, *, max_rows: int) -> pd.DataFrame:
    """Order evidence by unique populations and engine-specific quality."""

    if chain_df is None or chain_df.empty:
        return pd.DataFrame(columns=getattr(chain_df, "columns", []))
    work = actionable_rows(chain_df)
    if work.empty:
        return work
    for column in (
        "linked_pairs",
        "linked_incidents",
        "linked_comments",
        "responses",
        "avg_text_similarity",
    ):
        work[column] = _numeric_series(work, column).fillna(0.0)
    label_column = next(
        (column for column in ("nps_topic", "entity_label", "journey") if column in work),
        None,
    )
    work["_scenario_label"] = (
        work[label_column].astype(str) if label_column is not None else work.index.astype(str)
    )
    work["impact_score"] = work.apply(scenario_impact_score, axis=1)
    return (
        work.sort_values(
            [
                "linked_incidents",
                "linked_comments",
                "responses",
                "impact_score",
                "_scenario_label",
            ],
            ascending=[False, False, False, False, True],
        )
        .head(max_rows)
        .drop(columns="_scenario_label")
        .copy()
    )


def parse_markdown_strong(text: object) -> list[MarkdownSegment]:
    """Parse the small markdown subset used by business bullets: **bold**."""

    source = str(text or "")
    if not source:
        return []
    segments: list[MarkdownSegment] = []
    cursor = 0
    for match in re.finditer(r"\*\*(.+?)\*\*", source):
        if match.start() > cursor:
            segments.append(MarkdownSegment(source[cursor : match.start()], bold=False))
        inner = match.group(1)
        if inner:
            segments.append(MarkdownSegment(inner, bold=True))
        cursor = match.end()
    if cursor < len(source):
        segments.append(MarkdownSegment(source[cursor:], bold=False))
    return segments or [MarkdownSegment(source, bold=False)]


def strip_markdown_strong(text: object) -> str:
    return "".join(segment.text for segment in parse_markdown_strong(text))
