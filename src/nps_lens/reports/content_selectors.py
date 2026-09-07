from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class MarkdownSegment:
    text: str
    bold: bool = False


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
    work = delta_df.copy()
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


def select_gap_rows(gap_df: pd.DataFrame, *, max_rows: int) -> pd.DataFrame:
    """Select the largest negative gaps against the overall NPS."""

    if gap_df is None or gap_df.empty:
        return pd.DataFrame(columns=getattr(gap_df, "columns", []))
    work = gap_df.copy()
    work["gap_vs_overall"] = _numeric_series(work, "gap_vs_overall")
    work["n"] = _numeric_series(work, "n").fillna(0.0)
    work = work.dropna(subset=["gap_vs_overall"])
    if work.empty:
        return work
    negative = work[work["gap_vs_overall"] < 0].copy()
    if negative.empty:
        negative = work.copy()
    return (
        negative.sort_values(["gap_vs_overall", "n", "value"], ascending=[True, False, True])
        .head(max_rows)
        .copy()
    )


def select_causal_scenarios(chain_df: pd.DataFrame, *, max_rows: int) -> pd.DataFrame:
    """Order evidence by transparent counts and semantic similarity."""

    if chain_df is None or chain_df.empty:
        return pd.DataFrame(columns=getattr(chain_df, "columns", []))
    work = chain_df.copy()
    for column in (
        "linked_pairs",
        "linked_incidents",
        "linked_comments",
        "responses",
        "avg_similarity",
    ):
        work[column] = _numeric_series(work, column).fillna(0.0)
    label_column = next(
        (column for column in ("nps_topic", "entity_label", "journey") if column in work),
        None,
    )
    work["_scenario_label"] = (
        work[label_column].astype(str) if label_column is not None else work.index.astype(str)
    )
    return (
        work.sort_values(
            [
                "linked_pairs",
                "linked_incidents",
                "linked_comments",
                "responses",
                "avg_similarity",
                "_scenario_label",
            ],
            ascending=[False, False, False, False, False, True],
        )
        .head(max_rows)
        .drop(columns="_scenario_label")
        .copy()
    )


def select_nonzero_kpis(
    kpis: Iterable[tuple[str, object, str]], *, max_items: int
) -> list[tuple[str, str, str]]:
    """Keep only KPIs with a meaningful non-zero numeric value."""

    selected: list[tuple[str, str, str]] = []
    for label, raw_value, accent in kpis:
        numeric = _extract_numeric(raw_value)
        if numeric is None or abs(numeric) <= 1e-12:
            continue
        selected.append((str(label), str(raw_value), str(accent)))
        if len(selected) >= max_items:
            break
    return selected


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


def _extract_numeric(value: object) -> float | None:
    if isinstance(value, (int, float, np.integer, np.floating)):
        out = float(value)
        return out if np.isfinite(out) else None
    text = str(value or "").replace(",", ".")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        out = float(match.group(0))
    except ValueError:
        return None
    return out if np.isfinite(out) else None
