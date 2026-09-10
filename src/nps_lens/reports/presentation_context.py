from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import pandas as pd
import plotly.graph_objects as go


@dataclass(frozen=True)
class CausalEvidenceRecord:
    incident_id: str
    summary: str
    url: str = ""
    segments: list[dict[str, object]] | None = None


@dataclass(frozen=True)
class DimensionViewModel:
    change_table_df: pd.DataFrame
    change_figure: Optional[go.Figure]
    topic_table_df: pd.DataFrame


@dataclass(frozen=True)
class CausalScenarioViewModel:
    index: int
    row: pd.Series[Any]
    kpis: list[tuple[str, str, str]]
    incident_lines: list[str]
    comment_lines: list[str]
    helix_evidence_lines: list[str]
    helix_evidence_records: list[CausalEvidenceRecord]


@dataclass(frozen=True)
class CausalViewModel:
    touchpoint_source: str
    method_label: str
    method_title: str
    method_subtitle: str
    entity_summary_df: pd.DataFrame
    entity_summary_figure: Optional[go.Figure]
    entity_summary_kpis: list[dict[str, str]]
    journey_table_df: pd.DataFrame
    scenarios: list[CausalScenarioViewModel]


@dataclass(frozen=True)
class PresentationContext:
    service_origin: str
    service_origin_n1: str
    service_origin_n2: str
    period_start: date
    period_end: date
    period_label: str
    topic_channel: str
    overview: dict[str, object]
    period_kpis: dict[str, object]
    overview_figure: Optional[go.Figure]
    text_topics_df: pd.DataFrame
    current_label: str
    baseline_label: str
    dimensions: dict[str, DimensionViewModel]
    causal: CausalViewModel
