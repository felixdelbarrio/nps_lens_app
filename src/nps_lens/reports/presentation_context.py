from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import pandas as pd
import plotly.graph_objects as go


@dataclass(frozen=True)
class DimensionViewModel:
    dimension: str
    slide_number: int
    change_df: pd.DataFrame
    change_table_df: pd.DataFrame
    change_figure: Optional[go.Figure]
    web_heatmap_figure: Optional[go.Figure]
    web_table_df: pd.DataFrame
    opportunities_df: pd.DataFrame
    opportunities_figure: Optional[go.Figure]
    opportunity_bullets: list[str]


@dataclass(frozen=True)
class CausalScenarioViewModel:
    index: int
    row: pd.Series[Any]
    kpis: list[tuple[str, str, str]]
    incident_lines: list[str]
    comment_lines: list[str]
    helix_evidence_lines: list[str]


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
    period_days: int
    focus_name: str
    overview: dict[str, object]
    story_md: str
    selected_raw: pd.DataFrame
    daily_mix: pd.DataFrame
    daily_signals: pd.DataFrame
    overview_figure: Optional[go.Figure]
    text_topics_df: pd.DataFrame
    text_topic_figure: Optional[go.Figure]
    current_label: str
    baseline_label: str
    dimensions: dict[str, DimensionViewModel]
    causal: CausalViewModel
