from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EditorialContentLimits:
    """Centralized limits for committee-grade slide density."""

    max_text_chart_clusters: int = 10
    min_change_rows_n: int = 1
    max_change_rows: int = 4
    max_web_rows: int = 8
    max_opportunities: int = 8
    max_opportunity_bullets: int = 3
    max_journey_rows: int = 6
    max_causal_scenarios: int = 3
    max_visible_causal_kpis: int = 4
    max_helix_evidence: int = 4
    max_voc_evidence: int = 3

EDITORIAL_LIMITS = EditorialContentLimits()
