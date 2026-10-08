from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EditorialContentLimits:
    """Centralized limits for committee-grade slide density."""

    max_text_chart_clusters: int = 10
    max_change_rows: int = 4
    max_journey_rows: int = 6
    max_voc_evidence: int = 3


EDITORIAL_LIMITS = EditorialContentLimits()
