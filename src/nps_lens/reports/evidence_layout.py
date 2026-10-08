"""Shared geometry for topic summaries, separators and single-slide evidence."""

from dataclasses import dataclass


@dataclass(frozen=True)
class EvidenceLayout:
    margin: float = 0.38
    body_top: float = 1.35
    body_bottom: float = 4.69
    metric_width: float = 2.60
    content_left: float = 3.55
    content_width: float = 5.83
    gap: float = 0.10
    padding: float = 0.12
    body_font: float = 11
    title_font: float = 20
    conclusion_top: float = 4.91
    conclusion_height: float = 0.39
    max_summary_topics: int = 4
    label_font: float = 10
    kpi_font: float = 30


EVIDENCE_LAYOUT = EvidenceLayout()
