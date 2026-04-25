from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EditorialContentLimits:
    """Centralized limits for committee-grade slide density."""

    max_text_clusters: int = 3
    max_change_rows: int = 6
    max_web_rows: int = 8
    max_opportunities: int = 8
    max_opportunity_bullets: int = 3
    max_journey_rows: int = 6
    max_causal_scenarios: int = 3
    max_causal_kpis: int = 6
    max_visible_causal_kpis: int = 4
    max_helix_evidence: int = 4
    max_voc_evidence: int = 3


@dataclass(frozen=True)
class PptRect:
    """PowerPoint rectangle in inches."""

    left: float
    top: float
    width: float
    height: float


@dataclass(frozen=True)
class EditorialLayout:
    """Reusable 16:9 geometry primitives for the executive PPT."""

    slide_width: float = 13.333
    slide_height: float = 7.5
    margin_x: float = 0.66
    header_top: float = 0.28
    content_top: float = 1.48
    footer_top: float = 7.08
    gutter: float = 0.24
    radius: float = 0.12

    @property
    def content_width(self) -> float:
        return self.slide_width - (self.margin_x * 2)


EDITORIAL_LIMITS = EditorialContentLimits()
EDITORIAL_LAYOUT = EditorialLayout()


SLIDE_TITLES: tuple[str, ...] = (
    "NPS Lens",
    "NPS térmico",
    "Evolución del NPS del periodo",
    "Qué han dicho los clientes",
    "Qué ha cambiado en Palanca",
    "Qué ha cambiado en Subpalanca",
    "Dónde duele en Web · Palanca",
    "Dónde duele en Web · Subpalanca",
    "Oportunidades priorizadas · Palanca",
    "Oportunidades priorizadas · Subpalanca",
    "Narrativa causal",
    "Journeys de detracción",
    "Análisis causal editorial",
    "Detalle de evidencias Helix",
)
