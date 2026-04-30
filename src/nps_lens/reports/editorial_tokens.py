from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EditorialContentLimits:
    """Centralized limits for committee-grade slide density."""

    max_text_clusters: int = 3
    max_text_chart_clusters: int = 10
    max_text_table_clusters: int = 3
    min_change_rows_n: int = 1
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


@dataclass(frozen=True)
class DimensionChangeSlideLayout:
    """Geometry and table schema for slides 5 and 6."""

    chart_panel: PptRect = PptRect(0.63, 1.42, 12.08, 3.15)
    chart: PptRect = PptRect(1.15, 1.55, 10.70, 2.72)
    table_panel: PptRect = PptRect(0.64, 4.68, 12.05, 2.26)
    table_inner_x: float = 0.60
    table_inner_top: float = 0.32
    header_height: float = 0.42
    row_height: float = 0.44
    max_rows: int = 4
    headers: tuple[str, ...] = (
        "Valor",
        "Delta Score",
        "Score actual",
        "Score base",
        "n actual",
        "n base",
    )
    width_ratios: tuple[float, ...] = (2.80, 1.05, 1.30, 1.30, 1.05, 1.05)


@dataclass(frozen=True)
class JourneySummarySlideLayout:
    """Geometry and wrapped-table tuning for slide 12."""

    chart_panel: PptRect = PptRect(0.66, 2.72, 5.58, 4.18)
    chart: PptRect = PptRect(0.86, 2.92, 5.14, 3.76)
    table_panel_left: float = 6.52
    table_panel_top: float = 2.72
    table_panel_width: float = 6.16
    table_max_rows_height: float = 2.05
    wrapped_min_row_height: float = 0.55
    wrapped_header_height: float = 0.70
    wrapped_title_pad: float = 0.62
    max_rows: int = 6
    headers: tuple[str, ...] = (
        "Journey de detracción",
        "Touchpoint del catálogo",
        "Palanca",
        "Subpalanca",
        "Tópico NPS ancla",
    )
    width_ratios: tuple[float, ...] = (1.25, 1.30, 0.95, 1.25, 1.35)


@dataclass(frozen=True)
class ExecutiveTableStyle:
    panel_border: str = "D5DCE3"
    header_fill: str = "F2F4F8"


@dataclass(frozen=True)
class EditorialCopy:
    nps_block_eyebrow: str = "Bloque 1 · Analisis VoC"
    nps_highlights_title: str = "Highlights del periodo"
    causal_block_eyebrow: str = "Analisis VoC junto a las incidencias Helix reportada en el periodo"
    causal_title_prefix: str = "11. Análisis causal empleado"
    scenario_summary_title: str = "Sumario del análisis del escenario"
    incident_examples_title: str = "Ejemplos de incidencias en el caso de uso"
    linked_comments_examples_title: str = "Ejemplos de Comentarios enlazados"


EDITORIAL_LIMITS = EditorialContentLimits()
EDITORIAL_LAYOUT = EditorialLayout()
CHANGE_SLIDE_LAYOUT = DimensionChangeSlideLayout()
JOURNEY_SUMMARY_LAYOUT = JourneySummarySlideLayout()
EXECUTIVE_TABLE_STYLE = ExecutiveTableStyle()
EDITORIAL_COPY = EditorialCopy()


SLIDE_TITLES: tuple[str, ...] = (
    "NPS Lens",
    "NPS térmico",
    "Evolución del NPS clásico del periodo",
    "Qué han dicho los clientes",
    "Qué ha cambiado en Palanca",
    "Qué ha cambiado en Subpalanca",
    "Dónde duele en Web · Palanca",
    "Dónde duele en Web · Subpalanca",
    "Oportunidades priorizadas · Palanca",
    "Oportunidades priorizadas · Subpalanca",
    "Análisis causal empleado",
    "Journeys de detracción",
    "Análisis causal editorial",
    "Detalle de evidencias Helix",
)
