from __future__ import annotations

from dataclasses import dataclass

TOUCHPOINT_SOURCE_PALANCA = "palanca_touchpoint"
TOUCHPOINT_SOURCE_SUBPALANCA = "domain_touchpoint"
TOUCHPOINT_SOURCE_DOMAIN = TOUCHPOINT_SOURCE_SUBPALANCA
TOUCHPOINT_SOURCE_BBVA_SOURCE_N2 = "bbva_source_service_n2"
TOUCHPOINT_SOURCE_BROKEN_JOURNEYS = "broken_journeys"
TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS = "executive_journeys"


@dataclass(frozen=True)
class CausalMethodSpec:
    value: str
    label: str
    summary: str
    flow: str
    entity_singular: str
    entity_plural: str
    navigation_label: str
    navigation_title: str
    navigation_subtitle: str
    chart_title: str
    table_title: str
    table_empty_message: str
    situation_subtitle: str
    situation_note: str
    deep_dive_subtitle: str


CAUSAL_METHOD_SPECS = {
    TOUCHPOINT_SOURCE_PALANCA: CausalMethodSpec(
        value=TOUCHPOINT_SOURCE_PALANCA,
        label="Por Palanca",
        summary=(
            "La evidencia se agrupa por palanca y muestra los touchpoints, comentarios "
            "e incidencias relacionados."
        ),
        flow="Incidencias -> Touchpoint afectado -> Palanca -> Comentario -> NPS",
        entity_singular="Palanca",
        entity_plural="Palancas",
        navigation_label="Touchpoints afectados por Palanca",
        navigation_title="Touchpoints afectados por Palanca",
        navigation_subtitle=(
            "Cada escenario resume una palanca y los vínculos observados en sus touchpoints."
        ),
        chart_title="Palancas con más touchpoints afectados",
        table_title="Detalle de touchpoints afectados por Palanca",
        table_empty_message="No hay palancas con vínculos semánticos en esta ventana.",
        situation_subtitle=("Cruce diario entre incidencias y NPS organizado por palanca."),
        situation_note=(
            "La vista relaciona incidencias, touchpoints, palancas y comentarios de cliente."
        ),
        deep_dive_subtitle=(
            "Profundización sobre los tópicos NPS asociados a las palancas activas."
        ),
    ),
    TOUCHPOINT_SOURCE_SUBPALANCA: CausalMethodSpec(
        value=TOUCHPOINT_SOURCE_SUBPALANCA,
        label="Por Subpalanca",
        summary=(
            "La evidencia se agrupa por subpalanca y muestra los touchpoints, comentarios "
            "e incidencias relacionados."
        ),
        flow="Incidencias -> Touchpoint afectado -> Subpalanca -> Comentario -> NPS",
        entity_singular="Subpalanca",
        entity_plural="Subpalancas",
        navigation_label="Touchpoints afectados por Subpalanca",
        navigation_title="Touchpoints afectados por Subpalanca",
        navigation_subtitle=(
            "Cada escenario resume una subpalanca y los vínculos observados en su touchpoint."
        ),
        chart_title="Subpalancas con más touchpoints afectados",
        table_title="Detalle de touchpoints afectados por Subpalanca",
        table_empty_message=("No hay subpalancas con vínculos semánticos en esta ventana."),
        situation_subtitle=("Cruce diario entre incidencias y NPS organizado por subpalanca."),
        situation_note=("La vista conserva el nivel operativo de la subpalanca."),
        deep_dive_subtitle=(
            "Profundización sobre los tópicos NPS asociados a las subpalancas activas."
        ),
    ),
    TOUCHPOINT_SOURCE_BBVA_SOURCE_N2: CausalMethodSpec(
        value=TOUCHPOINT_SOURCE_BBVA_SOURCE_N2,
        label="Helix: Source Service N2",
        summary=(
            "La evidencia se agrupa por BBVA_SourceServiceN2 y conserva sus vínculos con "
            "comentarios y tópicos NPS."
        ),
        flow="Incidencias -> Helix Source N2 -> Comentario -> NPS",
        entity_singular="Source Service N2 de Hélix",
        entity_plural="Sources Service N2 de Hélix",
        navigation_label="Incidencias por Source Service N2 de Hélix",
        navigation_title="Incidencias por Source Service N2 de Hélix",
        navigation_subtitle=(
            "Cada escenario resume un Source Service N2 de Hélix y sus vínculos observados."
        ),
        chart_title="Source Service N2 de Hélix con más incidencias relacionadas",
        table_title="Detalle por Source Service N2 de Hélix",
        table_empty_message=(
            "No hay incidencias relacionadas agrupables por Source Service N2 de Hélix en esta ventana."
        ),
        situation_subtitle=(
            "Cruce diario entre incidencias y NPS organizado por Source Service N2."
        ),
        situation_note=("La vista usa el servicio origen reportado en Hélix como eje de lectura."),
        deep_dive_subtitle=(
            "Profundización sobre los tópicos NPS asociados a los Source Service N2 activos."
        ),
    ),
    TOUCHPOINT_SOURCE_BROKEN_JOURNEYS: CausalMethodSpec(
        value=TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
        label="Journeys rotos",
        summary=(
            "La vista agrupa journeys observados a partir de incidencias, comentarios, "
            "embeddings, keywords y clustering semántico antes de llegar al NPS."
        ),
        flow=(
            "Incidencias + comentarios + embeddings + keywords + clustering semántico -> "
            "Journey roto -> Touchpoint detectado -> NPS"
        ),
        entity_singular="Journey roto",
        entity_plural="Journeys rotos",
        navigation_label="Journeys rotos",
        navigation_title="Journeys rotos",
        navigation_subtitle=(
            "Cada escenario resume un journey roto detectado automáticamente y el touchpoint donde converge la evidencia."
        ),
        chart_title="Journeys rotos con más vínculos semánticos",
        table_title="Detalle de journeys rotos detectados",
        table_empty_message="No se han agrupado journeys rotos en esta ventana.",
        situation_subtitle=("Cruce diario entre incidencias y NPS organizado por journeys rotos."),
        situation_note=("La vista agrupa señales semánticas coincidentes por journey roto."),
        deep_dive_subtitle=(
            "Profundización sobre los tópicos NPS asociados a los journeys rotos activos."
        ),
    ),
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS: CausalMethodSpec(
        value=TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
        label="Journeys de detracción",
        summary=(
            "La vista organiza incidencias y comentarios vinculados mediante el catálogo "
            "de journeys ejecutivos de detracción."
        ),
        flow=(
            "Incidencias + comentarios + tópico NPS -> Journey ejecutivo del catálogo -> "
            "Touchpoint / Palanca / Subpalanca -> NPS"
        ),
        entity_singular="Journey de detracción",
        entity_plural="Journeys de detracción",
        navigation_label="Journeys de detracción",
        navigation_title="Journeys de detracción",
        navigation_subtitle=(
            "Cada escenario resume un journey del catálogo y su evidencia NPS y Helix."
        ),
        chart_title="Vínculos semánticos por journey",
        table_title="Detalle de journeys de detracción",
        table_empty_message=(
            "No hay journeys de detracción con vínculos semánticos en esta ventana."
        ),
        situation_subtitle=(
            "Cruce diario entre incidencias y NPS organizado por journeys de detracción."
        ),
        situation_note=(
            "La vista organiza la evidencia mediante el catálogo de journeys ejecutivos."
        ),
        deep_dive_subtitle=(
            "Profundización sobre los tópicos NPS asociados a los journeys de detracción activos."
        ),
    ),
}

TOUCHPOINT_MODE_OPTIONS = tuple(CAUSAL_METHOD_SPECS.keys())
TOUCHPOINT_MODE_MENU_LABELS = {key: spec.label for key, spec in CAUSAL_METHOD_SPECS.items()}
TOUCHPOINT_MODE_CONTEXT_LABELS = {key: spec.label for key, spec in CAUSAL_METHOD_SPECS.items()}
TOUCHPOINT_MODE_BANNER_LABELS = {key: spec.label for key, spec in CAUSAL_METHOD_SPECS.items()}
TOUCHPOINT_MODE_SUMMARIES = {key: spec.summary for key, spec in CAUSAL_METHOD_SPECS.items()}
TOUCHPOINT_MODE_FLOWS = {key: spec.flow for key, spec in CAUSAL_METHOD_SPECS.items()}


def get_causal_method_spec(value: str) -> CausalMethodSpec:
    key = str(value or TOUCHPOINT_SOURCE_SUBPALANCA).strip()
    return CAUSAL_METHOD_SPECS.get(key, CAUSAL_METHOD_SPECS[TOUCHPOINT_SOURCE_SUBPALANCA])


def causal_method_options() -> list[dict[str, str]]:
    return [
        {
            "value": spec.value,
            "label": spec.label,
            "summary": spec.summary,
            "flow": spec.flow,
        }
        for spec in CAUSAL_METHOD_SPECS.values()
    ]


def linking_navigation(spec: CausalMethodSpec) -> list[dict[str, str]]:
    return [
        {"id": "situation", "label": "Situación del periodo"},
        {"id": "entity-summary", "label": spec.navigation_label},
        {"id": "scenarios", "label": "Evidencia por escenario"},
        {"id": "nps-deep-dive", "label": "Análisis de Tópicos de NPS afectados"},
    ]
