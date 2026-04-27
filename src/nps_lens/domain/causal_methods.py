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
            "La lectura causal organiza la atribución por palanca y muestra qué touchpoints "
            "quedan afectados antes de llegar al comentario y al NPS."
        ),
        flow="Incidencias -> Touchpoint afectado -> Palanca -> Comentario -> NPS",
        entity_singular="Palanca",
        entity_plural="Palancas",
        navigation_label="Touchpoints afectados por Palanca",
        navigation_title="Touchpoints afectados por Palanca",
        navigation_subtitle=(
            "Cada escenario resume una palanca causal y los touchpoints afectados que están "
            "sosteniendo la fricción observada en NPS."
        ),
        chart_title="Palancas con más touchpoints afectados",
        table_title="Detalle de touchpoints afectados por Palanca",
        table_empty_message="No hay palancas defendibles con touchpoints afectados en esta ventana.",
        situation_subtitle=(
            "Cruce diario entre incidencias y NPS con lectura causal organizada por palanca."
        ),
        situation_note=(
            "El método causal activo interpreta la fricción como una secuencia: incidencia, "
            "touchpoint afectado, palanca y comentario de cliente."
        ),
        deep_dive_subtitle=(
            "Profundización sobre los tópicos NPS explicados por las palancas causales activas."
        ),
    ),
    TOUCHPOINT_SOURCE_SUBPALANCA: CausalMethodSpec(
        value=TOUCHPOINT_SOURCE_SUBPALANCA,
        label="Por Subpalanca",
        summary=(
            "La lectura causal fija la subpalanca como unidad operativa y explica qué "
            "touchpoint queda afectado antes de reflejarse en el comentario y el NPS."
        ),
        flow="Incidencias -> Touchpoint afectado -> Subpalanca -> Comentario -> NPS",
        entity_singular="Subpalanca",
        entity_plural="Subpalancas",
        navigation_label="Touchpoints afectados por Subpalanca",
        navigation_title="Touchpoints afectados por Subpalanca",
        navigation_subtitle=(
            "Cada escenario resume una subpalanca causal y el touchpoint donde se materializa la fricción."
        ),
        chart_title="Subpalancas con más touchpoints afectados",
        table_title="Detalle de touchpoints afectados por Subpalanca",
        table_empty_message=(
            "No hay subpalancas defendibles con touchpoints afectados en esta ventana."
        ),
        situation_subtitle=(
            "Cruce diario entre incidencias y NPS con lectura causal organizada por subpalanca."
        ),
        situation_note=(
            "El método causal activo interpreta la fricción al nivel operativo fino de la subpalanca."
        ),
        deep_dive_subtitle=(
            "Profundización sobre los tópicos NPS explicados por las subpalancas causales activas."
        ),
    ),
    TOUCHPOINT_SOURCE_BBVA_SOURCE_N2: CausalMethodSpec(
        value=TOUCHPOINT_SOURCE_BBVA_SOURCE_N2,
        label="Helix: Source Service N2",
        summary=(
            "La lectura causal se apoya en BBVA_SourceServiceN2 para ordenar el análisis "
            "por servicio origen de Hélix antes de llegar al comentario y al NPS."
        ),
        flow="Incidencias -> Helix Source N2 -> Comentario -> NPS",
        entity_singular="Source Service N2 de Hélix",
        entity_plural="Sources Service N2 de Hélix",
        navigation_label="Incidencias por Source Service N2 de Hélix",
        navigation_title="Incidencias por Source Service N2 de Hélix",
        navigation_subtitle=(
            "Cada escenario resume un Source Service N2 de Hélix y su conexión defendible con la caída de experiencia."
        ),
        chart_title="Source Service N2 de Hélix con más incidencias validadas",
        table_title="Detalle causal por Source Service N2 de Hélix",
        table_empty_message=(
            "No hay incidencias defendibles agrupables por Source Service N2 de Hélix en esta ventana."
        ),
        situation_subtitle=(
            "Cruce diario entre incidencias y NPS con lectura causal organizada por Source Service N2."
        ),
        situation_note=(
            "El método causal activo prioriza el servicio origen reportado en Hélix como eje de lectura."
        ),
        deep_dive_subtitle=(
            "Profundización sobre los tópicos NPS asociados a los Source Service N2 activos."
        ),
    ),
    TOUCHPOINT_SOURCE_BROKEN_JOURNEYS: CausalMethodSpec(
        value=TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
        label="Journeys rotos",
        summary=(
            "La lectura causal detecta journeys rotos a partir de incidencias, comentarios, "
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
        chart_title="Journeys rotos con mayor evidencia validada",
        table_title="Detalle de journeys rotos detectados",
        table_empty_message="No he identificado journeys rotos defendibles en esta ventana.",
        situation_subtitle=(
            "Cruce diario entre incidencias y NPS con lectura causal organizada por journeys rotos."
        ),
        situation_note=(
            "El método causal activo agrupa señales semánticas convergentes para aislar journeys rotos defendibles."
        ),
        deep_dive_subtitle=(
            "Profundización sobre los tópicos NPS explicados por los journeys rotos activos."
        ),
    ),
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS: CausalMethodSpec(
        value=TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
        label="Journeys de detracción",
        summary=(
            "La lectura causal reorganiza la evidencia en journeys ejecutivos de detracción "
            "para explicar dónde se rompe la experiencia y cómo cae el NPS."
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
            "Cada escenario resume un journey ejecutivo del catálogo y la evidencia que sostiene su impacto en NPS."
        ),
        chart_title="Evidencia validada por journey",
        table_title="Detalle de journeys de detracción",
        table_empty_message=(
            "No hay journeys de detracción defendibles con evidencia suficiente en esta ventana."
        ),
        situation_subtitle=(
            "Cruce diario entre incidencias y NPS con lectura causal organizada por journeys de detracción."
        ),
        situation_note=(
            "El método causal activo transforma la evidencia en journeys ejecutivos con foco de comité."
        ),
        deep_dive_subtitle=(
            "Profundización sobre los tópicos NPS explicados por los journeys de detracción activos."
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
        {"id": "scenarios", "label": "Análisis de escenarios causales"},
        {"id": "nps-deep-dive", "label": "Análisis de Tópicos de NPS afectados"},
    ]
