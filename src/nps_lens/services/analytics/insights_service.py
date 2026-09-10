from __future__ import annotations


def daily_nps_explanation(
    period_kpis: dict[str, object] | None,
) -> list[str]:
    """Explain aggregate and boundary NPS from the canonical period KPI payload."""
    if not period_kpis:
        return []
    fallback = [
        "NPS clásico = promotores menos detractores; se usa para seguir la señal neta del periodo."
    ]
    display = period_kpis.get("display", {})
    temporal = period_kpis.get("temporal", {})
    if not isinstance(display, dict) or not isinstance(temporal, dict):
        return fallback
    base_display = temporal.get("base_display", {})
    actual_display = temporal.get("display", {})
    if not isinstance(base_display, dict) or not isinstance(actual_display, dict):
        return fallback
    period_label = str(period_kpis.get("label") or "el periodo")
    start_label = str(temporal.get("base_label") or "primer día con respuestas")
    end_label = str(temporal.get("actual_label") or "último día con respuestas")
    return [
        f"El NPS clásico agregado de {period_label} es "
        f"**{display.get('classic_nps', 'n/d')}**.",
        f"En el primer día con respuestas ({start_label}) el NPS clásico es "
        f"**{base_display.get('classic_nps', 'n/d')}**; en el último "
        f"({end_label}) es **{actual_display.get('classic_nps', 'n/d')}**.",
        "El peso detractor pasa de "
        f"**{base_display.get('detractor_rate', 'n/d')}** a "
        f"**{actual_display.get('detractor_rate', 'n/d')}**.",
        fallback[0],
    ]
