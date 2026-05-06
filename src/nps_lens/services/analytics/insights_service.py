from __future__ import annotations

import pandas as pd

from nps_lens.services.analytics.kpis_service import build_period_boundary_kpis


def daily_nps_explanation(
    current_df: pd.DataFrame,
    *,
    temporal_kpis: dict[str, object] | None = None,
) -> list[str]:
    if current_df is None or current_df.empty or "NPS" not in current_df.columns:
        return []
    scores = pd.to_numeric(current_df["NPS"], errors="coerce").dropna()
    if scores.empty:
        return []
    fallback = [
        "NPS clásico = promotores menos detractores; se usa para seguir la señal neta del periodo."
    ]
    if "Fecha" not in current_df.columns:
        return fallback

    temporal = temporal_kpis or build_period_boundary_kpis(current_df)
    base_display = temporal.get("base_display", {})
    actual_display = temporal.get("display", {})
    if not isinstance(base_display, dict) or not isinstance(actual_display, dict):
        return fallback
    return [
        "El periodo arranca con NPS clásico "
        f"**{base_display.get('classic_nps', 'n/d')}** y termina en "
        f"**{actual_display.get('classic_nps', 'n/d')}**.",
        "El peso detractor pasa de "
        f"**{base_display.get('detractor_rate', 'n/d')}** a "
        f"**{actual_display.get('detractor_rate', 'n/d')}**.",
        fallback[0],
    ]
