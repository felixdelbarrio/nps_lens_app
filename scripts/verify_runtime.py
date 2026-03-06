#!/usr/bin/env python3
from __future__ import annotations

import importlib
from datetime import date
from typing import Callable

import pandas as pd


def _check_import(module_name: str) -> None:
    importlib.import_module(module_name)


def _check_plotly_png() -> None:
    import plotly.graph_objects as go

    from nps_lens.reports.executive_ppt import _kaleido_png  # reuse app workaround

    fig = go.Figure(data=[go.Scatter(x=[1, 2, 3], y=[1, 3, 2], mode="lines+markers")])
    payload = _kaleido_png(fig, width=640, height=360)
    if not isinstance(payload, (bytes, bytearray)) or len(payload) < 100:
        raise RuntimeError("Plotly image export returned an invalid PNG payload.")


def _check_business_ppt() -> None:
    from nps_lens.reports import generate_business_review_ppt

    overall_weekly = pd.DataFrame(
        [
            {"week": "2026-01-05", "focus_rate": 0.19, "incidents": 31, "responses": 820, "focus_count": 156},
            {"week": "2026-01-12", "focus_rate": 0.17, "incidents": 27, "responses": 790, "focus_count": 134},
            {"week": "2026-01-19", "focus_rate": 0.15, "incidents": 22, "responses": 775, "focus_count": 116},
        ]
    )
    rationale_df = pd.DataFrame(
        [
            {
                "nps_topic": "Errores en onboarding digital",
                "priority": 0.91,
                "confidence": 0.84,
                "nps_points_at_risk": 1.8,
                "nps_points_recoverable": 1.2,
                "delta_focus_rate_pp": 2.1,
                "incident_rate_per_100_responses": 4.8,
                "incidents": 39,
                "responses": 1020,
                "action_lane": "Estabilidad",
                "owner_role": "Producto",
                "eta_weeks": 4,
            },
            {
                "nps_topic": "Demoras en soporte",
                "priority": 0.78,
                "confidence": 0.76,
                "nps_points_at_risk": 1.2,
                "nps_points_recoverable": 0.8,
                "delta_focus_rate_pp": 1.5,
                "incident_rate_per_100_responses": 3.4,
                "incidents": 28,
                "responses": 980,
                "action_lane": "Operación",
                "owner_role": "Operaciones",
                "eta_weeks": 6,
            },
        ]
    )
    result = generate_business_review_ppt(
        service_origin="BBVA México",
        service_origin_n1="Senda",
        service_origin_n2="",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        focus_name="detractores",
        overall_weekly=overall_weekly,
        rationale_df=rationale_df,
        nps_points_at_risk=3.0,
        nps_points_recoverable=2.0,
        top3_incident_share=0.62,
        median_lag_weeks=1.8,
        story_md="Riesgo concentrado en onboarding y soporte.",
        script_8slides_md="Slide 1: mensaje principal.\nSlide 2: señal temporal.",
        template_name="Plantilla corporativa fija v1",
        corporate_fixed=True,
        logo_path=None,
    )
    if result.slide_count < 5:
        raise RuntimeError(f"PPT generation returned {result.slide_count} slides (< 5).")
    if not isinstance(result.content, (bytes, bytearray)) or len(result.content) < 1024:
        raise RuntimeError("PPT generation returned an invalid payload.")


def main() -> int:
    checks: list[tuple[str, Callable[[], None]]] = [
        ("import pandas", lambda: _check_import("pandas")),
        ("import numpy", lambda: _check_import("numpy")),
        ("import streamlit", lambda: _check_import("streamlit")),
        ("import plotly", lambda: _check_import("plotly")),
        ("import kaleido", lambda: _check_import("kaleido")),
        ("import python-pptx", lambda: _check_import("pptx")),
        ("import scikit-learn", lambda: _check_import("sklearn")),
        ("import ruptures", lambda: _check_import("ruptures")),
        ("plotly png export", _check_plotly_png),
        ("business ppt generation", _check_business_ppt),
    ]

    failed = False
    print("Runtime dependency smoke-check")
    for label, fn in checks:
        try:
            fn()
            print(f"[OK] {label}")
        except Exception as exc:
            failed = True
            print(f"[FAIL] {label}: {exc}")

    if failed:
        print(
            "\nValidation failed. Re-run `make setup` and verify your active environment/venv."
        )
        return 1

    print("\nAll runtime checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
