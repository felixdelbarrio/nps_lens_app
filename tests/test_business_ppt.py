from __future__ import annotations

import zipfile
from datetime import date
from io import BytesIO
from pathlib import Path

import pandas as pd
from pptx import Presentation

from nps_lens.analytics.incident_attribution import (
    TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
)
from nps_lens.reports import executive_ppt
from nps_lens.reports.content_selectors import (
    parse_markdown_strong,
    select_causal_scenarios,
    select_negative_delta_rows,
    select_nonzero_kpis,
)
from nps_lens.reports.executive_ppt import generate_business_review_ppt
from nps_lens.services.analytics.kpis_service import build_period_kpis
from nps_lens.services.dashboard_service import DashboardService
from nps_lens.settings import Settings
from nps_lens.ui.charts import (
    chart_daily_kpis,
    chart_daily_mix_business,
)
from nps_lens.ui.theme import get_theme


def _sample_payload() -> dict:
    def _nps_group(score: int) -> str:
        if score <= 6:
            return "DETRACTOR"
        if score >= 9:
            return "PROMOTER"
        return "PASSIVE"

    overall_daily = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="D"),
            "nps_mean": [7.3, 7.2, 7.1, 7.4, 7.5, 7.4, 7.2, 7.0] * 5,
            "focus_rate": [0.21, 0.23, 0.24, 0.22, 0.20, 0.19, 0.22, 0.25] * 5,
            "incidents": [4, 6, 8, 5, 4, 3, 7, 9] * 5,
            "responses": [120, 115, 118, 123, 119, 121, 117, 124] * 5,
        }
    )

    by_topic_daily = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="D").tolist() * 3,
            "nps_topic": (["Pagos > SPEI"] * 40)
            + (["Acceso > Login"] * 40)
            + (["Tarjetas > Bloqueo"] * 40),
            "responses": ([44, 46, 45, 43, 44] * 8)
            + ([38, 37, 39, 40, 38] * 8)
            + ([26, 27, 29, 28, 27] * 8),
            "focus_count": ([12, 11, 13, 12, 10] * 8)
            + ([9, 8, 10, 9, 8] * 8)
            + ([5, 4, 5, 5, 4] * 8),
            "nps_mean": ([6.1, 6.3, 6.0, 6.2, 6.4] * 8)
            + ([5.8, 6.0, 5.7, 5.9, 6.1] * 8)
            + ([7.2, 7.1, 7.0, 7.3, 7.2] * 8),
            "focus_rate": ([0.24, 0.23, 0.26, 0.27, 0.25] * 8)
            + ([0.20, 0.19, 0.21, 0.22, 0.20] * 8)
            + ([0.18, 0.17, 0.19, 0.20, 0.18] * 8),
            "incidents": ([2, 3, 4, 2, 1] * 8) + ([1, 2, 2, 1, 0] * 8) + ([1, 1, 2, 1, 1] * 8),
        }
    )

    rationale = pd.DataFrame(
        [
            {
                "nps_topic": "Pagos > SPEI",
                "touchpoint": "Pagos",
                "focus_rate_high_incidence": 0.47,
                "best_lag_weeks": 1.0,
            },
            {
                "nps_topic": "Acceso > Login",
                "touchpoint": "Acceso",
                "focus_rate_high_incidence": 0.39,
                "best_lag_weeks": 1.0,
            },
            {
                "nps_topic": "Tarjetas > Bloqueo",
                "touchpoint": "Tarjetas",
                "focus_rate_high_incidence": 0.31,
                "best_lag_weeks": 2.0,
            },
        ]
    )

    lag_days = pd.DataFrame(
        {
            "nps_topic": ["Pagos > SPEI", "Acceso > Login", "Tarjetas > Bloqueo"],
            "best_lag_days": [4, 3, 5],
        }
    )

    incident_evidence = pd.DataFrame(
        {
            "incident_id": ["INC-9001", "INC-9123", "INC-9200"],
            "incident_date": pd.to_datetime(["2026-01-12", "2026-01-20", "2026-01-28"]),
            "nps_topic": ["Pagos > SPEI", "Acceso > Login", "Tarjetas > Bloqueo"],
            "incident_summary": [
                "Falla intermitente en pagos SPEI de banca móvil.",
                "Error de autenticación al iniciar sesión en app.",
                "Bloqueos recurrentes en activación de tarjeta digital.",
            ],
            "detractor_comment": [
                "No pude transferir y nadie resolvió en soporte.",
                "La app no me deja entrar desde ayer.",
                "Se bloquea la tarjeta y me quedo sin poder pagar.",
            ],
            "similarity": [0.92, 0.88, 0.84],
        }
    )

    changepoints = pd.DataFrame(
        {
            "nps_topic": ["Pagos > SPEI", "Acceso > Login", "Tarjetas > Bloqueo"],
            "changepoints": [
                ["2026-01-10", "2026-01-24"],
                ["2026-01-18"],
                ["2026-01-26"],
            ],
        }
    )

    attribution = pd.DataFrame(
        [
            {
                "nps_topic": "Acceso > Login",
                "touchpoint": "Login",
                "palanca": "Acceso",
                "subpalanca": "Login",
                "linked_incidents": 5,
                "linked_comments": 2,
                "linked_pairs": 5,
                "avg_similarity": 0.89,
                "avg_nps": 1.5,
                "focus_rate_high_incidence": 0.47,
                "incident_rate_per_100_responses": 8.5,
                "incidents": 5,
                "responses": 120,
                "support_organizations": "Producto + Tecnologia",
                "historical_resolution_weeks": 6.0,
                "incident_records": [
                    {
                        "incident_id": "INC00001",
                        "summary": "problema en el login",
                        "url": "https://helix.example/INC00001",
                    },
                    {"incident_id": "INC00003", "summary": "no puedo acceder", "url": ""},
                    {
                        "incident_id": "INC00025",
                        "summary": "nada mas entras se desloguea",
                        "url": "",
                    },
                    {
                        "incident_id": "INC00040",
                        "summary": "error al autenticar usuario en acceso web",
                        "url": "",
                    },
                    {
                        "incident_id": "INC00041",
                        "summary": "falla de sesion al entrar en portal empresas",
                        "url": "",
                    },
                ],
                "incident_examples": [
                    "problema en el login",
                    "no puedo acceder",
                    "nada mas entras se desloguea",
                    "error al autenticar usuario en acceso web",
                    "falla de sesion al entrar en portal empresas",
                ],
                "comment_examples": [
                    "NPS 1: No hay quien entre a la aplicación",
                    "NPS 2: La web expulsa al usuario al entrar",
                ],
                "chain_story": "5 incidencias Helix degradan el touchpoint Login y se reflejan en 2 comentarios VoC con NPS muy bajo.",
            }
        ]
    )

    current_dates = pd.date_range("2026-01-01", periods=40, freq="D")
    baseline_dates = pd.date_range("2025-11-22", periods=40, freq="D")
    current_records: list[dict[str, object]] = []
    baseline_records: list[dict[str, object]] = []
    specs = [
        ("Acceso", "Login", "Web", [2, 3, 4, 5, 4], "La app no me deja entrar"),
        ("Pagos", "SPEI", "BBVA", [4, 5, 6, 5, 4], "Falla al transferir"),
        ("Tarjetas", "Bloqueo", "Otros", [7, 8, 7, 8, 9], "Se bloquea la tarjeta"),
    ]
    baseline_specs = [
        ("Acceso", "Login", "Web", [6, 7, 7, 8, 8], "Accedo sin problema"),
        ("Pagos", "SPEI", "BBVA", [7, 8, 8, 7, 8], "Transferencia completada"),
        ("Tarjetas", "Bloqueo", "Otros", [8, 8, 9, 9, 8], "Tarjeta operativa"),
    ]
    for idx, dt in enumerate(current_dates):
        for topic_idx, (palanca, subpalanca, canal, pattern, comment_base) in enumerate(specs):
            score = int(pattern[idx % len(pattern)])
            current_records.append(
                {
                    "ID": f"C-{idx}-{topic_idx}",
                    "Fecha": dt,
                    "NPS": score,
                    "NPS Group": _nps_group(score),
                    "Canal": canal,
                    "Palanca": palanca,
                    "Subpalanca": subpalanca,
                    "Comment": f"{comment_base} · {dt.date()}",
                }
            )
    for idx, dt in enumerate(baseline_dates):
        for topic_idx, (palanca, subpalanca, canal, pattern, comment_base) in enumerate(
            baseline_specs
        ):
            score = int(pattern[idx % len(pattern)])
            baseline_records.append(
                {
                    "ID": f"B-{idx}-{topic_idx}",
                    "Fecha": dt,
                    "NPS": score,
                    "NPS Group": _nps_group(score),
                    "Canal": canal,
                    "Palanca": palanca,
                    "Subpalanca": subpalanca,
                    "Comment": f"{comment_base} · {dt.date()}",
                }
            )

    selected_nps = pd.DataFrame(current_records)
    comparison_nps = pd.concat([pd.DataFrame(baseline_records), selected_nps], ignore_index=True)
    broken_journeys = pd.DataFrame(
        [
            {
                "journey_label": "Pagos / Transferencias / No funciona bien / falla",
                "touchpoint": "Transferencias",
                "palanca": "Pagos / Transferencias",
                "subpalanca": "No funciona bien / falla",
                "journey_keywords": "falla, transferencia, pago",
                "linked_pairs": 46,
                "linked_incidents": 26,
                "linked_comments": 32,
                "avg_similarity": 0.88,
                "avg_nps": 4.2,
                "semantic_cohesion": 0.87,
            },
            {
                "journey_label": "Uso / Practicidad / Facilidad de uso",
                "touchpoint": "Uso",
                "palanca": "Uso",
                "subpalanca": "Facilidad de uso",
                "journey_keywords": "facil, usar, practicidad",
                "linked_pairs": 5,
                "linked_incidents": 3,
                "linked_comments": 4,
                "avg_similarity": 0.81,
                "avg_nps": 5.0,
                "semantic_cohesion": 0.85,
            },
        ]
    )

    return {
        "overall_daily": overall_daily,
        "by_topic_daily": by_topic_daily,
        "rationale": rationale,
        "lag_days": lag_days,
        "incident_evidence": incident_evidence,
        "changepoints": changepoints,
        "attribution": attribution,
        "selected_nps": selected_nps,
        "comparison_nps": comparison_nps,
        "broken_journeys": broken_journeys,
    }


def _assert_no_shape_overflow(prs: Presentation) -> None:
    tolerance = 1000
    for slide_index, slide in enumerate(prs.slides):
        for shape in slide.shapes:
            if getattr(shape, "left", None) is None:
                continue
            assert shape.left >= -tolerance, f"shape overflow left on slide {slide_index}"
            assert shape.top >= -tolerance, f"shape overflow top on slide {slide_index}"
            assert (
                shape.left + shape.width <= prs.slide_width + tolerance
            ), f"shape overflow right on slide {slide_index}"
            assert (
                shape.top + shape.height <= prs.slide_height + tolerance
            ), f"shape overflow bottom on slide {slide_index}"


def test_generate_business_review_ppt_builds_new_story() -> None:
    payload = _sample_payload()
    business_story = """# Informe de negocio — NPS Lens

## 1) Qué está pasando
- Muestras: 36,872 · Score medio (0-10): 8.53 · Detractores: 12.7% · Promotores: 72.5%
- Zona de fricción: Agregar funcionalidad · Zona fuerte: FAN

## 2) Cambio vs base de comparación
- Periodo actual: Mes actual (Febrero 2026 · 2026-02-01 → 2026-02-22) (n=20,791)
- Periodo base: Base histórica anterior a Febrero 2026 (2025-11-01 → 2026-01-31) (n=16,081)
- Variación: Delta NPS Clásico -0.18 · Δ detractores +2.5 pp

## 3) Brechas NPS observadas
- Funcionamiento Continuo presenta una brecha de -4,2 puntos frente al total.

## 4) Qué están diciendo (temas de texto)
- Tema #1: fallas de continuidad, caídas y lentitud en procesos críticos.

## 5) Evidencia disponible
- Revisar las series observadas, los vínculos semánticos y los casos asociados.
"""

    out = generate_business_review_ppt(
        service_origin="BBVA México",
        service_origin_n1="Empresas Mobile",
        service_origin_n2="",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        focus_name="detractores",
        overall_weekly=payload["overall_daily"],
        rationale_df=payload["rationale"],
        story_md=business_story,
        script_8slides_md="",
        attribution_df=payload["attribution"],
        ranking_df=payload["rationale"],
        by_topic_daily=payload["by_topic_daily"],
        selected_nps_df=payload["selected_nps"],
        comparison_nps_df=payload["comparison_nps"],
        lag_days_by_topic=payload["lag_days"],
        by_topic_weekly=None,
        lag_weeks_by_topic=None,
        incident_evidence_df=payload["incident_evidence"],
        changepoints_by_topic=payload["changepoints"],
        touchpoint_source="domain_touchpoint",
        entity_summary_df=payload["attribution"],
        entity_summary_kpis=[
            {"label": "Subpalancas activas", "value": "1"},
            {"label": "Similitud media", "value": "0.82"},
            {"label": "Vínculos semánticos", "value": "5"},
        ],
    )

    assert out.content
    assert out.file_name.endswith(".pptx")
    assert out.slide_count == 7

    prs = Presentation(BytesIO(out.content))
    assert out.file_name.startswith("nps-comentarios-incidencias-")
    assert "thermal-causality-v3" in (prs.core_properties.keywords or "")
    assert len(prs.slides) == out.slide_count
    _assert_no_shape_overflow(prs)

    texts = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    texts.append(paragraph.text or "")

    cover_texts = []
    for shape in prs.slides[0].shapes:
        if getattr(shape, "has_text_frame", False):
            for paragraph in shape.text_frame.paragraphs:
                cover_texts.append(paragraph.text or "")

    assert any("NPS : Comentarios" in t for t in cover_texts)
    assert any("Método de agrupación:" in t for t in cover_texts)
    assert any("NPS" in t for t in texts)
    assert any("todo el histórico" in t for t in texts)
    assert any("detractores hacen visible" in t for t in texts)
    assert any("lidera el deterioro frente a la base" in t for t in texts)
    assert not any("Qué ha cambiado en Subpalanca" in t for t in texts)
    assert any("concentra el mayor dolor en la Web" in t for t in texts)
    assert not any("Dónde duele en la Web · Subpalanca" in t for t in texts)
    assert not any("oportunidades combinan impacto potencial" in t for t in texts)
    assert not any("Oportunidades priorizadas · Subpalanca" in t for t in texts)
    assert any("Evidencia para el tópico NPS: Acceso > Login" in t for t in texts)
    assert any("Delta NPS Clásico" in t for t in texts)
    assert not any("Lectura ejecutiva" in t for t in texts)
    assert not any("Criterio de recorte" in t for t in texts)
    assert not any("Mapa de dolor Web por Palanca" in t for t in texts)
    assert not any("Mapa de dolor Web por Subpalanca" in t for t in texts)
    assert not any("Impacto estimado en Palanca" in t for t in texts)
    assert not any("Impacto estimado en Subpalanca" in t for t in texts)
    assert not any("Journeys de detracción con mayor evidencia validada" in t for t in texts)
    assert not any("Detalle de evidencias Helix" in t for t in texts)
    assert not any("Qué destaca" in t for t in texts)
    assert not any("2. Cuándo y cómo lo dicen" in t for t in texts)
    assert not any("5. Casos más alejados del promedio" in t for t in texts)
    assert not any("Situación del periodo" in t for t in texts)
    assert not any("10.1 Matriz visual" in t for t in texts)
    assert not any("11.1 Señal temporal" in t for t in texts)
    assert not any("**" in t for t in texts)
    assert not any("Fix estructural" in t for t in texts)
    assert any("problema en el login" in t for t in texts)
    assert any("No hay quien entre a la aplicación" in t for t in texts)
    assert not any("Muestras" in t for t in cover_texts)
    assert out.compact_file_name.endswith("-sin-evolucion-nps.pptx")
    assert len(Presentation(BytesIO(out.compact_content)).slides) == out.slide_count - 2
    with zipfile.ZipFile(BytesIO(out.content)) as archive:
        rels = archive.read("ppt/slides/_rels/slide7.xml.rels").decode("utf-8")
    assert "https://helix.example/INC00001" in rels


def test_generate_business_review_ppt_sanitizes_file_name_for_disk_write() -> None:
    payload = _sample_payload()
    out = generate_business_review_ppt(
        service_origin="MX/BU",
        service_origin_n1="Movil:Empresas?",
        service_origin_n2="",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        focus_name="detractores",
        overall_weekly=payload["overall_daily"],
        rationale_df=payload["rationale"],
        story_md="",
        script_8slides_md="",
        attribution_df=payload["attribution"],
        by_topic_daily=payload["by_topic_daily"],
        selected_nps_df=payload["selected_nps"],
        comparison_nps_df=payload["comparison_nps"],
    )

    assert "/" not in out.file_name
    assert ":" not in out.file_name
    assert "?" not in out.file_name


def test_generate_business_review_ppt_can_render_executive_journey_slide() -> None:
    payload = _sample_payload()
    attribution = payload["attribution"].copy()
    attribution.loc[:, "nps_topic"] = [
        "Acceso bloqueado",
    ]
    attribution.loc[:, "touchpoint"] = ["Login / autenticación"]
    attribution.loc[:, "palanca"] = ["Acceso"]
    attribution.loc[:, "subpalanca"] = ["Bloqueo / OTP"]
    attribution.loc[:, "journey_evidence_pattern"] = [
        "Comentarios sobre login + incidencias de autenticación"
    ]
    attribution.loc[:, "presentation_mode"] = [TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS]

    out = generate_business_review_ppt(
        service_origin="BBVA México",
        service_origin_n1="Empresas Mobile",
        service_origin_n2="",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        focus_name="detractores",
        overall_weekly=payload["overall_daily"],
        rationale_df=payload["rationale"],
        story_md="",
        script_8slides_md="",
        attribution_df=attribution,
        ranking_df=payload["rationale"],
        by_topic_daily=payload["by_topic_daily"],
        selected_nps_df=payload["selected_nps"],
        comparison_nps_df=payload["comparison_nps"],
        lag_days_by_topic=payload["lag_days"],
        incident_evidence_df=payload["incident_evidence"],
        changepoints_by_topic=payload["changepoints"],
        touchpoint_source=TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
        entity_summary_df=attribution,
        entity_summary_kpis=[
            {"label": "Journeys de detracción", "value": "1"},
            {"label": "Touchpoints cubiertos", "value": "1"},
            {"label": "Vínculos semánticos", "value": "5"},
        ],
        broken_journeys_df=payload["broken_journeys"],
    )

    prs = Presentation(BytesIO(out.content))
    texts = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    texts.append(paragraph.text or "")

    assert any("Evidencia para el tópico NPS: Acceso bloqueado" in t for t in texts)
    assert any("Acceso bloqueado" in t for t in texts)


def test_generate_business_review_ppt_keeps_three_causal_scenarios_in_compact_deck() -> None:
    payload = _sample_payload()
    base = payload["attribution"].iloc[0].to_dict()
    rows = [
        {
            **base,
            "nps_topic": "Acceso bloqueado",
            "touchpoint": "Login / autenticación",
            "palanca": "Acceso",
            "subpalanca": "Bloqueo / OTP",
            "linked_incidents": 5,
            "linked_comments": 3,
            "linked_pairs": 5,
            "focus_rate_high_incidence": 0.13,
            "presentation_mode": TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
        },
        {
            **base,
            "nps_topic": "Operativa crítica fallida",
            "touchpoint": "Transferencias / pagos / firma",
            "palanca": "Operativa",
            "subpalanca": "Error funcional / timeout",
            "linked_incidents": 8,
            "linked_comments": 5,
            "linked_pairs": 10,
            "focus_rate_high_incidence": 0.45,
            "incident_records": [
                {
                    "incident_id": "INC000104256298",
                    "summary": "Condición de horario en proceso KNJCR2UC.",
                    "url": "https://helix.example/INC000104256298",
                },
                {
                    "incident_id": "INC000104257175",
                    "summary": "Caída en contratación de seguro cibernético Web.",
                    "url": "https://helix.example/INC000104257175",
                },
                {
                    "incident_id": "",
                    "summary": "Al ingresar al módulo de transferencias en tiempo real seguimiento, el sistema no muestra la opción de firmar las operaciones.",
                    "url": "",
                },
                {
                    "incident_id": "",
                    "summary": "Al querer obtener los comprobantes de pago del 16/01/2026, el sistema se queda cargando.",
                    "url": "",
                },
            ],
            "comment_examples": [
                "NPS 0: no me deja hacer trasnferencias , marca error de horario",
                "NPS 2: no me avisaron de que mi contrato de seguro subio de precio",
            ],
            "presentation_mode": TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
        },
        {
            **base,
            "nps_topic": "Rendimiento degradado",
            "touchpoint": "Lentitud / cuelgues",
            "palanca": "Uso recurrente",
            "subpalanca": "Degradación del servicio",
            "linked_incidents": 2,
            "linked_comments": 2,
            "linked_pairs": 2,
            "focus_rate_high_incidence": float("nan"),
            "incident_records": [
                {
                    "incident_id": "",
                    "summary": "Se detecta que al abrir el PDF del EDC del mes de febrero se muestra un error.",
                    "url": "",
                },
                {
                    "incident_id": "",
                    "summary": "Se procede a obtener un estado de cuenta CFDI y el flujo no completa.",
                    "url": "",
                },
            ],
            "presentation_mode": TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
        },
    ]
    attribution = pd.DataFrame(rows)

    out = generate_business_review_ppt(
        service_origin="BBVA México",
        service_origin_n1="ENTERPRISE WEB",
        service_origin_n2="",
        period_start=date(2026, 3, 1),
        period_end=date(2026, 3, 29),
        focus_name="detractores",
        overall_weekly=payload["overall_daily"],
        rationale_df=payload["rationale"],
        story_md="",
        script_8slides_md="",
        attribution_df=attribution,
        by_topic_daily=payload["by_topic_daily"],
        selected_nps_df=payload["selected_nps"],
        comparison_nps_df=payload["comparison_nps"],
        touchpoint_source=TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
        entity_summary_df=attribution,
        entity_summary_kpis=[
            {"label": "Journeys de detracción", "value": "3"},
            {"label": "Touchpoints cubiertos", "value": "3"},
            {"label": "Vínculos semánticos", "value": "17"},
        ],
    )

    prs = Presentation(BytesIO(out.content))
    texts = [
        paragraph.text or ""
        for slide in prs.slides
        for shape in slide.shapes
        if getattr(shape, "has_text_frame", False)
        for paragraph in shape.text_frame.paragraphs
    ]

    assert out.slide_count == 9
    compact_prs = Presentation(BytesIO(out.compact_content))
    assert len(compact_prs.slides) == 7
    compact_texts = [
        paragraph.text or ""
        for slide in compact_prs.slides
        for shape in slide.shapes
        if getattr(shape, "has_text_frame", False)
        for paragraph in shape.text_frame.paragraphs
    ]
    assert any(
        "Evidencia para el tópico NPS: Operativa crítica fallida" in t for t in compact_texts
    )
    assert any("Evidencia para el tópico NPS: Operativa crítica fallida" in t for t in texts)
    assert any("Evidencia para el tópico NPS: Acceso bloqueado" in t for t in texts)
    assert any("Evidencia para el tópico NPS: Rendimiento degradado" in t for t in texts)
    assert not any("14.1" in t or "14.2" in t or "14.3" in t for t in texts)
    slide_9_texts = [
        paragraph.text or ""
        for shape in prs.slides[8].shapes
        if getattr(shape, "has_text_frame", False)
        for paragraph in shape.text_frame.paragraphs
    ]
    assert not any("NPS EN RIESGO" in t or "NPS RECUPERABLE" in t for t in slide_9_texts)
    assert any("INC000104257175" in t for t in texts)
    assert any("VÍNCULOS SEMÁNTICOS" in t for t in texts)
    with zipfile.ZipFile(BytesIO(out.content)) as archive:
        rels = "".join(
            archive.read(f"ppt/slides/_rels/slide{index}.xml.rels").decode("utf-8")
            for index in range(7, 10)
        )
    assert "https://helix.example/INC000104257175" in rels


def test_generate_business_review_ppt_can_render_broken_journey_story() -> None:
    payload = _sample_payload()
    attribution = payload["attribution"].copy()
    attribution.loc[:, "nps_topic"] = ["Acceso / Login"]
    attribution.loc[:, "touchpoint"] = ["Login"]
    attribution.loc[:, "palanca"] = ["Acceso"]
    attribution.loc[:, "subpalanca"] = ["Login"]
    attribution.loc[:, "journey_route"] = [
        "Incidencia -> Login -> Acceso / Login -> comentario VoC -> NPS"
    ]
    attribution.loc[:, "journey_evidence_pattern"] = [
        "Keywords semánticas: Login, Otp. Helix Source Service N2 dominante: Auth."
    ]
    attribution.loc[:, "journey_cx_readout"] = ["5 links Helix↔VoC convergen en este journey roto."]
    attribution.loc[:, "presentation_mode"] = [TOUCHPOINT_SOURCE_BROKEN_JOURNEYS]

    out = generate_business_review_ppt(
        service_origin="BBVA México",
        service_origin_n1="Empresas Mobile",
        service_origin_n2="",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        focus_name="detractores",
        overall_weekly=payload["overall_daily"],
        rationale_df=payload["rationale"],
        story_md="",
        script_8slides_md="",
        attribution_df=attribution,
        ranking_df=payload["rationale"],
        by_topic_daily=payload["by_topic_daily"],
        selected_nps_df=payload["selected_nps"],
        comparison_nps_df=payload["comparison_nps"],
        lag_days_by_topic=payload["lag_days"],
        incident_evidence_df=payload["incident_evidence"],
        changepoints_by_topic=payload["changepoints"],
        touchpoint_source=TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
        entity_summary_df=attribution,
        entity_summary_kpis=[
            {"label": "Journeys rotos", "value": "1"},
            {"label": "Touchpoints detectados", "value": "1"},
            {"label": "Vínculos semánticos", "value": "5"},
        ],
        broken_journeys_df=payload["broken_journeys"],
    )

    prs = Presentation(BytesIO(out.content))
    texts = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    texts.append(paragraph.text or "")

    assert any("Evidencia para el tópico NPS: Acceso / Login" in t for t in texts)
    assert any("Acceso / Login" in t for t in texts)


def test_ppt_analytics_helpers_build_dynamic_tables() -> None:
    payload = _sample_payload()
    current = executive_ppt._coerce_nps_records(payload["selected_nps"])
    compare = executive_ppt._coerce_nps_records(payload["comparison_nps"])
    current_period, baseline_period = executive_ppt._split_period_frames(
        compare,
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
    )

    overview = executive_ppt._period_overview(current)
    assert int(overview["comments"]) > 0
    assert float(overview["detractor_rate"]) > 0

    palanca_change = executive_ppt._driver_change_table(
        current_period,
        baseline_period,
        dimension="Palanca",
    )
    assert not palanca_change.empty
    assert "delta_nps" in palanca_change.columns


def test_overview_figure_uses_full_history_and_highlights_requested_period() -> None:
    history = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2025-11-01", "2026-01-05", "2026-01-31"]),
            "NPS": [9, 2, 8],
        }
    )
    figure = executive_ppt._build_overview_figure(
        history,
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
    )

    assert figure is not None
    all_dates = [
        pd.Timestamp(value)
        for trace in figure.data
        for value in (list(trace.x) if trace.x is not None else [])
    ]
    assert min(all_dates) == pd.Timestamp("2025-11-01")
    assert any(
        pd.Timestamp(shape.x0) == pd.Timestamp("2026-01-01")
        and pd.Timestamp(shape.x1) == pd.Timestamp("2026-01-31")
        for shape in figure.layout.shapes
    )


def test_ppt_period_overview_reuses_period_kpis_payload_values() -> None:
    raw = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-02-02"]),
            "date": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-02-02"]),
            "NPS": [0, 10, 10],
            "Comment": ["base", "actual", "actual"],
        }
    )
    period_kpis = build_period_kpis(
        history_df=raw,
        current_df=raw[raw["date"].dt.month == 2],
        pop_year="2026",
        pop_month="02",
        context_label="Febrero 2026",
    )

    overview = executive_ppt._period_overview(
        raw[raw["date"].dt.month == 1],
        period_kpis=period_kpis,
    )

    assert overview["comments"] == period_kpis["period"]["kpis"]["comments"]
    assert overview["classic_nps"] == period_kpis["period"]["kpis"]["classic_nps"]
    assert overview["promoter_rate"] == period_kpis["period"]["kpis"]["promoter_rate"]


def test_editorial_content_selectors_are_deterministic_and_hide_zero_kpis() -> None:
    delta_df = pd.DataFrame(
        {
            "value": ["Mejora", "Peor A", "Peor B", "Neutro"],
            "delta_nps": [4.0, -8.0, -8.0, 0.0],
            "n_current": [100, 20, 80, 50],
        }
    )

    selected = select_negative_delta_rows(delta_df, max_rows=2)
    assert selected["value"].tolist() == ["Peor B", "Peor A"]
    assert select_negative_delta_rows(
        pd.DataFrame({"value": ["Mejora"], "delta_nps": [1.0], "n_current": [100]}),
        max_rows=2,
    ).empty

    kpis = select_nonzero_kpis(
        [
            ("Cero", "0 pts", "red"),
            ("Sin dato", "n/d", "blue"),
            ("Con valor", "1,5 pts", "green"),
        ],
        max_items=3,
    )
    assert kpis == [("Con valor", "1,5 pts", "green")]

    segments = parse_markdown_strong("Si mejoramos **Palanca=Acceso**, sube")
    assert [(segment.text, segment.bold) for segment in segments] == [
        ("Si mejoramos ", False),
        ("Palanca=Acceso", True),
        (", sube", False),
    ]

    scenarios = select_causal_scenarios(
        pd.DataFrame(
            [
                {
                    "nps_topic": "Acceso bloqueado",
                    "focus_rate_high_incidence": 0.13,
                    "linked_pairs": 5,
                    "linked_incidents": 5,
                    "linked_comments": 3,
                },
                {
                    "nps_topic": "Operativa crítica fallida",
                    "focus_rate_high_incidence": 0.45,
                    "linked_pairs": 10,
                    "linked_incidents": 8,
                    "linked_comments": 5,
                },
            ]
        ),
        max_rows=2,
    )
    assert scenarios["nps_topic"].tolist() == ["Operativa crítica fallida", "Acceso bloqueado"]


def test_daily_kpis_chart_places_x_axis_labels_at_bottom() -> None:
    payload = _sample_payload()

    fig = chart_daily_kpis(payload["selected_nps"], get_theme("light"), days=31)

    assert fig is not None
    assert fig.layout.xaxis.side == "bottom"
    assert fig.layout.xaxis.ticklabelposition == "outside bottom"

    stacked = executive_ppt.chart_daily_nps_committee_stack(
        payload["selected_nps"], get_theme("light"), days=31
    )
    assert stacked is not None
    assert len(stacked.data) == 5
    assert [trace.name for trace in stacked.data] == [
        "NPS clásico",
        "% detractores",
        "% promotores",
        "% pasivos",
        "% detractores",
    ]

    mix = chart_daily_mix_business(payload["selected_nps"], get_theme("light"), days=31)
    assert mix is not None
    assert {trace.type for trace in mix.data} == {"scatter"}
    assert [trace.name for trace in mix.data] == [
        "% Promotores",
        "% Pasivos",
        "% Detractores",
    ]


def test_generate_business_review_ppt_handles_selected_period_without_history_or_chains() -> None:
    payload = _sample_payload()
    out = generate_business_review_ppt(
        service_origin="BBVA México",
        service_origin_n1="Empresas Mobile",
        service_origin_n2="",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        focus_name="detractores",
        overall_weekly=payload["overall_daily"],
        rationale_df=payload["rationale"].head(0),
        story_md="",
        script_8slides_md="",
        attribution_df=pd.DataFrame(),
        ranking_df=pd.DataFrame(),
        by_topic_daily=payload["by_topic_daily"],
        selected_nps_df=payload["selected_nps"],
        comparison_nps_df=pd.DataFrame(),
        lag_days_by_topic=pd.DataFrame(),
        by_topic_weekly=None,
        lag_weeks_by_topic=None,
        incident_evidence_df=pd.DataFrame(),
        changepoints_by_topic=pd.DataFrame(),
    )

    prs = Presentation(BytesIO(out.content))
    texts = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    texts.append(paragraph.text or "")

    assert any("todo el histórico" in t for t in texts)
    assert len(prs.slides) == 6
    assert not any("Causalidad en tópico NPS ancla" in t for t in texts)
    assert not any("7.1" in t for t in texts)


def test_generate_business_review_ppt_can_omit_causal_section_explicitly() -> None:
    payload = _sample_payload()
    out = generate_business_review_ppt(
        service_origin="BBVA México",
        service_origin_n1="Empresas Mobile",
        service_origin_n2="",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        focus_name="detractores",
        overall_weekly=pd.DataFrame(),
        rationale_df=pd.DataFrame(),
        story_md="",
        script_8slides_md="",
        attribution_df=pd.DataFrame(),
        selected_nps_df=payload["selected_nps"],
        comparison_nps_df=payload["comparison_nps"],
        include_causal_section=False,
    )

    prs = Presentation(BytesIO(out.content))
    texts = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    texts.append(paragraph.text or "")

    assert out.slide_count > 0
    assert not any("Journeys de detracción" in t for t in texts)
    assert not any("evidencia disponible no permite afirmar causalidad" in t for t in texts)


def test_generate_business_review_ppt_falls_back_to_aggregate_signals_without_raw_nps() -> None:
    payload = _sample_payload()
    out = generate_business_review_ppt(
        service_origin="BBVA México",
        service_origin_n1="Empresas Mobile",
        service_origin_n2="",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        focus_name="detractores",
        overall_weekly=payload["overall_daily"],
        rationale_df=pd.DataFrame(),
        story_md="",
        script_8slides_md="",
        attribution_df=pd.DataFrame(),
        ranking_df=pd.DataFrame(),
        by_topic_daily=payload["by_topic_daily"],
        selected_nps_df=None,
        comparison_nps_df=None,
        lag_days_by_topic=None,
        by_topic_weekly=None,
        lag_weeks_by_topic=None,
        incident_evidence_df=None,
        changepoints_by_topic=None,
    )

    prs = Presentation(BytesIO(out.content))
    texts = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    texts.append(paragraph.text or "")

    assert any("todo el histórico" in t for t in texts)
    assert any("detractores hacen visible" in t for t in texts)


def test_text_topic_slide_uses_all_clusters_for_chart_and_top_three_for_table() -> None:
    topics = pd.DataFrame(
        {
            "cluster_id": [1, 2, 3, 4, 5],
            "n": [500, 400, 300, 200, 100],
            "top_terms": [["uno", "dos"]] * 5,
            "examples": [["ejemplo"]] * 5,
            "label": [""] * 5,
            "top_terms_txt": ["uno, dos"] * 5,
            "example_txt": ["ejemplo"] * 5,
        }
    )

    fig = executive_ppt._build_text_topic_figure(topics)
    assert fig is not None
    assert len(fig.data[0].x) == 5

    selected = executive_ppt.select_text_clusters(topics, max_clusters=3)
    assert selected["cluster_id"].tolist() == [1, 2, 3]


def test_dashboard_service_injects_helix_urls_into_incident_records(tmp_path: Path) -> None:
    settings = Settings(
        data_dir=tmp_path / "data",
        database_path=tmp_path / "data" / "dashboard.sqlite3",
        frontend_dist_dir=tmp_path / "frontend-dist",
        frontend_public_dir=tmp_path / "frontend-public",
        api_host="127.0.0.1",
        api_port=8000,
        default_service_origin="BBVA México",
        default_service_origin_n1="Senda",
        allowed_service_origins=["BBVA México"],
        allowed_service_origin_n1={"BBVA México": ["Senda"]},
        log_level="INFO",
    )
    service = DashboardService(repository=object(), settings=settings)  # type: ignore[arg-type]
    chain_df = pd.DataFrame(
        {
            "incident_records": [
                [{"incident_id": "INC00042", "summary": "Falla de acceso", "url": ""}]
            ]
        }
    )
    helix_df = pd.DataFrame(
        {
            "Incident Number": ["INC00042"],
            "Record ID": ["AGGADG1A2B3C"],
        }
    )

    out = service._inject_incident_record_urls(chain_df, helix_df=helix_df)
    record = out.iloc[0]["incident_records"][0]
    assert record["url"].endswith("/AGGADG1A2B3C")
    assert record["incident_id__href"] == record["url"]


def test_change_story_keeps_four_negative_rows() -> None:
    df = pd.DataFrame(
        {
            "value": ["B", "A", "C"],
            "delta_nps": [-0.2, -1.1, 0.5],
            "nps_current": [7.0, 6.0, 8.0],
            "nps_baseline": [7.2, 7.1, 7.5],
            "n_current": [100, 80, 30],
            "n_baseline": [120, 95, 40],
        }
    )

    out = select_negative_delta_rows(df, max_rows=2)

    assert out["value"].tolist() == ["A", "B"]


def test_journey_table_exposes_catalog_detail_columns() -> None:
    entity_summary = pd.DataFrame(
        [
            {
                "entity_label": "Operativa crítica fallida",
                "source_nps_topic": "Pagos / Transferencias > Mostrar movimientos actualizados",
                "touchpoint": "Transferencias / pagos / firma",
                "palanca": "Operativa",
                "subpalanca": "Error funcional / timeout",
                "linked_pairs": 16,
                "linked_comments": 13,
                "avg_nps": 2.0,
            }
        ]
    )

    table = executive_ppt._build_journey_table(
        touchpoint_source=TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
        entity_summary_df=entity_summary,
        broken_journeys_df=None,
    )

    assert table.loc[0, "journey"] == "Operativa crítica fallida"
    assert table.loc[0, "palanca"] == "Operativa"
    assert table.loc[0, "anchor_topic"].startswith("Pagos / Transferencias")
    assert {"touchpoint", "subpalanca", "links", "similarity"}.issubset(table.columns)
