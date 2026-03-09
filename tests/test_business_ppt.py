from __future__ import annotations

from datetime import date
from io import BytesIO

import pandas as pd
from pptx import Presentation

from nps_lens.analytics.incident_attribution import TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS
from nps_lens.reports import executive_ppt
from nps_lens.reports.executive_ppt import generate_business_review_ppt


def _sample_payload() -> dict:
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
                "priority": 0.91,
                "confidence": 0.80,
                "focus_probability_with_incident": 0.47,
                "nps_delta_expected": -4.8,
                "total_nps_impact": 1.9,
                "causal_score": 0.84,
                "nps_points_at_risk": 1.9,
                "nps_points_recoverable": 1.2,
                "best_lag_weeks": 1.0,
            },
            {
                "nps_topic": "Acceso > Login",
                "touchpoint": "Acceso",
                "priority": 0.79,
                "confidence": 0.71,
                "focus_probability_with_incident": 0.39,
                "nps_delta_expected": -3.6,
                "total_nps_impact": 1.1,
                "causal_score": 0.77,
                "nps_points_at_risk": 1.1,
                "nps_points_recoverable": 0.7,
                "best_lag_weeks": 1.0,
            },
            {
                "nps_topic": "Tarjetas > Bloqueo",
                "touchpoint": "Tarjetas",
                "priority": 0.68,
                "confidence": 0.64,
                "focus_probability_with_incident": 0.31,
                "nps_delta_expected": -2.8,
                "total_nps_impact": 0.9,
                "causal_score": 0.66,
                "nps_points_at_risk": 0.9,
                "nps_points_recoverable": 0.5,
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
                "detractor_probability": 0.47,
                "nps_delta_expected": -4.8,
                "total_nps_impact": 1.7,
                "nps_points_at_risk": 1.7,
                "nps_points_recoverable": 1.1,
                "priority": 0.91,
                "confidence": 0.82,
                "causal_score": 0.86,
                "delta_focus_rate_pp": 29.0,
                "incident_rate_per_100_responses": 8.5,
                "incidents": 5,
                "responses": 120,
                "action_lane": "Fix estructural",
                "owner_role": "Producto + Tecnologia",
                "eta_weeks": 6.0,
                "incident_records": [
                    {"incident_id": "INC00001", "summary": "problema en el login", "url": ""},
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

    return {
        "overall_daily": overall_daily,
        "by_topic_daily": by_topic_daily,
        "rationale": rationale,
        "lag_days": lag_days,
        "incident_evidence": incident_evidence,
        "changepoints": changepoints,
        "attribution": attribution,
    }


def test_generate_business_review_ppt_builds_new_story() -> None:
    payload = _sample_payload()
    business_story = """# Informe de negocio — NPS Lens

## 1) Qué está pasando
- Muestras: 36,872 · NPS medio (0-10): 8.53 · Detractores: 12.7% · Promotores: 72.5%
- Zona de fricción: Agregar funcionalidad · Zona fuerte: FAN

## 2) Cambio vs base de comparación
- Periodo actual: Mes actual (Febrero 2026 · 2026-02-01 → 2026-02-22) (n=20,791)
- Periodo base: Base histórica anterior a Febrero 2026 (2025-11-01 → 2026-01-31) (n=16,081)
- Variación: Δ NPS -0.18 · Δ detractores +2.5 pp

## 3) Dónde atacar primero (oportunidades)
- Si mejoramos Palanca=Funcionamiento Continuo, el modelo estima un potencial de +57.2 puntos.

## 4) Qué están diciendo (temas de texto)
- Tema #1: fallas de continuidad, caídas y lentitud en procesos críticos.

## 5) Próximos pasos recomendados
- Validar releases, alinear owners y aterrizar quick wins del mes.
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
        nps_points_at_risk=3.9,
        nps_points_recoverable=2.4,
        top3_incident_share=0.74,
        median_lag_weeks=1.2,
        story_md=business_story,
        script_8slides_md="",
        attribution_df=payload["attribution"],
        ranking_df=payload["rationale"],
        by_topic_daily=payload["by_topic_daily"],
        lag_days_by_topic=payload["lag_days"],
        by_topic_weekly=None,
        lag_weeks_by_topic=None,
        logo_path=None,
        incident_evidence_df=payload["incident_evidence"],
        changepoints_by_topic=payload["changepoints"],
    )

    assert out.content
    assert out.file_name.endswith(".pptx")
    assert out.slide_count == 9

    prs = Presentation(BytesIO(out.content))
    assert len(prs.slides) == 9

    texts = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    texts.append(paragraph.text or "")

    assert any("Informe de negocio" in t for t in texts)
    assert any("Cambio vs base de comparación" in t for t in texts)
    assert any("Periodo actual: Mes actual" in t for t in texts)
    assert any("Periodo base: Base histórica anterior" in t for t in texts)
    assert any("SERVICE ORIGEN" in t for t in texts)
    assert any("NIVEL N1" in t for t in texts)
    assert any("NIVEL N2" in t for t in texts)
    assert any("MES EN CURSO" in t for t in texts)
    assert any("Evolución histórica diaria de NPS e incidencias" in t for t in texts)
    assert any("Marco causal" in t for t in texts)
    assert any("Top 3 hotspots operativos" in t for t in texts)
    assert any("Incidencias históricas diarias por hotspot" in t for t in texts)
    assert any("Tema prioritario 1: Login" in t for t in texts)
    assert any("Evidencia Helix" in t for t in texts)
    assert any("INC00001" in t and "problema en el login" in t for t in texts)
    assert any(
        "INC00041" in t and "falla de sesion al entrar en portal empresas" in t for t in texts
    )
    assert any("No hay quien entre a la aplicación" in t for t in texts)
    assert any("La web expulsa al usuario al entrar" in t for t in texts)
    assert any("Priorización del tema" in t for t in texts)
    assert any("Fix estructural" in t for t in texts)


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
        nps_points_at_risk=3.9,
        nps_points_recoverable=2.4,
        top3_incident_share=0.74,
        median_lag_weeks=1.2,
        story_md="",
        script_8slides_md="",
        attribution_df=payload["attribution"],
        by_topic_daily=payload["by_topic_daily"],
        logo_path=None,
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
    attribution.loc[:, "journey_expected_evidence"] = [
        "Comentarios sobre login + incidencias de autenticación"
    ]
    attribution.loc[:, "journey_impact_label"] = ["Muy alto"]
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
        nps_points_at_risk=3.9,
        nps_points_recoverable=2.4,
        top3_incident_share=0.74,
        median_lag_weeks=1.2,
        story_md="",
        script_8slides_md="",
        attribution_df=attribution,
        ranking_df=payload["rationale"],
        by_topic_daily=payload["by_topic_daily"],
        lag_days_by_topic=payload["lag_days"],
        logo_path=None,
        incident_evidence_df=payload["incident_evidence"],
        changepoints_by_topic=payload["changepoints"],
        touchpoint_source=TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
    )

    prs = Presentation(BytesIO(out.content))
    texts = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    texts.append(paragraph.text or "")

    assert any("Journeys que explican la detracción" in t for t in texts)
    assert any("Valor diferencial de NPS Lens" in t for t in texts)
    assert any("Acceso bloqueado" in t for t in texts)


def test_history_fig_daily_uses_requested_colors() -> None:
    daily = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
            "nps_mean": [7.2, 7.4, 7.1],
            "detractor_rate": [0.22, 0.20, 0.24],
            "incidents": [3, 4, 2],
        }
    )
    fig = executive_ppt._history_fig(daily, focus_name="detractores")
    assert fig is not None
    assert fig.data[0]["line"]["color"] == "#" + executive_ppt.BBVA_COLORS["green"]
    assert fig.data[2]["marker"]["color"] == "#" + executive_ppt.BBVA_COLORS["yellow"]


def test_month_overlap_highlights_matched_incidents_with_labels() -> None:
    month = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
            "nps_mean": [7.2, 7.3, 7.1],
            "detractor_rate": [0.21, 0.22, 0.24],
            "incidents": [4, 5, 2],
        }
    )
    matched = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-01", "2026-01-03"]),
            "matched_incidents": [2, 1],
        }
    )
    fig = executive_ppt._month_overlap_fig(
        month,
        focus_name="detractores",
        matched_daily=matched,
    )
    assert fig is not None
    assert fig.data[3]["marker"]["color"] == "#" + executive_ppt.BBVA_COLORS["orange"]
    assert list(fig.data[3]["text"]) == ["2", "", "1"]


def test_top_hotspots_fig_uses_top3_colors_and_inbar_labels() -> None:
    evidence = pd.DataFrame(
        {
            "hot_rank": [1, 1, 2, 2, 3, 3],
            "hot_term": [
                "pagos",
                "pagos",
                "movimientos",
                "movimientos",
                "transferencias",
                "transferencias",
            ],
            "mention_incidents": [60, 60, 45, 45, 30, 30],
            "mention_comments": [114, 114, 98, 98, 85, 85],
            "hotspot_comments": [114, 114, 98, 98, 85, 85],
            "hotspot_links": [90, 90, 72, 72, 54, 54],
        }
    )
    timeline = pd.DataFrame(
        {
            "incident_id": ["", "", ""],
            "hot_term": ["pagos", "movimientos", "transferencias"],
            "date": pd.to_datetime(["2026-01-01", "2026-01-01", "2026-01-01"]),
            "helix_records": [60, 45, 30],
            "nps_comments": [114, 98, 85],
        }
    )

    fig = executive_ppt._top_hotspots_fig(evidence, timeline, top_k=3)
    assert fig is not None
    assert len(fig.data) == 1

    colors = list(fig.data[0]["marker"]["color"])
    assert colors == [
        "#" + executive_ppt.BBVA_COLORS["yellow"],
        "#" + executive_ppt.BBVA_COLORS["orange"],
        "#" + executive_ppt.BBVA_COLORS["red"],
    ]
    assert fig.layout.xaxis.visible is False
    assert all(str(t).strip().startswith("#") for t in list(fig.data[0]["text"]))


def test_hotspot_matches_by_day_uses_hot_terms_and_overlap_signal() -> None:
    timeline = pd.DataFrame(
        {
            "incident_id": ["", "", "", ""],
            "hot_term": ["transferencias", "transferencias", "login", "login"],
            "date": pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-05", "2026-01-07"]),
            "helix_records": [3, 2, 1, 2],
            "nps_comments": [2, 0, 1, 3],
        }
    )
    evidence = pd.DataFrame(
        {
            "hot_term": ["transferencias", "login"],
            "hot_rank": [1, 2],
        }
    )
    out = executive_ppt._hotspot_matches_by_day(
        timeline,
        evidence,
        month_start=pd.Timestamp("2026-01-01"),
        month_end=pd.Timestamp("2026-01-31"),
    )
    assert list(out["date"].dt.strftime("%Y-%m-%d")) == ["2026-01-05", "2026-01-07"]
    assert list(out["matched_incidents"].astype(int)) == [4, 2]


def test_hotspot_stack_fig_uses_requested_color_semantics_and_horizontal_legend() -> None:
    daily = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "nps_mean": [7.1, 7.2],
            "detractor_rate": [0.2, 0.21],
            "incidents": [5, 6],
        }
    )
    evidence = pd.DataFrame(
        {
            "incident_id": ["INC-1", "INC-2", "INC-3"],
            "incident_date": pd.to_datetime(["2026-01-01", "2026-01-01", "2026-01-02"]),
            "hot_rank": [1, 2, 3],
            "hot_term": ["transferencias", "token", "autenticacion"],
            "similarity": [0.9, 0.8, 0.7],
        }
    )
    fig = executive_ppt._hotspot_stack_fig(daily, evidence)
    assert fig is not None
    assert fig.data[0]["marker"]["color"] == "#" + executive_ppt.BBVA_COLORS["blue"]
    assert fig.data[1]["marker"]["color"] == "#" + executive_ppt.BBVA_COLORS["yellow"]
    assert fig.data[2]["marker"]["color"] == "#" + executive_ppt.BBVA_COLORS["orange"]
    assert fig.data[3]["marker"]["color"] == "#" + executive_ppt.BBVA_COLORS["red"]
    assert fig.layout.legend.orientation == "h"


def test_incident_related_timeline_keeps_only_days_with_related_evidence() -> None:
    timeline = pd.DataFrame(
        {
            "incident_id": ["INC-9001", "INC-9001", "INC-9001", "INC-9002"],
            "date": pd.to_datetime(["2026-01-10", "2026-01-11", "2026-01-12", "2026-01-10"]),
            "helix_records": [1, 0, 2, 1],
            "nps_comments": [0, 3, 0, 1],
        }
    )
    out = executive_ppt._incident_related_timeline(
        incident_id="INC-9001",
        incident_timeline_df=timeline,
    )
    assert not out.empty
    assert list(out["date"].dt.strftime("%Y-%m-%d")) == ["2026-01-10", "2026-01-11", "2026-01-12"]
    assert out["helix_records"].sum() == 3
    assert out["nps_comments"].sum() == 3


def test_prepare_incident_evidence_prefers_detailed_description_rowwise() -> None:
    raw = pd.DataFrame(
        {
            "incident_id": ["INC-1", "INC-2"],
            "summary": ["Resumen muy corto", "Resumen fallback"],
            "Detailed Description": ["Detalle amplio 1", ""],
            "detractor_comment": ["Comentario 1", "Comentario 2"],
            "similarity": [0.81, 0.62],
        }
    )
    out = executive_ppt._prepare_incident_evidence(raw)
    assert out.iloc[0]["incident_summary"] == "Detalle amplio 1"
    assert out.iloc[1]["incident_summary"] == "Resumen fallback"


def test_select_zoom_incidents_groups_hotspots_instead_of_single_1to1() -> None:
    evidence = pd.DataFrame(
        {
            "incident_id": ["INC-A1", "INC-A2", "INC-B1"],
            "incident_date": pd.to_datetime(["2026-01-10", "2026-01-11", "2026-01-13"]),
            "nps_topic": ["Pagos > Transferencias", "Pagos > Transferencias", "Acceso > Login"],
            "incident_summary": ["Fallo 1", "Fallo 2", "Fallo 3"],
            "detractor_comment": ["No transfiere", "Transferencia falla", "No puedo entrar"],
            "similarity": [0.91, 0.87, 0.80],
            "hot_term": ["transferencias", "transferencias", "login"],
            "hot_rank": [1, 1, 2],
            "hotspot_incidents": [8, 8, 3],
            "hotspot_comments": [21, 21, 7],
            "hotspot_links": [13, 13, 5],
        }
    )
    selected = executive_ppt._select_zoom_incidents([], evidence, max_items=2)
    assert len(selected) == 2
    assert selected[0].hot_term == "transferencias"
    assert selected[0].hotspot_incidents == 8
    assert selected[0].hotspot_comments == 21
    assert selected[0].hotspot_links == 13
    assert "INC-A1" in selected[0].sample_incidents
    assert "INC-A2" in selected[0].sample_incidents


def test_incident_related_timeline_can_aggregate_by_hotspot_term() -> None:
    timeline = pd.DataFrame(
        {
            "incident_id": ["INC-A1", "INC-A2", "INC-B1"],
            "hot_term": ["transferencias", "transferencias", "login"],
            "date": pd.to_datetime(["2026-01-10", "2026-01-10", "2026-01-11"]),
            "helix_records": [2, 1, 1],
            "nps_comments": [1, 3, 0],
            "nps_comments_moderate": [1, 1, 0],
            "nps_comments_high": [0, 2, 0],
            "nps_comments_critical": [0, 0, 0],
        }
    )
    out = executive_ppt._incident_related_timeline(
        incident_id="INC-A1",
        hot_term="transferencias",
        incident_timeline_df=timeline,
    )
    assert len(out) == 1
    assert out.iloc[0]["helix_records"] == 3
    assert out.iloc[0]["nps_comments"] == 4
    assert out.iloc[0]["nps_comments_moderate"] == 2
    assert out.iloc[0]["nps_comments_high"] == 2
    assert "INC-A1" in out.iloc[0]["incident_ids"]
    assert "INC-A2" in out.iloc[0]["incident_ids"]


def test_zoom_hotspot_fig_uses_daily_red_comments_blue_points_and_nps_line() -> None:
    rel = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-01", "2026-01-20"]),
            "helix_records": [1, 1],
            "nps_comments": [2, 0],
            "nps_comments_moderate": [1, 0],
            "nps_comments_high": [1, 0],
            "nps_comments_critical": [0, 0],
            "incident_ids": ["INC-1", "INC-2"],
        }
    )
    incident = executive_ppt.ZoomIncident(
        incident_id="INC-1",
        incident_date=pd.Timestamp("2026-01-20"),
        nps_topic="Pagos > Transferencias",
        incident_summary="Resumen",
        detractor_comment="Comentario",
        similarity=0.9,
        hot_term="transferencias",
    )
    topic_daily = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-01", "2026-01-20"]),
            "nps_mean": [8.2, 6.1],
        }
    )
    fig = executive_ppt._zoom_incident_fig(
        topic_daily=topic_daily,
        related_timeline=rel,
        incident=incident,
        lag_days=4,
        changepoints=[],
        focus_name="detractores",
    )
    assert fig is not None
    assert fig.data[0]["type"] == "bar"
    assert fig.data[0]["name"] == "Comentarios moderados (NPS 5-6)"
    assert fig.data[1]["name"] == "Comentarios altos (NPS 3-4)"
    assert fig.data[2]["name"] == "Comentarios críticos (NPS 0-2)"
    assert fig.data[4]["type"] == "scatter"
    assert fig.data[4]["mode"] == "markers+text"
    assert fig.data[4]["yaxis"] == "y2"
    assert any(str(t) == "INC-1" for t in fig.data[4]["text"])
    assert fig.data[5]["name"] == "NPS medio"
    assert fig.data[5]["type"] == "scatter"
    assert fig.data[5]["mode"] == "lines"
    assert fig.data[5]["yaxis"] == "y3"
    assert fig.data[5]["line"]["color"] == "#" + executive_ppt.BBVA_COLORS["green"]
