from __future__ import annotations

import pandas as pd
import pytest

from nps_lens.analytics.drivers import driver_table
from nps_lens.analytics.signal_quality import actionable_rows, is_reserve_category
from nps_lens.domain.comment_text import useful_comment_mask
from nps_lens.reports import executive_ppt
from nps_lens.reports.executive_newsletter import _focus_rows
from nps_lens.services.analytics.kpis_service import compute_score_kpis
from nps_lens.services.dashboard_service import DashboardService


@pytest.mark.parametrize(
    "label",
    [
        "Sin comentarios",
        " SIN CONTENIDO. ",
        "Genérico",
        "GENÉRICA",
        "Comentario genérico",
        "Otros",
        "Sin clasificación temática > Información insuficiente",
        "Acceso / sin comentarios",
        "",
        None,
        pd.NA,
        float("nan"),
    ],
)
def test_placeholder_categories_never_qualify_as_insights(label):
    assert is_reserve_category(label)
    frame = pd.DataFrame({"Subpalanca": [label], "Comment": ["No puedo generar token"], "NPS": [0]})
    assert actionable_rows(frame).empty


@pytest.mark.parametrize(
    "label",
    [
        "Valoración general del canal",
        "Error genérico al firmar",
        "Sin acceso a la cuenta",
        "Faltan comentarios en el expediente",
    ],
)
def test_specific_topics_are_not_excluded_by_a_word_inside_their_name(label):
    assert not is_reserve_category(label)


def test_placeholder_comments_are_excluded_even_when_the_topic_name_looks_specific():
    frame = pd.DataFrame(
        {
            "Subpalanca": ["Token"] * 6,
            "Comment": ["sin comentarios", "...", pd.NA, "GENÉRICO", "", "No genera token"],
            "NPS": [0, 0, 10, 10, 10, 2],
        }
    )
    assert useful_comment_mask(frame).tolist() == [False] * 5 + [True]
    assert actionable_rows(frame).Comment.tolist() == ["No genera token"]
    result = executive_ppt._period_overview(frame)
    assert result["friction"]["n"] == 1
    assert result["strength"] == {}
    assert compute_score_kpis(frame).samples == 6


@pytest.mark.parametrize("categorical", [False, True])
def test_generic_topics_remain_in_nps_and_gaps_but_never_lead_editorial_rankings(categorical):
    frame = pd.DataFrame(
        {
            "Palanca": ["Genérico"] * 6 + ["Sin comentarios"] * 6 + ["Acceso", "Atención"],
            "Subpalanca": ["Genérico"] * 6 + ["Sin comentarios"] * 6 + ["Token", "Ayuda eficaz"],
            "Comment": ["opinión"] * 12 + ["No genera token", "Me ayudaron a resolver"],
            "NPS": [0] * 6 + [10] * 6 + [2, 10],
        }
    )
    if categorical:
        frame = frame.astype(
            {"Palanca": "category", "Subpalanca": "category", "Comment": "category"}
        )
    kpis = compute_score_kpis(frame)
    assert kpis.samples == 14 and kpis.classic_nps == 0
    gaps = driver_table(frame, "Subpalanca")
    assert {row.value for row in gaps} == {"Genérico", "Sin comentarios", "Token", "Ayuda eficaz"}
    assert sum(row.valid_n for row in gaps) == 14
    insights = executive_ppt._period_overview(frame)
    assert insights["friction"]["topic"] == "Token"
    assert insights["strength"]["topic"] == "Ayuda eficaz"
    assert {row["label"] for row in _focus_rows(frame, topic_channel="Todos")} == {
        "Acceso",
        "Atención",
    }
    service = DashboardService.__new__(DashboardService)
    clusters = service._topics_df(frame)
    assert {term for terms in clusters.top_terms for term in terms} == {"Token", "Ayuda eficaz"}


def test_no_meaningful_topic_produces_an_explicit_empty_insight():
    frame = pd.DataFrame(
        {"Subpalanca": ["Genérico", "Sin comentarios"], "Comment": ["opinión", ""], "NPS": [0, 10]}
    )
    result = executive_ppt._period_overview(frame)
    assert result["friction"] == {} and result["strength"] == {}
    for positive in (False, True):
        assert "No hay un tópico con comentarios útiles" in executive_ppt._topic_signal_copy(
            {}, positive=positive
        )
