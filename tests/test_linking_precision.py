"""Negative controls for evidence linking and shared KPI denominators."""

import pandas as pd
import pytest

from nps_lens.analytics.nps_helix_link import (
    daily_aggregates,
    link_incidents_to_nps_topics,
    weekly_aggregates,
)
from nps_lens.core.metrics import summarize
from nps_lens.core.nps_math import grouped_focus_rates
from nps_lens.services.analytics.kpis_service import compute_score_kpis


def frames(comment, narrative, *, day="2026-09-01"):
    return (
        pd.DataFrame(
            [
                {
                    "ID": "N1",
                    "Fecha": "2026-09-01",
                    "NPS": 0,
                    "Palanca": "Acceso bloqueado",
                    "Subpalanca": "Credenciales vencidas",
                    "Comment": comment,
                }
            ]
        ),
        pd.DataFrame(
            [
                {
                    "Incident Number": "INC1",
                    "Fecha": day,
                    "summary": narrative,
                    "BBVA_SourceServiceN2": "Acceso bloqueado credenciales vencidas",
                }
            ]
        ),
    )


@pytest.mark.parametrize(
    "comment,narrative",
    [
        ("", "Acceso bloqueado credenciales vencidas"),
        ("Transferencia rechazada comprobante ausente", "Acceso bloqueado credenciales vencidas"),
        ("Error sistema usuario empresa", "Error sistema usuario empresa"),
        ("La firma falla", "Error firma de contrato"),
        ("Autorizacion pendiente", "Actualizacion pendientes"),
        ("Tarjeta credito cobro duplicado", "Tarjeta credito entrega demorada"),
    ],
)
def test_labels_generic_words_and_character_fragments_do_not_link(comment, narrative):
    assignments, links = link_incidents_to_nps_topics(*frames(comment, narrative), min_similarity=0)
    assert links.empty
    assert assignments.empty


@pytest.mark.parametrize("day", [None, "2020-01-01"])
def test_assignment_cannot_bypass_comment_time_window(day):
    assignments, links = link_incidents_to_nps_topics(
        *frames("Login bloqueado", "Login bloqueado", day=day)
    )
    assert links.empty and assignments.empty


def test_specific_evidence_survives_and_does_not_inherit_unrelated_service():
    assignments, links = link_incidents_to_nps_topics(
        *frames("Transferencia rechazada", "Transferencia rechazada")
    )
    assert len(links) == len(assignments) == 1
    assert set(links.iloc[0].matched_terms) == {"transferencia", "rechazada"}


def test_all_kpis_use_valid_scores_and_count_responses_without_external_id():
    nps = pd.DataFrame(
        {"NPS": [0, 6, 8, 10, -1, 11, 6.5, None], "ID": [None] * 8, "Fecha": ["2026-09-01"] * 8}
    )
    summary = summarize(nps)
    kpis = compute_score_kpis(nps)
    assert summary.n == kpis.samples == 4
    assert summary.nps_avg == kpis.nps_average == 6
    assert summary.detractor_rate == kpis.detractor_rate == 0.5
    assert summary.nps_classic_pp == kpis.classic_nps == -25
    assert grouped_focus_rates(nps).iloc[0].detractor_rate == 0.5
    for aggregate in (daily_aggregates, weekly_aggregates):
        overall, _ = aggregate(
            nps, pd.DataFrame(columns=["Incident Number", "Fecha"]), pd.DataFrame()
        )
        assert overall.iloc[0].responses == 4
        assert overall.iloc[0].focus_rate == 0.5
        assert overall.iloc[0].nps_mean == 6
