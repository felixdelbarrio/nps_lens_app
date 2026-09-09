import pandas as pd

from nps_lens.analytics.evidence_highlights import contributing_terms, evidence_segments
from nps_lens.analytics.nps_helix_link import link_incidents_to_nps_topics


def test_segments_preserve_unicode_punctuation_and_untrusted_markup():
    text = "😀 Error: TRANSFERÉNCIA, transferencia. <script>alert(1)</script>"
    segments = evidence_segments(text, {"transferencia"})
    assert "".join(segment["text"] for segment in segments) == text
    assert [segment["text"] for segment in segments if segment["bold"]] == [
        "TRANSFERÉNCIA",
        "transferencia",
    ]
    assert evidence_segments(text, set()) == []
    assert evidence_segments(text, {"inexistente"}) == []


def test_contributors_use_only_nonzero_shared_features_and_skip_stopwords():
    assert contributing_terms("La conexión falla", {"la", "conexion"}, set()) == ("conexion",)
    assert contributing_terms("Pagos pendientes", set(), {"pago"}) == ("pagos",)
    assert contributing_terms("Sin coincidencia", set(), set()) == ()


def test_linking_carries_contributing_terms_from_existing_sparse_vectors():
    nps = pd.DataFrame(
        [
            {
                "ID": "N1",
                "Fecha": "2026-03-01",
                "Palanca": "Pagos",
                "Subpalanca": "Transferencias",
                "Comment": "Transferencia pendiente error",
                "NPS": 0,
            }
        ]
    )
    helix = pd.DataFrame(
        [
            {
                "Incident Number": "INC1",
                "Fecha": "2026-03-01",
                "Detailed Description": "Error transferencia pendiente al enviar",
            }
        ]
    )
    _, links = link_incidents_to_nps_topics(nps, helix, min_similarity=0.01)
    assert len(links) == 1
    assert {"transferencia", "pendiente", "error"}.issubset(links.iloc[0].matched_terms)


def test_snapshot_preserves_nested_emphasis_and_canonical_identity():
    from nps_lens.services.dashboard_service import DashboardService, _cap_chain_evidence_rows

    segments = evidence_segments("Error transferencia", {"transferencia"})
    chains = pd.DataFrame(
        [
            {
                "nps_topic": "Journey",
                "anchor_topic": "Pagos",
                "touchpoint": "Transferencias",
                "linked_pairs": 1,
                "linked_incidents": 1,
                "linked_comments": 1,
                "avg_nps": 2,
                "avg_similarity": 0.7,
                "support_organizations": "Equipo",
                "historical_resolution_weeks": 1.234,
                "incident_records": [
                    {
                        "incident_id": "INC1",
                        "summary": "Error transferencia",
                        "summary_segments": segments,
                    }
                ],
            }
        ]
    )
    capped = _cap_chain_evidence_rows(chains, max_incident_examples=1, max_comment_examples=1)
    service = object.__new__(DashboardService)
    card = service._build_linking_scenario_cards(capped)[0]
    assert card["incident_records"][0]["summary_segments"] == segments
    assert card["identity_rows"][-1]["value"] == "1,23"
    assert len(card["identity_rows"]) == 3
    summary = service._build_entity_summary_df(chains, touchpoint_source="broken_journeys")
    assert list(summary.columns) == [
        "Tópico NPS ancla",
        "Incidencias relacionadas",
        "Comentarios relacionados",
        "Vínculos semánticos",
        "Similitud media",
        "Nota media (0–10)",
    ]
