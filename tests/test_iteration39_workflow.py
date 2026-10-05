"""Helix classification readiness and directional evidence reuse contracts."""

import pandas as pd
import pytest
from test_iteration28_taxonomy_helix import helix_fixture as helix_fixture
from test_iteration28_taxonomy_helix import helix_response
from test_taxonomy_exchange import exchange_fixture as exchange_fixture
from test_taxonomy_exchange import exported, zipped

from nps_lens.analytics.linking_diagnostics import linking_diagnostics
from nps_lens.analytics.nps_helix_link import (
    link_incidents_to_nps_topics,
    retrieve_incident_candidates,
)


@pytest.mark.parametrize("received", [0, 1, 3])
def test_readiness_counts_categories_without_requiring_links(helix, monkeypatch, received):
    handler, ctx, _, incidents, client = helix
    incidents = incidents.iloc[:3].copy()
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    if received:
        request = exported(handler.export(ctx, inputs)["saved_paths"])
        handler.import_response(ctx, inputs, zipped(helix_response(request, "not-a-candidate")))
        with handler.repository._connect() as db:
            db.execute("DELETE FROM helix_incident_links")
            if received < len(incidents):
                db.execute("DELETE FROM helix_incident_categories WHERE incident != 'INC-0'")
    status = handler.status(ctx, inputs)
    assert status["received"] == received
    assert status["pending"] == 3 - received
    assert status["link_pending"] == received
    dashboard = client.app.state.dashboard_service
    monkeypatch.setattr(dashboard, "_load_helix_df", lambda *a, **kw: incidents)
    engine = dashboard.classification_status("helix", ctx)
    assert engine["received"] == received
    assert engine["ready"] is (received == 3)
    if received < 3:
        assert (
            engine["reason"] == "Clasifica primero todas las incidencias Helix de la lente activa."
        )
    result = handler.export(ctx, inputs, only_linking=True)
    if received:
        request = exported(result["saved_paths"])
        rows = [
            row
            for name, batch in request.items()
            if name.startswith("incidents/")
            for row in batch["incidents"]
        ]
        assert len(rows) == received
        assert all("classification" in row for row in rows)
    else:
        assert result["saved_paths"] == []
    response = client.put(
        "/api/taxonomy/helix/engine", params={"service_origin": "Bank", "engine": "llm"}
    )
    assert response.status_code == (200 if received == 3 else 409)
    assert handler.taxonomy.state(ctx)["active"] == "SOURCE"
    assert (
        client.put(
            "/api/taxonomy/helix/engine", params={"service_origin": "Bank", "engine": "rules"}
        ).status_code
        == 200
    )
    assert handler.taxonomy.state(ctx)["active"] == "SOURCE"
    if received == 3:
        client.put("/api/taxonomy/helix/engine", params={"service_origin": "Bank", "engine": "llm"})
    if received == 3:
        diagnostic = dashboard.linking_dashboard(context=ctx)["diagnostics"]
        assert diagnostic["evaluation_state"] == "NOT_EVALUATED"
        assert diagnostic["evaluation_reason"] == "evaluation_pending"


def test_directional_retrieval_rules_and_uncapped_reuse(record_property):
    nps = pd.DataFrame(
        [
            dict(
                ID="N",
                Fecha="2026-09-01",
                NPS=2,
                Comment="Transferencia retenida comprobante ausente",
                Palanca="Pagos",
                Subpalanca="Transferencias",
            )
        ]
    )
    incidents = pd.DataFrame(
        [
            {
                "Incident Number": f"I{i}",
                "Fecha": "2026-09-01",
                "Detailed Description": "Transferencia retenida comprobante ausente",
            }
            for i in range(12)
        ]
        + [
            {
                "Incident Number": "future",
                "Fecha": "2026-09-02",
                "Detailed Description": "Transferencia retenida comprobante ausente",
            }
        ]
    )
    candidates = retrieve_incident_candidates(nps, incidents)
    _, links = link_incidents_to_nps_topics(nps, incidents)
    assert set(candidates.incident_id) == set(links.incident_id) == {f"I{i}" for i in range(12)}
    diagnostic = linking_diagnostics(
        nps=nps,
        focus=nps,
        helix=incidents,
        scoped=incidents,
        period_total=len(incidents),
        eligible=incidents,
        links=links,
        requested_scope=[],
    )
    assert diagnostic["incidents_per_comment"] == dict(
        p50=12, p90=12, p95=12, max=12, comments_gt1=1, comments_gt5=1, comments_gt10=1
    )
    record_property("incidents_per_comment", diagnostic["incidents_per_comment"])
    # Duplicate projections must count each incidence once per comment.
    pairs = pd.DataFrame(
        {
            "nps_id": ["a", "b", "b", "b", "c", "c", "c"],
            "incident_id": ["1", "1", "2", "2", "1", "2", "3"],
        }
    )
    reuse = linking_diagnostics(
        nps=nps,
        focus=nps,
        helix=incidents,
        scoped=incidents,
        period_total=len(incidents),
        eligible=incidents,
        links=pairs,
        requested_scope=[],
    )["incidents_per_comment"]
    assert reuse == dict(
        p50=2, p90=2.8, p95=2.9, max=3, comments_gt1=2, comments_gt5=0, comments_gt10=0
    )


def test_llm_links_reject_incidents_after_the_comment(helix):
    handler, ctx, frame, incidents, _ = helix
    incidents = incidents.iloc[:1].copy()
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    handler.import_response(
        ctx, inputs, zipped(helix_response(request, inputs["comments"][0]["id"]))
    )
    earlier = frame.assign(Fecha=pd.Timestamp("2026-08-31"))
    assert handler.links(ctx, inputs, earlier, incidents).empty
