from __future__ import annotations

from dotenv import dotenv_values
from test_iteration28_taxonomy_helix import discover, helix_response
from test_iteration28_taxonomy_helix import helix_fixture as helix_fixture
from test_taxonomy_exchange import exchange_fixture as exchange_fixture
from test_taxonomy_exchange import (
    exported,
    zipped,
)

from nps_lens.domain.models import UploadContext
from nps_lens.services.helix_exchange import HelixExchange
from nps_lens.services.taxonomy_service import TaxonomyService


def test_framework_persists_per_owner_and_drives_analysis_and_snapshot(exchange, monkeypatch):
    handler, ctx, frame, client = exchange
    frame["Palanca"], frame["Subpalanca"] = "Atención", "Resolución"
    tax = handler.taxonomy
    path = tax.dotenv_path
    path.write_text("UNRELATED_SETTING=preserved\n", encoding="utf-8")
    discover(handler, ctx)
    tax.configure(ctx, {"active": "SOURCE"})
    other = UploadContext("Other")
    tax.save_manual(other, [{"lever": "Manual", "sublevers": ["Propia"]}], template="NONE")
    tax.configure(other, {"active": "COMPLETED"})
    assert dotenv_values(path)["UNRELATED_SETTING"] == "preserved"
    assert '"Bank": "SOURCE"' in dotenv_values(path)["NPS_LENS_CLASSIFICATION_FRAMEWORKS"]
    restarted = TaxonomyService(tax.repository, tax.equivalences_path, path)
    monkeypatch.setattr(restarted, "source", lambda context: frame.copy())
    assert restarted.state(ctx)["active"] == "SOURCE"
    assert restarted.state(other)["active"] == "COMPLETED"
    assert restarted.resolve(ctx).attrs["taxonomy_mode"] == "SOURCE"
    assert restarted.snapshot(ctx)["active"] == "SOURCE"
    tax.configure(ctx, {"active": "DISCOVERED"})
    assert tax.resolve(ctx).attrs["taxonomy_mode"] == "DISCOVERED"
    assert tax.snapshot(ctx)["active"] == "DISCOVERED"
    dashboard = client.app.state.dashboard_service
    dashboard.clear_caches()
    assert dashboard._load_nps_df(ctx).attrs["taxonomy_mode"] == "DISCOVERED"
    assert handler.progress(ctx)["pending"] == 0
    assert (
        client.put(
            "/api/taxonomy/settings",
            params={"service_origin": "Bank"},
            json={"llm_active": "SOURCE"},
        ).status_code
        == 422
    )


def test_switching_framework_keeps_both_comment_classifications(exchange):
    handler, ctx, frame, _ = exchange
    frame["Palanca"], frame["Subpalanca"] = "Original", "Categoría"
    discover(handler, ctx)
    discovered = handler.taxonomy.resolve(ctx)
    handler.taxonomy.configure(ctx, {"active": "SOURCE"})
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    response = {"manifest.json": request["manifest.json"]}
    response.update(
        {
            name.replace("comments/", "results/"): {
                "classifications": [
                    {
                        "id": row["id"],
                        "primary": "c001",
                        "secondary": [],
                    }
                    for row in batch["comments"]
                ]
            }
            for name, batch in request.items()
            if name.startswith("comments/")
        }
    )
    handler.import_response(ctx, zipped(response), "classifier")
    for mode in ["DISCOVERED", "SOURCE", "DISCOVERED"]:
        handler.taxonomy.configure(ctx, {"active": mode})
        assert handler.progress(ctx)["pending"] == 0
        assert handler.export(ctx, "classifier")["saved_paths"] == []
    assert handler.taxonomy.resolve(ctx).Palanca.tolist() == discovered.Palanca.tolist()
    frame.loc[len(frame)] = ["new", "Nuevo comentario", "Original", "Categoría", "Web"]
    assert handler.progress(ctx)["pending"] == 1
    assert handler.taxonomy.studio(ctx)["active"] == "DISCOVERED"
    assert handler.taxonomy.resolve(ctx).iloc[-1].Palanca == ""


def test_helix_new_comments_invalidate_prior_absence_of_links(helix, monkeypatch):
    handler, ctx, frame, incidents, _ = helix
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"])
    rows = response["results/000001.json"]["classifications"]
    for row in rows[1:]:
        row["links"] = []
    for row in response["results/000002.json"]["classifications"]:
        row["links"] = []
    handler.import_response(ctx, inputs, zipped(response))
    extra = frame.iloc[0].copy()
    extra["_business_key"], extra["Comment"] = "new", "Nuevo comentario"
    frame.loc[len(frame)] = extra
    restarted = TaxonomyService(handler.repository, handler.taxonomy.equivalences_path)
    monkeypatch.setattr(restarted, "source", lambda context: frame.copy())
    handler = HelixExchange(restarted, handler.downloads)
    updated = handler.inputs(ctx, incidents, "SOURCE")
    assert updated["scopes"] != inputs["scopes"]
    status = handler.status(ctx, updated)
    assert status["pending"] == 0
    assert status["link_pending"] == len(incidents)


def test_accept_proposal_preserves_source_and_explores_only_real_assignments(exchange):
    from test_taxonomy_exchange import classifier_files, classifier_zip, designer_zip

    import pandas as pd

    handler, ctx, frame, client = exchange
    frame["Palanca"], frame["Subpalanca"] = "Original", "Categoría"
    frame["NPS"], frame["Fecha"] = 10, pd.Timestamp("2026-09-01")
    tax = handler.taxonomy
    dashboard = client.app.state.dashboard_service
    params = {"service_origin": "Bank"}
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    proposal = tax.state(ctx)["proposed_discovered_taxonomy"]
    fingerprint = tax.state(ctx)["proposed_discovered_fingerprint"]
    assert tax.explore(ctx, "DISCOVERED")["rows"] == []
    # Selecting a pending proposal never accepts it implicitly.
    assert (
        client.put(
            "/api/taxonomy/settings", params=params, json={"active": "DISCOVERED"}
        ).status_code
        == 409
    )
    accepted = client.put("/api/taxonomy/settings", params=params, json={"accept_proposal": True})
    assert accepted.status_code == 200
    assert accepted.json()["active"] == "SOURCE"
    state = tax.state(ctx)
    assert state["discovered_taxonomy"] == proposal
    assert state["taxonomy_fingerprint"] == fingerprint
    assert "proposed_discovered_taxonomy" not in state
    assert "proposed_discovered_fingerprint" not in state
    assert next(card for card in tax.studio(ctx)["taxonomies"] if card["mode"] == "DISCOVERED")[
        "available"
    ]
    assert dashboard._load_nps_df(ctx).Palanca.eq("Original").all()
    empty = tax.explore(ctx, "DISCOVERED")
    assert empty["rows"] == [] and empty["total"] == 0
    assert (
        empty["note"]
        == "Clasifica comentarios con esta taxonomía para ver volumen, NPS y ejemplos."
    )
    assert (
        client.put(
            "/api/taxonomy/settings", params=params, json={"active": "DISCOVERED"}
        ).status_code
        == 200
    )
    assert dashboard._load_nps_df(ctx).Palanca.eq("").all()
    assert (
        client.get("/api/taxonomy/comments/engine", params=params).json()["selected_engine"]
        == "llm"
    )
    assert (
        client.put(
            "/api/taxonomy/comments/engine", params={**params, "engine": "rules"}
        ).status_code
        == 409
    )
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    handler.import_response(
        ctx, classifier_zip(classifier_files(request["manifest.json"], request)), "classifier"
    )
    explored = tax.explore(ctx, "DISCOVERED")
    assert explored["total"] == 1
    row = explored["rows"][0]
    assert (row["Palanca"], row["Subpalanca"], row["volume"], row["share"], row["nps"]) == (
        "Atención",
        "Resolución",
        len(frame),
        1,
        100,
    )
    assert row["promoters"] == len(frame) and row["detractors"] == 0
    dashboard.clear_caches()
    assert dashboard._load_nps_df(ctx).Palanca.eq("Atención").all()
    # Reaccepting an identical semantic catalog preserves assignments and in-flight jobs.
    signature = tax.state(ctx)["artifacts"]["DISCOVERED"]
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    tax.configure(ctx, {"accept_proposal": True})
    assert tax.state(ctx)["artifacts"]["DISCOVERED"] == signature
    assert handler.progress(ctx)["pending"] == 0
