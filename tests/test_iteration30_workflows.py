from __future__ import annotations

import copy
from dataclasses import replace

import pandas as pd
import pytest
from test_iteration28_taxonomy_helix import helix_fixture as helix_fixture
from test_iteration28_taxonomy_helix import helix_response
from test_taxonomy_exchange import classifier_files, classifier_zip, exported, zipped
from test_taxonomy_exchange import exchange_fixture as exchange_fixture

from nps_lens.domain.models import UploadContext
from nps_lens.services.equivalence_exchange import EquivalenceExchange


def test_concepts_are_owner_scoped_atomic_and_corpus_bound(exchange):
    handler, context, frame, client = exchange
    frame["Palanca"], frame["Subpalanca"] = "Atención", "Resolución"
    other = UploadContext("Other")
    baseline = handler.taxonomy.registry(other).to_dict()
    concepts = EquivalenceExchange(handler.taxonomy, handler.downloads)
    files = exported(concepts.export_concepts(context, pd.DataFrame())["saved_path"])
    response = {
        "manifest.json": files["manifest.json"],
        "equivalences.json": {
            "dimensions": {
                "nps.Palanca": [
                    {
                        "canonical": "Soporte",
                        "aliases": ["Atención"],
                        "reason": "Ambos designan la misma atención al cliente.",
                    }
                ]
            }
        },
    }
    invalid = copy.deepcopy(response)
    invalid["equivalences.json"]["dimensions"]["nps.Palanca"][0]["aliases"] = ["Inventado"]
    with pytest.raises(ValueError, match="alias"):
        concepts.import_concepts(context, pd.DataFrame(), zipped(invalid))
    assert handler.taxonomy.registry(context).to_dict() == baseline
    with pytest.raises(ValueError, match="dataset"):
        concepts.import_concepts(other, pd.DataFrame(), zipped(response))
    assert concepts.import_concepts(context, pd.DataFrame(), zipped(response))["owner"] == "Bank"
    assert handler.taxonomy.registry(context).normalize("nps.Palanca", "Atención") == "Soporte"
    assert handler.taxonomy.registry(other).to_dict() == baseline
    handler.taxonomy.save_manual(context, [{"lever": "Atención", "sublevers": ["Resolución"]}])
    for mode in ("SOURCE", "COMPLETED"):
        assert handler.taxonomy.resolve(context, mode=mode).Palanca.eq("Soporte").all()
    params = {"service_origin": "Other"}
    assert (
        client.get("/api/settings/equivalences", params=params).json()["dimensions"]
        == baseline["dimensions"]
    )
    frame.loc[0, "Comment"] = "Changed"
    with pytest.raises(ValueError, match="cambiado"):
        concepts.import_concepts(context, pd.DataFrame(), zipped(response))


def test_comment_classifications_are_independent_multi_topic_and_scoped(exchange):
    handler, ctx, frame, client = exchange
    frame["Palanca"], frame["Subpalanca"] = "Atención", "Resolución"
    frame.loc[404, ["Palanca", "Subpalanca"]] = ["Acceso", "Token"]
    frame["Fecha"], frame["NPS"] = pd.Timestamp("2026-10-01"), 2
    frame.loc[:199, "Fecha"] = pd.Timestamp("2026-09-01")
    handler.taxonomy.configure(ctx, {"active": "SOURCE"})
    files = exported(handler.export(ctx, "classifier")["saved_paths"])
    result = classifier_files(files["manifest.json"], files)
    result["results"] = {"000001": result["results"]["000001"]}
    for row in result["results"]["000001"]["classifications"]:
        row["primary"] = "c001"
        row["secondary"] = ["c002"]
    handler.import_response(ctx, classifier_zip(result), "classifier")
    assert handler.progress(ctx)["multiple"] == 200
    assert handler.taxonomy.resolve(ctx, mode="SOURCE").Palanca.eq("Atención").sum() == 404
    assert handler.apply(ctx, frame.iloc[:200], "SOURCE").Palanca.eq("Acceso").all()
    params = {
        "service_origin": "Bank",
        "pop_year": "2026",
        "pop_month": "09",
        "score_channel": "Web",
        "nps_group": "Detractores",
    }
    status = client.get("/api/taxonomy/comments/engine", params=params).json()
    assert status["ready"] and status["received"] == status["total"] == 200
    assert (
        client.put("/api/taxonomy/comments/engine", params={**params, "engine": "llm"}).status_code
        == 200
    )
    status = client.get(
        "/api/taxonomy/comments/engine", params={**params, "pop_month": "10"}
    ).json()
    assert not status["ready"] and status["engine"] == "rules"
    assert (
        client.put(
            "/api/taxonomy/comments/engine", params={**params, "pop_month": "10", "engine": "llm"}
        ).status_code
        == 409
    )
    handler.taxonomy.save_manual(ctx, handler.taxonomy.manual_draft(ctx)["taxonomy"])
    handler.taxonomy.configure(ctx, {"active": "COMPLETED"})
    assert handler.progress(ctx)["received"] == 0
    handler.taxonomy.configure(ctx, {"active": "SOURCE"})
    assert handler.progress(ctx)["received"] == 200


def test_helix_toggle_uses_visible_window_and_llm_links_obey_dates(helix, monkeypatch):
    handler, ctx, frame, incidents, client = helix
    incidents.loc[200, "Submit Date"] = pd.Timestamp("2026-10-01")
    dashboard = client.app.state.dashboard_service
    monkeypatch.setattr(dashboard, "_load_helix_df", lambda *args, **kwargs: incidents)
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    files = exported(handler.export(ctx, inputs)["saved_paths"])
    response = helix_response(files, inputs["comments"][0]["id"])
    partial = {name: value for name, value in response.items() if name != "results/000002.json"}
    handler.import_response(ctx, inputs, zipped(partial))
    params = {
        "service_origin": "Bank",
        "pop_year": "2026",
        "pop_month": "09",
        "max_days_apart": "0",
    }
    status = client.get("/api/taxonomy/helix/engine", params=params).json()
    assert status["ready"] and status["total"] == 200
    assert (
        client.put("/api/taxonomy/helix/engine", params={**params, "engine": "llm"}).status_code
        == 200
    )
    status = client.get(
        "/api/taxonomy/helix/engine", params={**params, "max_days_apart": "90"}
    ).json()
    assert not status["ready"] and status["engine"] == "rules"
    handler.import_response(ctx, inputs, zipped(response))
    assert len(handler.links(ctx, inputs, frame, incidents, max_days_apart=0)) == 200
    assert len(handler.links(ctx, inputs, frame, incidents, max_days_apart=90)) == 201
    assert "min_similarity" not in files["manifest.json"]


def test_normalization_and_comment_engine_are_local_only(exchange):
    _, _, _, client = exchange
    client.app.state.settings = replace(client.app.state.settings, auth_mode="iap")
    for path in ("/api/taxonomy/comments/engine", "/api/taxonomy/discovery/instructions"):
        assert client.get(path).status_code in (401, 403, 404)
    assert client.post("/api/taxonomy/discovery/normalizer/export").status_code in (401, 403, 404)
