"""State, retrieval recall and independent Helix artifacts through real consumers."""

import pandas as pd
import pytest
from test_iteration28_taxonomy_helix import discover, helix_response
from test_iteration28_taxonomy_helix import helix_fixture as helix_fixture
from test_taxonomy_exchange import designer_zip, exported, zipped
from test_taxonomy_exchange import exchange_fixture as exchange_fixture

from nps_lens.analytics.nps_helix_link import (
    link_incidents_to_nps_topics,
    retrieve_incident_candidates,
)
from nps_lens.services.classification_protocol import (
    digest,
    encode,
    incident_classification_fingerprint,
    taxonomy_fingerprint,
)
from nps_lens.services.helix_exchange import HelixExchange
from nps_lens.services.taxonomy_exchange import TaxonomyExchange


def test_selected_llm_pending_can_be_disabled_without_artifacts(helix, monkeypatch):
    handler, ctx, _, incidents, client = helix
    dashboard = client.app.state.dashboard_service
    monkeypatch.setattr(dashboard, "_load_helix_df", lambda *a, **kw: incidents)
    state = handler.taxonomy.state(ctx)
    state["causal_engine"] = "llm"
    handler.taxonomy.save_state(ctx, state)
    params = {"service_origin": "Bank"}
    status = client.get("/api/taxonomy/helix/engine", params=params).json()
    assert status["selected_engine"] == "llm" and not status["ready"]
    assert "engine" not in status
    pending = dashboard.linking_dashboard(context=ctx)["diagnostics"]
    assert pending["evaluation_state"] == "NOT_EVALUATED"
    assert pending["evaluation_reason"] == "evaluation_pending"
    response = client.put("/api/taxonomy/helix/engine", params={**params, "engine": "rules"})
    assert response.status_code == 200
    assert response.json()["selected_engine"] == "rules"
    assert handler.taxonomy.state(ctx)["causal_engine"] == "rules"
    evaluated = dashboard.linking_dashboard(context=ctx)["diagnostics"]
    assert evaluated["evaluation_state"] == "MATCHED"
    assert evaluated["evaluation_reason"] != "evaluation_pending"


@pytest.mark.parametrize("mode", ["SOURCE", "DISCOVERED"])
@pytest.mark.parametrize("engine", ["rules", "llm"])
def test_taxonomy_engine_matrix(helix, monkeypatch, mode, engine):
    handler, ctx, _, incidents, client = helix
    comments = TaxonomyExchange(handler.taxonomy, handler.downloads)
    if mode == "DISCOVERED":
        discover(comments, ctx)
    else:
        # An incomplete DISCOVERED lens must not block SOURCE.
        comments.import_response(ctx, designer_zip(comments, ctx), "designer")
    dashboard = client.app.state.dashboard_service
    monkeypatch.setattr(dashboard, "_load_helix_df", lambda *a, **kw: incidents)
    handler.taxonomy.configure(ctx, {"active": mode})
    if engine == "llm":
        inputs = handler.inputs(ctx, incidents, mode)
        request = exported(handler.export(ctx, inputs)["saved_paths"])
        handler.import_response(
            ctx, inputs, zipped(helix_response(request, inputs["comments"][0]["id"]))
        )
    state = handler.taxonomy.state(ctx)
    state["causal_engine"] = engine
    handler.taxonomy.save_state(ctx, state)
    result = dashboard.linking_dashboard(context=ctx)["diagnostics"]
    assert result["evaluation_state"] == "MATCHED"
    assert result["evaluation_reason"] != "evaluation_pending"


# Independently labelled semantic pairs. Each competes with lexical, same-product,
# ambiguous, duplicate, and out-of-window distractors, including lower stable IDs.
GOLDEN = [
    ("Clave", "Entrega", "No llega la clave al móvil", "Token OTP con entrega fallida"),
    ("Pagos", "Envío", "El dinero no sale", "Timeout del procesamiento de remesas"),
    ("Acceso", "Bloqueo", "No me deja entrar", "Autenticación denegada"),
]


def test_candidate_recall_at_n_with_distractors_and_id_permutation(record_property):
    hits = 0
    for lever, sublever, comment, narrative in GOLDEN:
        rows = [
            dict(
                ID=f"a-{i}",
                Fecha="2026-08-10",
                Comment=f"{narrative} consulta {i}",
                Palanca="Otro producto",
                Subpalanca="Consulta",
            )
            for i in range(12)
        ]
        rows += [
            dict(
                ID="z-match",
                Fecha="2026-09-01",
                Comment=comment,
                Palanca=lever,
                Subpalanca=sublever,
            ),
            dict(
                ID="a-symptom",
                Fecha="2026-08-20",
                Comment="Cobro duplicado",
                Palanca=lever,
                Subpalanca="Cobro",
            ),
            dict(
                ID="a-ambiguous",
                Fecha="2026-08-20",
                Comment="No funciona",
                Palanca=lever,
                Subpalanca=sublever,
            ),
            dict(
                ID="a-outside",
                Fecha="2025-01-01",
                Comment=narrative,
                Palanca=lever,
                Subpalanca=sublever,
            ),
        ]
        nps = pd.DataFrame(rows).assign(NPS=2)
        incidents = pd.DataFrame(
            [
                {
                    "Incident Number": "I",
                    "Fecha": "2026-09-01",
                    "summary": narrative,
                    "Palanca": lever,
                    "Subpalanca": sublever,
                }
            ]
        )
        _, rules = link_incidents_to_nps_topics(nps, incidents)
        assert "z-match" not in set(rules.nps_id)
        candidates = retrieve_incident_candidates(nps, incidents, top_k_per_incident=4)
        assert len(candidates) <= 4 and "a-outside" not in set(candidates.nps_id)
        hits += "z-match" in set(candidates.nps_id)
        # Fixed mock accepts only the independently labelled relation.
        accepted = candidates[candidates.nps_id.eq("z-match")].assign(semantic_confidence=0.9)
        assert not accepted.empty
        assert accepted.text_similarity.lt(0.15).all()
        permuted = nps.copy()
        permuted["ID"] = [f"renamed-{i}" for i in reversed(range(len(nps)))]
        renamed_match = permuted.loc[nps.ID.eq("z-match"), "ID"].iloc[0]
        assert renamed_match in set(
            retrieve_incident_candidates(permuted, incidents, top_k_per_incident=4).nps_id
        )
    recall = hits / len(GOLDEN)
    record_property("candidate_recall@4", recall)
    assert recall == 1.0


def test_categories_survive_comment_engine_date_changes_but_not_taxonomy_or_narrative(helix):
    handler, ctx, frame, incidents, _ = helix
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    handler.import_response(
        ctx, inputs, zipped(helix_response(request, inputs["comments"][0]["id"]))
    )
    categories = handler.classifications(ctx, inputs)
    frame.loc[0, "Comment"] = "Otra consulta distinta"
    updated = handler.inputs(ctx, incidents, "SOURCE")
    assert handler.classifications(ctx, updated) == categories
    assert not handler.current(ctx, updated)["SOURCE"]
    for engine in ("llm", "rules"):
        state = handler.taxonomy.state(ctx)
        state["causal_engine"] = engine
        handler.taxonomy.save_state(ctx, state)
        assert handler.classifications(ctx, handler.inputs(ctx, incidents, "SOURCE")) == categories
    assert handler.export(ctx, updated)["saved_paths"] == []
    request = exported(handler.export(ctx, updated, only_linking=True)["saved_paths"])
    assert all(
        "classification" in row
        for name, batch in request.items()
        if name.startswith("incidents/")
        for row in batch["incidents"]
    )
    response = helix_response(request, updated["comments"][1]["id"])
    handler.import_response(ctx, updated, zipped(response))
    assert handler.classifications(ctx, updated) == categories
    incidents.loc[0, "Submit Date"] -= pd.Timedelta(days=1)
    redated = handler.inputs(ctx, incidents, "SOURCE")
    assert handler.classifications(ctx, redated) == categories
    assert "INC-0" not in handler.current(ctx, redated)["SOURCE"]
    incidents.loc[0, "Detailed Description"] = "Nuevo síntoma"
    assert (
        "INC-0"
        not in handler.classifications(ctx, handler.inputs(ctx, incidents, "SOURCE"))["SOURCE"]
    )
    frame["Subpalanca"] = "Nueva categoría"
    assert not handler.classifications(ctx, handler.inputs(ctx, incidents, "SOURCE"))["SOURCE"]


def test_one_time_migration_preserves_categories_and_drops_combined_table(helix):
    handler, ctx, _, incidents, _ = helix
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    handler.import_response(
        ctx, inputs, zipped(helix_response(request, inputs["comments"][0]["id"]))
    )
    categories = handler.classifications(ctx, inputs)
    combined = handler.current(ctx, inputs)["SOURCE"]
    with handler.repository._connect() as db:
        db.execute(
            "CREATE TABLE helix_classifications (context TEXT, scope TEXT, incident TEXT, fingerprint TEXT, payload TEXT)"
        )
        db.executemany(
            "INSERT INTO helix_classifications VALUES (?, ?, ?, ?, ?)",
            [
                (
                    ctx.service_origin,
                    inputs["scopes"]["SOURCE"],
                    row["id"],
                    digest([row["description"], row["date"]]),
                    encode(combined[row["id"]]).decode(),
                )
                for row in inputs["incidents"]
            ],
        )
        db.execute("DELETE FROM helix_incident_categories")
        db.execute("DELETE FROM helix_incident_links")
    for _ in range(2):
        handler = HelixExchange(handler.taxonomy, handler.downloads)
        assert handler.classifications(ctx, inputs) == categories
        with handler.repository._connect() as db:
            assert (
                db.execute(
                    "SELECT 1 FROM sqlite_master WHERE name='helix_classifications'"
                ).fetchone()
                is None
            )
    assert not handler.current(ctx, inputs)["SOURCE"]


def test_narrative_fingerprint_excludes_linking_date():
    incident = {"description": "Transferencia retenida", "date": "2026-09-01"}
    assert incident_classification_fingerprint(incident) == incident_classification_fingerprint(
        {
            **incident,
            "date": "2026-10-01",
            "title": "Acceso / Claves / Token",
            "routing": "Mesa administrativa",
            "Product Categorization Tier 1": "Canal",
        }
    )


def test_mexico_real_excel_rules_evidence_without_llm():
    from pathlib import Path

    from nps_lens.ingest.helix_incidents import read_helix_incidents_excel
    from nps_lens.ingest.nps_thermal import read_nps_thermal_excel

    fixtures = Path(__file__).parent / "fixtures" / "excel"
    nps = read_nps_thermal_excel(
        str(next(fixtures.glob("*01Enero*"))), service_origin="BBVA México"
    ).df
    incidents = read_helix_incidents_excel(
        str(fixtures / "issues_20260506_173749.xlsx"), "BBVA México", "", ""
    ).df
    _, links = link_incidents_to_nps_topics(nps, incidents)
    assert not links.empty
    assert links.attrs["evaluation_state"] == "MATCHED"
    assert links.attrs["evaluation_reason"] != "evaluation_pending"
    assert links.semantic_confidence.isna().all()


def test_export_recall_with_distractors_and_mock_import(helix):
    handler, ctx, frame, incidents, _ = helix
    frame.drop(frame.index[16:], inplace=True)
    frame["Comment"] = [f"Token OTP con entrega fallida consulta {i}" for i in range(len(frame))]
    frame["Palanca"], frame["Subpalanca"] = "Otros", "Consulta"
    frame["Fecha"] = pd.Timestamp("2026-09-02")
    frame.loc[15, ["Comment", "Palanca", "Subpalanca", "Fecha"]] = [
        "No llega la clave al móvil",
        "Atención",
        "Resolución",
        pd.Timestamp("2026-09-01"),
    ]
    incidents = incidents.iloc[:1].assign(
        **{
            "Detailed Description": "Token OTP con entrega fallida",
            "Product Categorization Tier 1": "Atención",
            "Product Categorization Tier 2": "Resolución",
        }
    )
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    correct = inputs["comments"][-1]["id"]
    _, rules = link_incidents_to_nps_topics(inputs["frame"], incidents)
    assert correct not in set(rules.nps_id)
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    candidates = request["incidents/000001.json"]["incidents"][0]["candidates"]
    assert correct in {row["id"] for row in candidates}
    assert 1 < len(candidates) < len(frame)
    handler.import_response(ctx, inputs, zipped(helix_response(request, correct)))
    links = handler.links(ctx, inputs, inputs["frame"], incidents)
    assert set(links.nps_id) == {correct}
    assert links.semantic_confidence.eq(0.9).all()
    assert links.text_similarity.lt(0.15).all()


@pytest.mark.parametrize("with_candidates", [True, False])
def test_imported_llm_empty_decision_distinguishes_no_match_from_not_evaluated(
    helix, with_candidates
):
    handler, ctx, frame, incidents, _ = helix
    incidents = incidents.iloc[:1]
    if not with_candidates:
        frame["Comment"] = ""
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    response = helix_response(request, "not-a-candidate")
    handler.import_response(ctx, inputs, zipped(response))
    links = handler.links(ctx, inputs, inputs["frame"], incidents)
    assert links.empty
    assert links.attrs["evaluation_state"] == (
        "EVALUATED_NO_MATCH" if with_candidates else "NOT_EVALUATED"
    )


def test_comment_assignment_signature_invalidates_only_linking(helix, monkeypatch):
    handler, ctx, _, incidents, _ = helix
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    handler.import_response(
        ctx, inputs, zipped(helix_response(request, inputs["comments"][0]["id"]))
    )
    categories = handler.classifications(ctx, inputs)
    resolve = handler.taxonomy.resolve

    def changed_assignments(*args, **kwargs):
        result = resolve(*args, **kwargs)
        result.attrs["classification_signature"] = "new-comment-assignments"
        return result

    monkeypatch.setattr(handler.taxonomy, "resolve", changed_assignments)
    updated = handler.inputs(ctx, incidents, "SOURCE")
    assert handler.classifications(ctx, updated) == categories
    assert not handler.current(ctx, updated)["SOURCE"]


@pytest.mark.parametrize("mode", ["SOURCE", "DISCOVERED"])
def test_unrecoverable_legacy_classification_is_discarded_and_can_be_reexported(helix, mode):
    handler, ctx, _, incidents, _ = helix
    if mode == "DISCOVERED":
        discover(TaxonomyExchange(handler.taxonomy, handler.downloads), ctx)
    handler.taxonomy.configure(ctx, {"active": mode})
    with handler.repository._connect() as db:
        db.execute(
            "CREATE TABLE helix_classifications (context TEXT, scope TEXT, incident TEXT, fingerprint TEXT, payload TEXT)"
        )
        db.execute("INSERT INTO helix_classifications VALUES ('Bank','missing','I','missing','{}')")
        db.execute(
            "INSERT INTO helix_exchange VALUES ('unrelated', 'Other', ?)",
            (encode({"current": True}).decode(),),
        )

    handler = HelixExchange(handler.taxonomy, handler.downloads)
    inputs = handler.inputs(ctx, incidents, mode)
    status = handler.status(ctx, inputs)
    exported_result = handler.export(ctx, inputs)

    assert status["received"] == 0
    assert status["pending"] == status["total"] == len(incidents)
    assert exported_result["pending"] == len(incidents)
    assert exported_result["saved_paths"]
    with handler.repository._connect() as db:
        assert (
            db.execute("SELECT 1 FROM sqlite_master WHERE name='helix_classifications'").fetchone()
            is None
        )
        assert (
            db.execute("SELECT COUNT(*) FROM helix_exchange WHERE id='unrelated'").fetchone()[0]
            == 1
        )


def test_late_source_helix_job_imports_without_contaminating_active_discovered(helix):
    handler, ctx, _, incidents, _ = helix
    source_inputs = handler.inputs(ctx, incidents, "SOURCE")
    source_request = exported(handler.export(ctx, source_inputs)["saved_paths"])
    assert source_request["manifest.json"]["taxonomy_mode"] == "SOURCE"

    comments = TaxonomyExchange(handler.taxonomy, handler.downloads)
    discover(comments, ctx)
    discovered_inputs = handler.inputs(ctx, incidents, "DISCOVERED")
    discovered_fingerprint = taxonomy_fingerprint(discovered_inputs["taxonomies"]["DISCOVERED"])
    discovered_request = exported(handler.export(ctx, discovered_inputs)["saved_paths"])
    assert discovered_request["manifest.json"]["taxonomy_mode"] == "DISCOVERED"
    assert discovered_request["manifest.json"]["taxonomy_fingerprint"] == discovered_fingerprint

    handler.import_response(
        ctx,
        discovered_inputs,
        zipped(helix_response(source_request, source_inputs["comments"][0]["id"])),
    )
    assert handler.taxonomy.state(ctx)["active"] == "DISCOVERED"
    assert handler.classifications(ctx, discovered_inputs)["DISCOVERED"] == {}
