"""Contracts and calibration on anonymized Argentina evidence; no live LLM claims."""

import copy
import json
from pathlib import Path

import pandas as pd
import pytest
from test_iteration28_taxonomy_helix import helix_fixture as helix_fixture
from test_iteration28_taxonomy_helix import helix_response
from test_taxonomy_exchange import (
    classifier_files,
    classifier_zip,
    designer_zip,
    exported,
    zipped,
)
from test_taxonomy_exchange import exchange_fixture as exchange_fixture

from nps_lens.analytics.nps_helix_link import build_incident_text, retrieve_incident_candidates
from nps_lens.analytics.signal_quality import audit_classifications
from nps_lens.services.classification_protocol import (
    CompactComment,
    canonical_comment_key,
    taxonomy_fingerprint,
)
from nps_lens.services.taxonomy_exchange import TaxonomyExchange

GOLDEN = json.loads((Path(__file__).parent / "fixtures/argentina_semantic.json").read_text())


def test_pending_proposal_guards_both_exports_and_late_results(helix):
    handler, ctx, _, incidents, _ = helix
    comments = TaxonomyExchange(handler.taxonomy, handler.downloads)
    comments.import_response(ctx, designer_zip(comments, ctx), "designer")
    handler.taxonomy.configure(ctx, {"accept_proposal": True})
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    old_comments = exported(comments.export(ctx, "classifier")["saved_paths"])
    old_inputs = handler.inputs(ctx, incidents, "DISCOVERED")
    old_helix = exported(handler.export(ctx, old_inputs)["saved_paths"])
    state = handler.taxonomy.state(ctx)
    proposal = copy.deepcopy(state["discovered_taxonomy"])
    proposal["taxonomy"][0]["sublevers"][0]["criterion"] += " Nueva frontera."
    state["proposed_discovered_taxonomy"] = proposal
    state["proposed_discovered_fingerprint"] = taxonomy_fingerprint(proposal)
    handler.taxonomy.save_state(ctx, state)
    old_fingerprint = taxonomy_fingerprint(handler.taxonomy.catalog(ctx, "DISCOVERED"))
    for export in (
        lambda: comments.export(ctx, "classifier"),
        lambda: handler.export(ctx, old_inputs),
    ):
        with pytest.raises(ValueError, match="Activa o descarta"):
            export()
    assert taxonomy_fingerprint(handler.taxonomy.catalog(ctx, "DISCOVERED")) == old_fingerprint
    handler.taxonomy.configure(ctx, {"active": "SOURCE"})
    assert comments.export(ctx, "classifier")["saved_paths"]
    assert handler.export(ctx, handler.inputs(ctx, incidents, "SOURCE"))["saved_paths"]
    handler.taxonomy.configure(ctx, {"accept_proposal": True})
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    fingerprint = taxonomy_fingerprint(proposal)
    new_comments = exported(comments.export(ctx, "classifier")["saved_paths"])
    new_helix = exported(
        handler.export(ctx, handler.inputs(ctx, incidents, "DISCOVERED"))["saved_paths"]
    )
    for request in (new_comments, new_helix):
        assert request["manifest.json"]["taxonomy_mode"] == "DISCOVERED"
        assert request["manifest.json"]["taxonomy_fingerprint"] == fingerprint
    comments.import_response(
        ctx,
        classifier_zip(classifier_files(old_comments["manifest.json"], old_comments)),
        "classifier",
    )
    handler.import_response(
        ctx,
        handler.inputs(ctx, incidents, "DISCOVERED"),
        zipped(helix_response(old_helix, "not-a-candidate")),
    )
    assert taxonomy_fingerprint(handler.taxonomy.catalog(ctx, "DISCOVERED")) == fingerprint
    assert comments.progress(ctx)["received"] == 0
    assert handler.status(ctx, handler.inputs(ctx, incidents, "DISCOVERED"))["received"] == 0
    state = handler.taxonomy.state(ctx)
    state["proposed_discovered_taxonomy"] = copy.deepcopy(proposal)
    state["proposed_discovered_taxonomy"]["taxonomy"][0]["sublevers"][0]["criterion"] += " Otra."
    handler.taxonomy.save_state(ctx, state)
    handler.taxonomy.configure(ctx, {"discard_proposal": True})
    assert comments.export(ctx, "classifier")["saved_paths"]
    assert taxonomy_fingerprint(handler.taxonomy.catalog(ctx, "DISCOVERED")) == fingerprint


def test_canonical_raw_representative_and_reuse_after_format_changes(exchange):
    handler, ctx, frame, _ = exchange
    raw = "  NO PUEDO REALIZAR MIS PAGOS PORQUE NO FUNCIONA EL TOKEN DIGITAL  "
    frame["Comment"] = [raw if i % 2 else raw.lower().replace(" ", "\t") for i in range(len(frame))]
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"accept_proposal": True})
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    assert request["comments/000001.json"]["comments"] == [
        {"id": "1", "Comment": frame.iloc[0].Comment}
    ]
    handler.import_response(
        ctx, classifier_zip(classifier_files(request["manifest.json"], request)), "classifier"
    )
    frame["Comment"] = raw.swapcase().replace(" ", "\n")
    assert handler.progress(ctx)["pending"] == 0
    assert handler.export(ctx, "classifier")["saved_paths"] == []
    assert canonical_comment_key(" PE\u0301SIMO \n") == canonical_comment_key("pésimo")


@pytest.mark.parametrize("task,symptom", [(False, True), (True, False), (False, False)])
def test_link_contract_rejects_false_booleans_atomically(helix, task, symptom):
    handler, ctx, _, incidents, _ = helix
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"])
    response["results/000001.json"]["classifications"][0]["links"][0].update(
        same_task=task, same_symptom=symptom
    )
    with pytest.raises(ValueError, match="same_task=true y same_symptom=true"):
        handler.import_response(ctx, inputs, zipped(response))
    assert handler.status(ctx, inputs)["received"] == 0


def test_narrative_invariant_to_titles_routing_and_template(helix):
    handler, ctx, _, incidents, _ = helix
    incidents["Detailed Description"] = GOLDEN["incident"]["description"]
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    handler.import_response(ctx, inputs, zipped(helix_response(request, "not-a-candidate")))
    changed = incidents.assign(
        Summary="Acceso / Claves",
        Description="Transferencias",
        **{"Categoría de Producto": "Cheques", "Assigned Group": "Otro routing"},
    )
    changed["Detailed Description"] = (
        "Categoría de Producto: Cheques\nDescripción: " + GOLDEN["incident"]["description"]
    )
    assert build_incident_text(changed).tolist() == build_incident_text(incidents).tolist()
    assert handler.classifications(
        ctx, handler.inputs(ctx, changed, "SOURCE")
    ) == handler.classifications(ctx, inputs)


def test_relink_only_reuses_categories_when_classification_pending_is_zero(helix):
    handler, ctx, frame, incidents, _ = helix
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    assert handler.export(ctx, inputs, only_linking=True)["saved_paths"] == []
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    handler.import_response(ctx, inputs, zipped(helix_response(request, "not-a-candidate")))
    before = handler.classifications(ctx, inputs)
    frame.loc[0, "Comment"] = "Nueva evidencia"
    updated = handler.inputs(ctx, incidents, "SOURCE")
    status = handler.status(ctx, updated)
    assert status["pending"] == 0 and status["link_pending"] == len(incidents)
    request = exported(handler.export(ctx, updated, only_linking=True)["saved_paths"])
    assert all(
        "classification" in row
        for name, batch in request.items()
        if name.startswith("incidents/")
        for row in batch["incidents"]
    )
    handler.import_response(ctx, updated, zipped(helix_response(request, "not-a-candidate")))
    assert handler.classifications(ctx, updated) == before
    assert handler.status(ctx, updated)["link_pending"] == 0


def test_single_secondary_is_contractual():
    with pytest.raises(ValueError):
        CompactComment(id="1", primary="c001", secondary=["c002", "c003"])


def test_argentina_reserve_calibration_uses_reserve_denominators(record_property):
    rows = GOLDEN["comments"]
    for reserve in ("c028", "c029"):
        selected = [row for row in rows if row["observed_primary"] == reserve]
        errors = sum(row["expected_primary"] != reserve for row in selected)
        record_property(f"baseline_{reserve}_errors", errors)
        record_property(f"baseline_{reserve}_error_rate", errors / len(selected))
        assert errors > 0  # labelled regression examples, not a quality threshold
    audit = audit_classifications(
        {row["id"]: row["text"] for row in rows},
        {
            row["id"]: {"primary_classification": GOLDEN["catalog"][row["observed_primary"]]}
            for row in rows
        },
        GOLDEN["catalog"],
    )
    assert audit["suspicious_rate"] == audit["suspicious_count"] / audit["insufficient_count"]
    assert (
        audit["uncovered_suspicious_rate"]
        == audit["uncovered_suspicious_count"] / audit["uncovered_count"]
    )


def test_argentina_candidate_recall_at_5_with_distractors(record_property):
    catalog = GOLDEN["catalog"]
    rows = [
        dict(
            ID=row["id"],
            Comment=row["text"],
            Palanca=catalog[row["expected_primary"]]["lever"],
            Subpalanca=catalog[row["expected_primary"]]["sublever"],
            Fecha="2026-09-01",
            NPS=2,
        )
        for row in GOLDEN["comments"]
    ]
    rows += [
        dict(
            ID=f"d{i}",
            Comment="token grisado consulta administrativa",
            Palanca="Información operativa",
            Subpalanca="Consulta",
            Fecha="2026-08-20",
            NPS=2,
        )
        for i in range(10)
    ]
    incident = pd.DataFrame(
        [
            {
                "Incident Number": "golden",
                "Detailed Description": GOLDEN["incident"]["description"],
                "Fecha": "2026-09-01",
                "Palanca": catalog["c010"]["lever"],
                "Subpalanca": catalog["c010"]["sublever"],
            }
        ]
    )
    candidates = retrieve_incident_candidates(pd.DataFrame(rows), incident, top_k_per_incident=5)
    expected = {row["comment_id"] for row in GOLDEN["links"] if row["expected"]}
    recall = len(expected & set(candidates.nps_id)) / len(expected)
    record_property("candidate_recall@5", recall)
    assert expected <= set(candidates.nps_id)


def test_argentina_labelled_link_precision_recall_at_import(helix, record_property):
    handler, ctx, frame, incidents, _ = helix
    labelled_comments = [
        row
        for row in GOLDEN["comments"]
        if row["id"] in {link["comment_id"] for link in GOLDEN["links"]}
    ]
    frame.drop(frame.index[len(labelled_comments) :], inplace=True)
    frame["Comment"] = [row["text"] for row in labelled_comments]
    incidents = incidents.iloc[:1].copy()
    incidents["Detailed Description"] = GOLDEN["incident"]["description"]
    incidents["Palanca"], incidents["Subpalanca"] = "Atención", "Resolución"
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    accepted = set()
    expected = {row["comment_id"] for row in GOLDEN["links"] if row["expected"]}
    for labelled in sorted(GOLDEN["links"], key=lambda row: row["expected"]):
        index = next(
            i for i, row in enumerate(labelled_comments) if row["id"] == labelled["comment_id"]
        )
        comment_id = inputs["comments"][index]["id"]
        response = helix_response(request, comment_id)
        links = response["results/000001.json"]["classifications"][0]["links"]
        assert len(links) == 1
        links[0].update(same_task=labelled["same_task"], same_symptom=labelled["same_symptom"])
        if labelled["expected"]:
            handler.import_response(ctx, inputs, zipped(response))
            accepted.add(labelled["comment_id"])
        else:
            with pytest.raises(ValueError, match="same_task=true y same_symptom=true"):
                handler.import_response(ctx, inputs, zipped(response))
    record_property("labelled_contract_link_precision", len(accepted & expected) / len(accepted))
    record_property("labelled_contract_link_recall", len(accepted & expected) / len(expected))
    assert accepted == expected
