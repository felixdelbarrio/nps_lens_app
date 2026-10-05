from __future__ import annotations

import io
import json
import zipfile
from dataclasses import replace

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from nps_lens.api.app import create_app
from nps_lens.domain.models import UploadContext
from nps_lens.services.classification_protocol import label_criterion, taxonomy_fingerprint
from nps_lens.services.taxonomy_discovery import TaxonomyDiscoveryError
from nps_lens.services.taxonomy_exchange import TaxonomyExchange, encode, read_zip
from nps_lens.services.taxonomy_prompts import (
    FALLBACK_LEVER,
    FALLBACK_SUBLEVERS,
    INSTRUCTIONS_VERSIONS,
)
from nps_lens.settings import Settings

TAXONOMY = {
    "taxonomy": [
        {"lever": "Atención", "sublevers": ["Resolución"]},
        {"lever": FALLBACK_LEVER, "sublevers": list(FALLBACK_SUBLEVERS)},
        {"lever": "Velocidad", "sublevers": ["Espera"]},
    ]
}


def zipped(files):
    result = io.BytesIO()
    with zipfile.ZipFile(result, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in files.items():
            archive.writestr(name, encode(payload))
    return result.getvalue()


def exported(path):
    # Consolidate a series only for the existing multi-batch import regression cases.
    # Numbered ZIPs are exercised individually by test_numbered_exchange.
    if isinstance(path, list):
        combined = {}
        batches = []
        for member in path:
            files = exported(member)
            batches.extend(files["manifest.json"]["batches"])
            combined.update(files)
        combined["manifest.json"]["batches"] = batches
        return combined
    with zipfile.ZipFile(path) as archive:
        return {
            name: json.loads(archive.read(name))
            for name in archive.namelist()
            if name.endswith(".json")
        }


@pytest.fixture(name="exchange")
def exchange_fixture(tmp_path, monkeypatch):
    # Settings.from_env() intentionally requires an explicit service-origin hierarchy.
    # Keep this shared fixture hermetic instead of relying on a developer/CI .env.
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "Bank")
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", '{"Bank":["Web"]}')
    monkeypatch.setenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN", "Bank")
    monkeypatch.setenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1", "Web")
    settings = replace(
        Settings.from_env(),
        database_path=tmp_path / "test.db",
        dotenv_path=tmp_path / ".env",
        data_dir=tmp_path,
        equivalences_path=tmp_path / "equivalences.json",
        auth_mode="local",
    )
    # Small batches keep these pre-existing partial/restart scenarios lightweight.
    # Production limits are exercised without this override in iteration31 tests.
    monkeypatch.setattr("nps_lens.services.taxonomy_exchange.CLASSIFICATION_BATCH_ROWS", 200)
    app = create_app(settings)
    service = app.state.dashboard_service.taxonomy
    frame = pd.DataFrame(
        {
            "_business_key": [f"private-{i:05d}" for i in range(405)],
            "Comment": ["No resuelven mi problema. á漢字 " + str(i) for i in range(405)],
            "Palanca": "",
            "Subpalanca": "",
            "Canal": "Web",
        }
    )
    monkeypatch.setattr(service, "source", lambda context: frame.copy())
    # API Downloads writes are isolated; never pollute real user downloads in tests.
    monkeypatch.setattr(
        "nps_lens.api.app.normalize_downloads_path",
        lambda value, create=False: str(tmp_path / "Downloads"),
    )
    return (
        TaxonomyExchange(service, tmp_path / "Downloads"),
        UploadContext("Bank", "Web", ""),
        frame,
        TestClient(app),
    )


def classifier_files(manifest, inputs):
    return {
        "manifest": manifest,
        "results": {
            name.removeprefix("comments/").removesuffix(".json"): {
                "classifications": [
                    {
                        "id": row["id"],
                        "primary": next(
                            key
                            for key, pair in inputs["taxonomy.json"]["categories"].items()
                            if pair["lever"] == "Atención" and pair["sublever"] == "Resolución"
                        ),
                        "secondary": [],
                    }
                    for row in payload["comments"]
                ]
            }
            for name, payload in inputs.items()
            if name.startswith("comments/")
        },
    }


def reviewed_taxonomy(taxonomy, request):
    quotes = [
        row["Comment"]
        for name, value in request.items()
        if name.startswith("comments/")
        for row in value["comments"]
        if row["Comment"].strip()
    ][:1]
    return {
        "taxonomy": [
            {
                "lever": b["lever"],
                "sublevers": [
                    (
                        {"name": sub, "criterion": label_criterion(b["lever"], sub)}
                        if isinstance(sub, str)
                        else sub
                    )
                    for sub in b["sublevers"]
                ],
            }
            for b in taxonomy["taxonomy"]
        ],
        "review": {
            "quotes": quotes,
            "reason": "Fronteras contrastadas con la narrativa del corpus.",
        },
    }


def designer_zip(handler, context, taxonomy=TAXONOMY):
    request = exported(handler.export(context, "designer")["saved_path"])
    return zipped(
        {
            "manifest.json": request["manifest.json"],
            "taxonomy.json": reviewed_taxonomy(taxonomy, request),
        }
    )


def classifier_zip(response):
    return zipped(
        {
            "manifest.json": response["manifest"],
            **{f"results/{key}.json": value for key, value in response["results"].items()},
        }
    )


def test_zip_api_roundtrip_restart_partial_atomic_and_idempotent(exchange):
    handler, context, frame, client = exchange
    params = {"service_origin": "Bank", "service_origin_n1": "Web"}
    response = client.post("/api/taxonomy/discovery/designer/export", params=params)
    assert response.status_code == 200, response.text
    request = exported(response.json()["saved_path"])
    assert request["manifest.json"]["stage"] == "designer"
    assert len(request["manifest.json"]["batches"]) == 3
    sent = [
        row
        for name, payload in request.items()
        if name.startswith("comments/")
        for row in payload["comments"]
    ]
    assert [row["Comment"] for row in sent] == frame["Comment"].tolist()
    assert all(set(row) == {"id", "Comment"} for row in sent)
    assert "private-" not in json.dumps(request)
    result = client.post(
        "/api/taxonomy/discovery/designer/import",
        params=params,
        files={
            "file": (
                "response.zip",
                zipped(
                    {
                        "manifest.json": request["manifest.json"],
                        "taxonomy.json": reviewed_taxonomy(TAXONOMY, request),
                    }
                ),
                "application/zip",
            )
        },
    )
    assert result.status_code == 200, result.text
    assert "saved_path" not in result.json()
    assert (
        client.put(
            "/api/taxonomy/settings", params=params, json={"active": "DISCOVERED"}
        ).status_code
        == 200
    )
    response = client.post("/api/taxonomy/discovery/classifier/export", params=params)
    assert response.status_code == 200, response.text
    classification = exported(response.json()["saved_paths"])
    assert list(classification["taxonomy.json"]["categories"].values()) == [
        {
            "lever": branch["lever"],
            "sublever": sub,
            "criterion": label_criterion(branch["lever"], sub),
        }
        for branch in TAXONOMY["taxonomy"]
        for sub in branch["sublevers"]
    ]
    files = classifier_files(classification["manifest.json"], classification)
    partial = {"manifest": files["manifest"], "results": {"000001": files["results"]["000001"]}}
    first = handler.import_response(context, classifier_zip(partial), "classifier")
    assert first["received"] == 1 and first["stage"] == "classifier"
    assert first["progress"] == {"total": 405, "received": 200, "pending": 205}
    assert handler.taxonomy.resolve(context, mode="DISCOVERED").Palanca.ne("").sum() == 200
    handler = TaxonomyExchange(handler.taxonomy, handler.downloads)
    assert handler.import_response(context, classifier_zip(partial), "classifier")["received"] == 1
    broken = {**files, "results": {**files["results"], "000003": {"classifications": []}}}
    with pytest.raises(ValueError):
        handler.import_response(context, classifier_zip(broken), "classifier")
    assert handler.import_response(context, classifier_zip(partial), "classifier")["received"] == 1
    complete = handler.import_response(context, classifier_zip(files), "classifier")
    assert complete["stage"] == "complete" and complete["received"] == 3
    assert (
        handler.import_response(context, classifier_zip(files), "classifier")["stage"] == "complete"
    )
    resolved = handler.taxonomy.resolve(context, frame, "DISCOVERED")
    assert len(resolved) == 405 and resolved["Palanca"].eq("Atención").all()
    assert resolved["Subpalanca"].eq("Resolución").all()


def test_changed_corpus_and_foreign_job_rejected(exchange):
    handler, context, frame, _ = exchange
    handler.import_response(context, designer_zip(handler, context), "designer")
    handler.taxonomy.configure(context, {"accept_proposal": True})
    handler.taxonomy.configure(context, {"active": "DISCOVERED"})
    original = exported(handler.export(context, "classifier")["saved_paths"])
    response = classifier_files(original["manifest.json"], original)
    with pytest.raises(ValueError, match="dataset"):
        handler.import_response(
            UploadContext("Other", "Web", ""), classifier_zip(response), "classifier"
        )
    frame.loc[0, "Comment"] = "Changed"
    with pytest.raises(ValueError, match="corpus"):
        handler.import_response(context, classifier_zip(response), "classifier")


def test_exchange_retains_inflight_jobs(exchange):
    handler, context, _, _ = exchange
    handler.import_response(context, designer_zip(handler, context), "designer")
    handler.taxonomy.configure(context, {"accept_proposal": True})
    handler.taxonomy.configure(context, {"active": "DISCOVERED"})
    jobs = [handler.export(context, "classifier") for _ in range(4)]
    with handler.repository._connect() as db:
        assert (
            db.execute(
                "SELECT COUNT(*) FROM taxonomy_exchange WHERE json_extract(payload, '$.stage') = 'classifier'"
            ).fetchone()[0]
            == 4
        )
    oldest = exported(jobs[0]["saved_paths"])
    handler.import_response(
        context, classifier_zip(classifier_files(oldest["manifest.json"], oldest)), "classifier"
    )


@pytest.mark.parametrize(
    "name", ["../taxonomy.json", "/taxonomy.json", "a\\b.json", "a//b.json", "script.py", "folder/"]
)
def test_unsafe_zip_names(name):
    with pytest.raises(ValueError):
        read_zip(zipped({name: {}}))


def test_duplicate_json_and_duplicate_members():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("manifest.json", '{"a":1,"a":2}')
    with pytest.raises(ValueError, match="duplicadas"):
        read_zip(output.getvalue())
    with pytest.warns(UserWarning), zipfile.ZipFile(output, "a") as archive:
        archive.writestr("manifest.json", "{}")
    with pytest.raises(ValueError, match="duplicados"):
        read_zip(output.getvalue())


def test_invalid_taxonomy_and_unknown_category_leave_state_unchanged(exchange):
    handler, context, _, _ = exchange
    with pytest.raises((ValueError, TaxonomyDiscoveryError)):
        handler.import_response(
            context, designer_zip(handler, context, {"taxonomy": []}), "designer"
        )
    with pytest.raises(ValueError):
        handler.import_response(context, zipped({"taxonomy.json": TAXONOMY}), "designer")
        handler.taxonomy.configure(context, {"accept_proposal": True})
        handler.taxonomy.configure(context, {"active": "DISCOVERED"})
    handler.import_response(context, designer_zip(handler, context), "designer")
    handler.taxonomy.configure(context, {"accept_proposal": True})
    handler.taxonomy.configure(context, {"active": "DISCOVERED"})
    inputs = exported(handler.export(context, "classifier")["saved_paths"])
    files = classifier_files(inputs["manifest.json"], inputs)
    files["results"]["000001"]["classifications"][0]["primary"] = "Inventada"
    with pytest.raises((ValueError, TaxonomyDiscoveryError)):
        handler.import_response(context, classifier_zip(files), "classifier")
    assert "DISCOVERED" not in handler.taxonomy.state(context)["artifacts"]


@pytest.mark.parametrize("stage", ["designer", "classifier", "helix"])
def test_all_projects_share_strict_zip_validation(stage):
    from nps_lens.services.taxonomy_exchange import read_response

    manifest = {
        "schema_version": {
            "helix": "nps-lens-helix/5",
            "classifier": "nps-lens-comments/5",
            "designer": "nps-lens-taxonomy/4",
        }[stage],
        "stage": stage,
        "instructions_version": INSTRUCTIONS_VERSIONS[stage],
        "job_id": "test",
    }
    member = "taxonomy.json" if stage == "designer" else "results/000001.json"
    response = {"manifest.json": manifest, member: {}}
    assert read_response(zipped(response), stage) == (manifest, {member: {}})
    with pytest.raises(ValueError, match="ZIP"):
        read_response(encode(response), stage)
    with pytest.raises(ValueError, match="proyecto"):
        read_response(zipped(response), "classifier" if stage != "classifier" else "designer")
    with pytest.raises(ValueError, match="únicamente"):
        read_response(zipped({**response, "extra.json": {}}), stage)
    malformed = io.BytesIO()
    with zipfile.ZipFile(malformed, "w") as archive:
        archive.writestr("manifest.json", encode(manifest))
        archive.writestr(member, '{"a":1,"a":2}')
    with pytest.raises(ValueError, match="duplicadas"):
        read_response(malformed.getvalue(), stage)


def test_designer_zip_rejects_changed_dataset_without_mutation(exchange):
    handler, context, frame, _ = exchange
    response = designer_zip(handler, context)
    frame.loc[0, "Comment"] = "Changed"
    with pytest.raises(ValueError, match="corpus"):
        handler.import_response(context, response, "designer")
        handler.taxonomy.configure(context, {"accept_proposal": True})
        handler.taxonomy.configure(context, {"active": "DISCOVERED"})
    assert "discovered_taxonomy" not in handler.taxonomy.state(context)


def test_classifier_tolerates_known_nonsemantic_llm_annotations(exchange):
    handler, ctx, _, _ = exchange
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"accept_proposal": True})
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    response = classifier_files(request["manifest.json"], request)
    for row in response["results"]["000001"]["classifications"]:
        row["evidence"] = {"quotes": ["texto"], "reason": "explicación no contractual"}
    result = handler.import_response(ctx, classifier_zip(response), "classifier")
    assert result["received"] == len(request["manifest.json"]["batches"])


def test_classifier_drops_fallback_categories_from_secondary_topics(exchange):
    handler, ctx, _, _ = exchange
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"accept_proposal": True})
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    response = classifier_files(request["manifest.json"], request)
    categories = request["taxonomy.json"]["categories"]
    fallback = next(key for key, value in categories.items() if value["lever"] == FALLBACK_LEVER)
    row = response["results"]["000001"]["classifications"][0]
    row["secondary"] = [fallback]

    result = handler.import_response(ctx, classifier_zip(response), "classifier")

    assert result["received"] == len(request["manifest.json"]["batches"])
    resolved = handler.assignments(ctx, handler._frame(ctx), "DISCOVERED")
    first = resolved[next(iter(resolved))]
    assert first["secondary_classifications"] == []


def test_classifier_still_rejects_unknown_schema_drift(exchange):
    handler, ctx, _, _ = exchange
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"accept_proposal": True})
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    response = classifier_files(request["manifest.json"], request)
    response["results"]["000001"]["classifications"][0]["entity"] = "unexpected"
    with pytest.raises(ValueError, match="formato inválido"):
        handler.import_response(ctx, classifier_zip(response), "classifier")


def test_designer_proposal_does_not_replace_active_taxonomy(exchange):
    handler, context, _, _ = exchange
    handler.import_response(context, designer_zip(handler, context), "designer")
    handler.taxonomy.configure(context, {"accept_proposal": True})
    handler.taxonomy.configure(context, {"active": "DISCOVERED"})
    before = handler.taxonomy.catalog(context, "DISCOVERED")
    proposed = {
        "taxonomy": [*TAXONOMY["taxonomy"], {"lever": "Acceso", "sublevers": ["Autenticación"]}]
    }
    result = handler.import_response(context, designer_zip(handler, context, proposed), "designer")
    assert result["proposed_discovered_fingerprint"]
    assert handler.taxonomy.catalog(context, "DISCOVERED") == before
    handler.taxonomy.configure(context, {"accept_proposal": True})
    handler.taxonomy.configure(context, {"active": "DISCOVERED"})
    assert handler.taxonomy.catalog(context, "DISCOVERED") != before


def test_proposal_requires_activation_and_late_classifier_keeps_new_active_version(exchange):
    handler, context, frame, _ = exchange
    frame["Palanca"] = "Atención"
    frame["Subpalanca"] = "Resolución"
    handler.import_response(context, designer_zip(handler, context), "designer")

    source_request = exported(handler.export(context, "classifier")["saved_paths"])
    assert source_request["manifest.json"]["taxonomy_mode"] == "SOURCE"
    assert source_request["manifest.json"]["taxonomy_fingerprint"] == taxonomy_fingerprint(
        handler.taxonomy.catalog(context, "SOURCE")
    )

    handler.taxonomy.configure(context, {"accept_proposal": True})

    handler.taxonomy.configure(context, {"active": "DISCOVERED"})
    old_request = exported(handler.export(context, "classifier")["saved_paths"])
    old_fingerprint = old_request["manifest.json"]["taxonomy_fingerprint"]
    assert old_request["manifest.json"]["taxonomy_mode"] == "DISCOVERED"

    replacement = {
        "taxonomy": [
            *TAXONOMY["taxonomy"],
            {"lever": "Acceso", "sublevers": ["Autenticación"]},
        ]
    }
    handler.import_response(context, designer_zip(handler, context, replacement), "designer")
    assert taxonomy_fingerprint(handler.taxonomy.catalog(context, "DISCOVERED")) == old_fingerprint
    handler.taxonomy.configure(context, {"accept_proposal": True})
    handler.taxonomy.configure(context, {"active": "DISCOVERED"})
    new_fingerprint = taxonomy_fingerprint(handler.taxonomy.catalog(context, "DISCOVERED"))
    assert new_fingerprint != old_fingerprint

    result = handler.import_response(
        context,
        classifier_zip(classifier_files(old_request["manifest.json"], old_request)),
        "classifier",
    )
    assert result["stage"] == "complete"
    state = handler.taxonomy.state(context)
    assert state["active"] == "DISCOVERED"
    assert "DISCOVERED" not in state.get("artifacts", {})
    assert taxonomy_fingerprint(handler.taxonomy.catalog(context, "DISCOVERED")) == new_fingerprint
