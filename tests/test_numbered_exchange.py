from __future__ import annotations

import copy
from pathlib import Path

import pandas as pd
import pytest
from test_iteration28_taxonomy_helix import helix_response
from test_taxonomy_exchange import classifier_files, classifier_zip, exported, zipped
from test_taxonomy_exchange import exchange_fixture as exchange_fixture

from nps_lens.services.helix_exchange import HelixExchange
from nps_lens.services.taxonomy_service import TaxonomyService


@pytest.fixture(params=["classifier", "helix"])
def numbered(request, exchange, monkeypatch):
    handler, ctx, _, _ = exchange
    monkeypatch.setattr("nps_lens.services.taxonomy_exchange.CLASSIFICATION_BATCH_ROWS", 2)
    frame = pd.DataFrame(
        {
            "_business_key": [f"key-{i}" for i in range(9)],
            "Comment": [f"Texto {i}" for i in range(9)],
            "Palanca": "Atención",
            "Subpalanca": "Resolución",
            "Canal": "Web",
            "Fecha": pd.Timestamp("2026-09-01"),
        }
    )
    monkeypatch.setattr(handler.taxonomy, "source", lambda context: frame.copy())
    incidents = pd.DataFrame(
        {
            "Incident Number": [f"INC-{i}" for i in range(9)],
            "Detailed Description": "Mismo texto",
            "Submit Date": pd.Timestamp("2026-09-01"),
        }
    )
    kind = request.param
    if kind == "helix":
        handler = HelixExchange(handler.taxonomy, handler.downloads)

    def export(single_zip=False):
        return handler.export(
            ctx,
            "classifier" if kind == "classifier" else handler.inputs(ctx, incidents, "SOURCE"),
            single_zip=single_zip,
        )

    def response(path):
        files = exported(path)
        return (
            classifier_zip(classifier_files(files["manifest.json"], files))
            if kind == "classifier"
            else zipped(
                helix_response(files, handler.inputs(ctx, incidents, "SOURCE")["comments"][0]["id"])
            )
        )

    def receive(content):
        return (
            handler.import_response(ctx, content, "classifier")
            if kind == "classifier"
            else handler.import_response(ctx, handler.inputs(ctx, incidents, "SOURCE"), content)
        )

    def restart():
        nonlocal handler
        service = TaxonomyService(handler.repository, handler.taxonomy.equivalences_path)
        monkeypatch.setattr(service, "source", lambda context: frame.copy())
        handler = type(handler)(service, handler.downloads)

    return kind, handler, export, response, receive, restart


def test_all_numbered_files_survive_restart_and_import_out_of_order(numbered):
    kind, _, export, response, receive, restart = numbered
    result = export()
    label = "comentarios" if kind == "classifier" else "incidencias_helix"
    assert [Path(path).name for path in result["saved_paths"]] == [
        f"{i}_5_{label}.zip" for i in range(1, 6)
    ]
    assert all(
        Path(path).parent == Path(result["saved_directory"]) for path in result["saved_paths"]
    )
    requests = [exported(path) for path in result["saved_paths"]]
    assert len({r["manifest.json"]["job_id"] for r in requests}) == 1
    assert all(len(r["manifest.json"]["batches"]) == 1 for r in requests)
    if kind == "helix":
        evidence = [
            {key: value for key, value in r.items() if key.startswith("comments/")}
            for r in requests
        ]
        assert all(e == evidence[0] for e in evidence)
        assert evidence[0] == {}
    for index in [4, 1, 0, 3, 2]:
        restart()
        content = response(result["saved_paths"][index])
        status = receive(content)
        assert receive(content) == status
    assert export()["saved_paths"] == []
    progress = status["progress"] if kind == "classifier" else status
    assert progress["received"] == 9 and progress["pending"] == 0


def test_reexport_is_pending_only_without_expiring_sibling_files(numbered):
    kind, _, export, response, receive, restart = numbered
    first = export()
    receive(response(first["saved_paths"][0]))
    second = export()
    assert len(second["saved_paths"]) == 4
    field = "comments" if kind == "classifier" else "incidents"
    first_rows = exported(first["saved_paths"][0])[f"{field}/000001.json"][field]
    sent = [
        row
        for path in second["saved_paths"]
        for key, payload in exported(path).items()
        if key.startswith(f"{field}/")
        for row in payload[field]
    ]
    assert len(sent) == 7
    key = "Comment" if kind == "classifier" else "id"
    assert not {row[key] for row in sent}.intersection(row[key] for row in first_rows)
    restart()
    for path in first["saved_paths"][1:]:
        receive(response(path))
    assert export()["saved_paths"] == []


def test_zip_cannot_submit_sibling_results_or_altered_manifest(numbered):
    _, _, export, response, receive, _ = numbered
    result = export()
    from nps_lens.services.taxonomy_exchange import read_zip

    first = read_zip(response(result["saved_paths"][0]))
    second = read_zip(response(result["saved_paths"][1]))
    wrong_batch = {
        "manifest.json": first["manifest.json"],
        "results/000002.json": second["results/000002.json"],
    }
    with pytest.raises(ValueError, match="ZIP|desconocido"):
        receive(zipped(wrong_batch))
    altered = copy.deepcopy(first)
    altered["manifest.json"]["batches"][0]["count"] += 1
    with pytest.raises(ValueError, match="manifiesto"):
        receive(zipped(altered))
    assert receive(zipped(first))


def test_failed_series_write_leaves_no_partial_download_or_registered_job(numbered, monkeypatch):
    kind, handler, export, _, _, _ = numbered
    from nps_lens.services import taxonomy_exchange

    original = taxonomy_exchange.persist_download

    def fail_second(content, name, directory):
        if name.startswith("2_"):
            raise OSError("Disco lleno")
        return original(content, name, directory)

    monkeypatch.setattr(taxonomy_exchange, "persist_download", fail_second)
    with pytest.raises(OSError, match="Disco lleno"):
        export()
    assert list(handler.downloads.iterdir()) == []
    table = "taxonomy_exchange" if kind == "classifier" else "helix_exchange"
    with handler.repository._connect() as db:
        assert db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0


def test_single_zip_partial_delivery_restart_and_mode_switch_preserve_semantics(numbered):
    import zipfile

    from nps_lens.services.taxonomy_exchange import read_zip
    from nps_lens.services.taxonomy_prompts import PROJECT_INSTRUCTIONS

    kind, _, export, response, receive, restart = numbered
    numbered_export = export()
    result = export(single_zip=True)
    assert result["single_zip"] is True
    assert len(result["saved_paths"]) == 1 and result["batches"] == 5
    source = exported(result["saved_paths"][0])
    before = exported(numbered_export["saved_paths"])
    for name, payload in before.items():
        if name == "manifest.json":
            assert {k: v for k, v in payload.items() if k != "job_id"} == {
                k: v for k, v in source[name].items() if k != "job_id"
            }
        else:
            assert source[name] == payload
    with zipfile.ZipFile(result["saved_paths"][0]) as archive:
        assert (
            archive.read("INSTRUCCIONES.txt").decode() == PROJECT_INSTRUCTIONS[f"{kind}_single_zip"]
        )
    complete = read_zip(response(result["saved_paths"][0]))
    manifest = complete["manifest.json"]
    for index in [4, 1, 0, 3, 2]:
        restart()
        key = f"results/{index + 1:06d}.json"
        partial = zipped({"manifest.json": manifest, key: complete[key]})
        status = receive(partial)
        assert receive(partial) == status
        if index == 4:
            # Re-export after restart excludes finished rows, even when switching mode.
            resumed = export(single_zip=True)
            assert resumed["batches"] == 4
            field = "comments" if kind == "classifier" else "incidents"
            identity = "Comment" if kind == "classifier" else "id"
            sent = {
                row[identity]
                for name, payload in exported(resumed["saved_paths"][0]).items()
                if name.startswith(f"{field}/")
                for row in payload[field]
            }
            assert not sent.intersection(
                row[identity] for row in source[f"{field}/000005.json"][field]
            )
    progress = status["progress"] if kind == "classifier" else status
    assert progress["received"] == 9 and progress["pending"] == 0
    assert export(single_zip=True)["saved_paths"] == []
    assert export()["saved_paths"] == []


def test_single_zip_rejects_altered_partial_manifest_and_unknown_batch(numbered):
    from nps_lens.services.taxonomy_exchange import read_zip

    _, _, export, response, receive, _ = numbered
    package = export(single_zip=True)
    files = read_zip(response(package["saved_paths"][0]))
    altered = copy.deepcopy(files["manifest.json"])
    altered["batches"][0]["sha256"] = "altered"
    with pytest.raises(ValueError, match="manifiesto"):
        receive(
            zipped({"manifest.json": altered, "results/000001.json": files["results/000001.json"]})
        )
    with pytest.raises(ValueError, match="desconocido|esperado"):
        receive(
            zipped(
                {
                    "manifest.json": files["manifest.json"],
                    "results/999999.json": files["results/000001.json"],
                }
            )
        )


def test_single_zip_limit_failure_is_atomic(numbered, monkeypatch):
    kind, handler, export, _, _, _ = numbered
    monkeypatch.setattr("nps_lens.services.taxonomy_exchange.MAX_EXPANDED_BYTES", 1)
    with pytest.raises(ValueError, match="límites"):
        export(single_zip=True)
    assert list(handler.downloads.iterdir()) == []
    table = "taxonomy_exchange" if kind == "classifier" else "helix_exchange"
    with handler.repository._connect() as db:
        assert db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
