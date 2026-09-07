from __future__ import annotations

from dataclasses import replace
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from nps_lens.analytics.nps_helix_link import build_nps_text, link_incidents_to_nps_topics
from nps_lens.analytics.taxonomy import (
    TaxonomyConfig,
    complete,
    detect_taxonomy,
    discover,
    signature,
)
from nps_lens.api.app import create_app
from nps_lens.domain.models import UploadContext
from nps_lens.domain.normalization import EquivalenceRegistry
from nps_lens.ingest.nps_thermal import read_nps_thermal_excel
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.nps_service import NpsService
from nps_lens.services.taxonomy_service import TaxonomyService
from nps_lens.settings import Settings


def corpus() -> pd.DataFrame:
    rows = []
    for pal, sub, words in [
        ("Pagos", "Transferir", "transferencia bancaria pago destinatario"),
        ("Pagos", "Firma", "firma token autorizar operacion"),
        ("Acceso", "Login", "acceso login contrasena sesion"),
        ("Acceso", "Clave", "clave recuperar credencial seguridad"),
    ]:
        for i in range(24):
            rows.append(
                {
                    "ID": str(len(rows)),
                    "NPS": i % 11,
                    "Fecha": "2026-03-01",
                    "Comment": f"{words} solicitud numero {i}",
                    "Palanca": pal if i < 20 else "",
                    "Subpalanca": sub if i < 20 else "",
                    "Canal": "Otros",
                    "_business_key": str(len(rows)),
                }
            )
    return pd.DataFrame(rows)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return replace(
        Settings.from_env(),
        data_dir=tmp_path,
        database_path=tmp_path / "nps.sqlite3",
        equivalences_path=tmp_path / "equivalences.json",
        auth_mode="local",
        default_service_origin="Bank",
        default_service_origin_n1="Web",
        allowed_service_origins=["Bank"],
        allowed_service_origin_n1={"Bank": ["Web"]},
    )


@pytest.fixture
def service(settings: Settings) -> tuple[TaxonomyService, UploadContext]:
    repo = SqliteNpsRepository(settings.database_path)
    context = UploadContext("Bank", "Web")
    content = BytesIO()
    corpus().drop(columns="_business_key").to_excel(content, index=False)
    result = NpsService(repo, settings).ingest_excel(
        filename="test.xlsx", payload=content.getvalue(), context=context
    )
    assert result["status"] == "completed", result
    return TaxonomyService(repo, settings.equivalences_path), context


@pytest.mark.parametrize("state", ["COMPLETE", "PARTIAL", "MISSING", "NO_TEXT"])
def test_ingest_taxonomy_states_and_original_text(tmp_path: Path, state: str) -> None:
    frame = corpus().iloc[:3].copy()
    frame["Comment"] = ["  transferencia\n urgente  "] * 3
    frame["Canal"] = ["  Otros "] * 3
    if state == "PARTIAL":
        frame.loc[0, "Palanca"] = ""
    if state == "MISSING":
        frame = frame.drop(columns=["Palanca", "Subpalanca"])
    if state == "NO_TEXT":
        frame["Comment"] = ""
    file = tmp_path / "nps.xlsx"
    frame.to_excel(file, index=False)
    result = read_nps_thermal_excel(str(file), service_origin="Bank", service_origin_n1="Web")
    assert not any(issue.level == "ERROR" for issue in result.issues)
    assert result.meta["taxonomy"]["state"] == state
    assert result.df.Comment.tolist() == frame.Comment.tolist()
    assert result.df.source_channel.tolist() == frame.Canal.tolist()


def test_business_identity_ignores_taxonomy_and_channel(tmp_path: Path) -> None:
    frame = corpus().iloc[:1].drop(columns="ID")
    file = tmp_path / "nps.xlsx"
    frame.to_excel(file, index=False)
    first = read_nps_thermal_excel(str(file), service_origin="Bank", service_origin_n1="Web")
    frame[["Palanca", "Subpalanca", "Canal"]] = "Changed"
    frame.to_excel(file, index=False)
    second = read_nps_thermal_excel(str(file), service_origin="Bank", service_origin_n1="Web")
    assert first.df._business_key.tolist() == second.df._business_key.tolist()


def test_scoped_equivalences_are_explicit_and_never_touch_free_text() -> None:
    registry = EquivalenceRegistry.from_dict(
        {
            "dimensions": {
                "nps.Canal": [{"canonical": "Otros Canales", "aliases": ["Otros"]}],
                "helix.Service": [{"canonical": "Acceso", "aliases": ["Otros"]}],
            }
        }
    )
    frame = pd.DataFrame(
        {
            "Canal": ["Otros", "OTROS"],
            "Service": ["Otros", "Otros"],
            "Comment": ["Otros", "  Otros\n"],
            "Description": ["Otros", "Otros"],
        }
    )
    nps = registry.apply("nps", frame)
    helix = registry.apply("helix", frame)
    assert nps.Canal.tolist() == ["Otros Canales", "OTROS"]
    assert nps.Service.tolist() == ["Otros", "Otros"]
    assert helix.Service.tolist() == ["Acceso", "Acceso"]
    pd.testing.assert_series_equal(frame.Comment, nps.Comment)
    pd.testing.assert_series_equal(frame.Description, helix.Description)
    with pytest.raises(ValueError):
        EquivalenceRegistry.from_dict({"dimensions": {"nps.Comment": []}})
    assert registry.collision_report("nps.Canal", ["OTROS", "Otros"])


def test_completed_preserves_humans_and_learns_separable_comments() -> None:
    frame = corpus()
    original = frame.Palanca.ne("")
    result = complete(frame, TaxonomyConfig())
    assert np.array(result["lever"])[original].tolist() == frame.loc[original, "Palanca"].tolist()
    assert (
        np.array(result["sublever"])[original].tolist()
        == frame.loc[original, "Subpalanca"].tolist()
    )
    assert all(result["lever"])
    assert all(result["sublever"])
    assert all(row["macro_f1"] == 1 for row in result["quality"])
    changed = frame.copy()
    changed.NPS = 10 - changed.NPS
    assert complete(changed, TaxonomyConfig()) == result


def test_completed_rejects_uncertain_unseen_and_insufficient_training() -> None:
    frame = corpus()
    frame.loc[frame.Palanca.eq(""), "Comment"] = "astronomia saturno galaxia"
    result = complete(frame, TaxonomyConfig())
    assert np.array(result["lever"])[frame.Palanca.eq("")].tolist() == [""] * 16
    tiny = complete(frame.iloc[:3], TaxonomyConfig())
    assert tiny["quality"][0]["macro_f1"] is None


def test_discovered_deterministic_and_independent_of_score() -> None:
    frame = corpus()
    config = TaxonomyConfig(clusters=4, subclusters=2)
    first = discover(frame, config)
    second = discover(frame.assign(NPS=10), config)
    assert first == second
    assert len(set(first["lever"])) == 4
    assert all(node["label"].startswith("Tema · ") for node in first["nodes"])
    assert all(1 <= len(node["examples"]) <= 5 for node in first["nodes"])
    assert signature(frame, "DISCOVERED", config, "a") == signature(
        frame.assign(NPS=9), "DISCOVERED", config, "b"
    )


@pytest.mark.parametrize(
    "frame",
    [
        pd.DataFrame(),
        pd.DataFrame({"Comment": [""], "Palanca": [""], "Subpalanca": [""]}),
        pd.DataFrame({"Comment": ["transferencia"], "Palanca": [""], "Subpalanca": [""]}),
    ],
)
def test_empty_small_engines(frame: pd.DataFrame) -> None:
    assert len(discover(frame, TaxonomyConfig())["lever"]) == len(frame)
    assert len(complete(frame, TaxonomyConfig())["lever"]) == len(frame)
    assert detect_taxonomy(frame)["rows"] == len(frame)


def test_resolver_all_modes_and_cached_generation(service) -> None:
    tax, ctx = service
    source = tax.source(ctx)
    before = source.copy()
    assert tax.resolve(ctx, mode="SOURCE").Canal.iloc[0] == "Otros"
    assert tax.resolve(ctx, mode="NORMALIZED").Canal.iloc[0] == "Otros Canales"
    for mode in ["COMPLETED", "DISCOVERED"]:
        first = tax.generate(ctx, mode, TaxonomyConfig())
        assert not first["cache_hit"]
        with (
            patch(
                "nps_lens.services.taxonomy_service.complete",
                side_effect=AssertionError("retrained"),
            ),
            patch(
                "nps_lens.services.taxonomy_service.discover",
                side_effect=AssertionError("reclustered"),
            ),
        ):
            assert tax.generate(ctx, mode, TaxonomyConfig())["cache_hit"]
            tax.configure(ctx, {"active": mode})
            assert len(tax.resolve(ctx)) == len(source)
            assert tax.explore(ctx, mode)["total"] > 0
    pd.testing.assert_frame_equal(tax.source(ctx), before)
    assert tax.compare(ctx, "COMPLETED", "DISCOVERED")["rows"]


def test_cache_invalidation_uses_only_relevant_inputs(service) -> None:
    tax, ctx = service
    for mode in ["COMPLETED", "DISCOVERED"]:
        tax.generate(ctx, mode, TaxonomyConfig())
    registry = EquivalenceRegistry.load(tax.equivalences_path).to_dict()
    registry["dimensions"]["helix.Service"] = [{"canonical": "Banco", "aliases": ["Bank"]}]
    EquivalenceRegistry.from_dict(registry).save(tax.equivalences_path)
    assert all(t["available"] for t in tax.studio(ctx)["taxonomies"])
    registry["dimensions"]["nps.Palanca"] = [{"canonical": "Pagos Nuevo", "aliases": ["Pagos"]}]
    EquivalenceRegistry.from_dict(registry).save(tax.equivalences_path)
    available = {t["mode"]: t["available"] for t in tax.studio(ctx)["taxonomies"]}
    assert not available["COMPLETED"] and available["DISCOVERED"]
    with tax.repository._connect() as connection:
        connection.execute("UPDATE records SET comment_text = 'changed'")
    assert not next(t for t in tax.studio(ctx)["taxonomies"] if t["mode"] == "DISCOVERED")[
        "available"
    ]


@pytest.mark.parametrize(
    "policy,number", [("ACTIVE_ONLY", 1), ("SOURCE_AND_ACTIVE", 2), ("ALL_AVAILABLE", 4)]
)
def test_snapshots_restore_frozen_assignments_without_sklearn(service, policy, number) -> None:
    tax, ctx = service
    tax.generate(ctx, "COMPLETED", TaxonomyConfig())
    tax.generate(ctx, "DISCOVERED", TaxonomyConfig(clusters=4))
    tax.configure(ctx, {"active": "DISCOVERED", "default": "DISCOVERED", "policy": policy})
    before = tax.resolve(ctx)
    snapshot = tax.snapshot(ctx)
    assert len(snapshot["taxonomies"]) == number
    with patch(
        "nps_lens.services.taxonomy_service.discover", side_effect=AssertionError("reclustered")
    ):
        tax.restore(ctx, snapshot)
        pd.testing.assert_series_equal(tax.resolve(ctx).Palanca, before.Palanca, check_dtype=False)
        assert tax.studio(ctx)["restored"]
    tax.resume_local(ctx)
    assert not tax.studio(ctx)["restored"]


@pytest.mark.parametrize("mode", ["SOURCE", "NORMALIZED", "COMPLETED", "DISCOVERED"])
def test_causal_path_accepts_every_lens(service, mode) -> None:
    tax, ctx = service
    if mode in ("COMPLETED", "DISCOVERED"):
        tax.generate(ctx, mode, TaxonomyConfig())
    frame = tax.resolve(ctx, mode=mode)
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC01"],
            "Fecha": pd.to_datetime(["2026-03-01"]),
            "Description": ["transferencia bancaria pago destinatario"],
            "Service": ["Pagos"],
        }
    )
    _, links = link_incidents_to_nps_topics(frame, helix, min_similarity=0.05)
    assert not links.empty
    no_labels = frame.drop(columns=["Palanca", "Subpalanca"])
    assert build_nps_text(no_labels).iloc[0] == frame.Comment.iloc[0]
    _, links = link_incidents_to_nps_topics(no_labels, helix, min_similarity=0.05)
    assert not links.empty


def test_taxonomy_api_round_trip(settings, service) -> None:
    _, ctx = service
    client = TestClient(create_app(settings))
    params = {"service_origin": ctx.service_origin, "service_origin_n1": ctx.service_origin_n1}
    assert client.get("/api/taxonomy", params=params).status_code == 200
    generated = client.post(
        "/api/taxonomy/generate",
        params=params,
        json={"mode": "DISCOVERED", "config": {"clusters": 4}},
    )
    assert generated.status_code == 200, generated.text
    assert (
        client.put(
            "/api/taxonomy/settings", params=params, json={"default": "DISCOVERED"}
        ).status_code
        == 200
    )
    snapshot = client.get("/api/taxonomy/snapshot", params=params)
    assert snapshot.status_code == 200
    restored = client.post(
        "/api/taxonomy/restore",
        params=params,
        files={"file": ("snapshot.json", snapshot.content, "application/json")},
    )
    assert restored.status_code == 200, restored.text
    assert restored.json()["active"] == "DISCOVERED"
    assert client.get("/api/settings/equivalences", params=params).status_code == 200


def test_migration_recovers_source_and_preserves_record_identity(service, settings) -> None:
    tax, ctx = service
    repo = tax.repository
    expected = repo.load_records_df(ctx)
    with repo._connect() as connection:
        connection.execute("UPDATE records SET source_lever = 'LOST', source_sublever = 'LOST', source_preserved = 0")
        connection.execute("PRAGMA user_version = 0")
    repo.migrate_source_identity(settings.data_dir / "uploads")
    recovered = repo.load_records_df(ctx)
    assert recovered.source_lever.tolist() == expected.source_lever.tolist()
    assert recovered._business_key.tolist() == expected._business_key.tolist()
    assert recovered.source_preserved.eq(1).all()


@pytest.mark.parametrize("mode", ["SOURCE", "NORMALIZED", "COMPLETED", "DISCOVERED"])
def test_snapshot_default_resolves_without_changing_active(service, mode) -> None:
    tax, ctx = service
    if mode in ("COMPLETED", "DISCOVERED"):
        tax.generate(ctx, mode, TaxonomyConfig())
    tax.configure(ctx, {"active": "SOURCE", "default": mode})
    with tax.snapshot_lens(ctx):
        assert tax.resolve(ctx).attrs["taxonomy_mode"] == mode
        snapshot = tax.snapshot(ctx)
        assert snapshot["active"] == mode
    assert tax.resolve(ctx).attrs["taxonomy_mode"] == "SOURCE"
