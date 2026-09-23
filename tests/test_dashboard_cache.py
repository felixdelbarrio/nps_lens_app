from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock

import pandas as pd

from nps_lens.domain.models import UploadContext
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.dashboard_service import DashboardService
from nps_lens.settings import Settings


def _service(tmp_path: Path) -> DashboardService:
    settings = Settings(
        data_dir=tmp_path / "data",
        database_path=tmp_path / "data" / "dashboard.sqlite3",
        frontend_dist_dir=tmp_path / "frontend-dist",
        frontend_public_dir=tmp_path / "frontend-public",
        api_host="127.0.0.1",
        api_port=8000,
        default_service_origin="BBVA México",
        default_service_origin_n1="Senda",
        allowed_service_origins=["BBVA México"],
        allowed_service_origin_n1={"BBVA México": ["Senda"]},
        log_level="INFO",
    )
    return DashboardService(SqliteNpsRepository(settings.database_path), settings)


def test_identical_dashboard_requests_are_computed_once(tmp_path: Path, monkeypatch) -> None:
    service = _service(tmp_path)
    context = UploadContext("BBVA México", "Senda", "")
    call_lock = Lock()
    calls = 0

    monkeypatch.setattr(service, "_data_revision", lambda _context: ("revision",))

    def build(**_kwargs: object) -> dict[str, object]:
        nonlocal calls
        with call_lock:
            calls += 1
        time.sleep(0.05)
        return {"result": "shared"}

    monkeypatch.setattr(service, "_build_nps_dashboard", build)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: service.nps_dashboard(context=context), range(2)))

    assert calls == 1
    assert results == [{"result": "shared"}, {"result": "shared"}]


def test_cache_clear_forces_recalculation(tmp_path: Path, monkeypatch) -> None:
    service = _service(tmp_path)
    context = UploadContext("BBVA México", "Senda", "")
    calls = 0

    monkeypatch.setattr(service, "_data_revision", lambda _context: ("revision",))

    def build(**_kwargs: object) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {"generation": calls}

    monkeypatch.setattr(service, "_build_nps_dashboard", build)
    first = service.nps_dashboard(context=context)
    second = service.nps_dashboard(context=context)
    service.clear_caches()
    third = service.nps_dashboard(context=context)

    assert first is second
    assert third == {"generation": 2}


def test_causal_window_uses_nps_dates_and_handles_missing_dates() -> None:
    nps = pd.DataFrame({"Fecha": pd.to_datetime(["2026-03-10", "2026-03-20"])})
    helix = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2025-11-01", "2026-02-01", "2026-04-15"]),
            "Incident Number": ["old", "before", "after"],
        }
    )

    selected = DashboardService._causal_helix_window(helix, nps, max_days_apart=45)

    assert selected["Incident Number"].tolist() == ["before", "after"]
    assert DashboardService._causal_helix_window(
        helix.drop(columns="Fecha"), nps, max_days_apart=45
    ).empty
    assert DashboardService._causal_helix_window(
        helix, pd.DataFrame({"Fecha": [None]}), max_days_apart=45
    ).empty


def test_empty_causal_bundle_is_cached_with_explicit_scope(tmp_path: Path) -> None:
    service = _service(tmp_path)
    context = UploadContext("BBVA México", "Senda", "")

    first = service._causal_analysis_bundle(
        context=context,
        pop_year="2026",
        pop_month="03",
        score_channel="Todos",
        min_similarity=0.15,
        max_days_apart=90,
        touchpoint_source="domain_touchpoint",
    )
    second = service._causal_analysis_bundle(
        context=context,
        pop_year="2026",
        pop_month="03",
        score_channel="Todos",
        min_similarity=0.15,
        max_days_apart=90,
        touchpoint_source="domain_touchpoint",
    )

    assert first is second
    assert first["ready"] is False
    assert first["resolved_channel"] == "Todos"
    assert first["helix_window_rows"] == 0


def test_empty_linking_dashboard_exposes_scope_funnel(tmp_path: Path, monkeypatch) -> None:
    service = _service(tmp_path)
    context = UploadContext("BBVA México", "Senda", "")
    service.settings.service_origin_n2_map[context.service_origin] = {"App": ["Missing service"]}
    nps = pd.DataFrame(
        {
            "ID": ["a", "b"],
            "Fecha": pd.to_datetime(["2026-03-01"] * 2),
            "NPS": [2, 3],
            "Canal": ["App", "App"],
            "Comment": ["error login", ""],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["i1"],
            "BBVA_SourceServiceN1": ["Other"],
            "Fecha": pd.to_datetime(["2026-03-01"]),
            "summary": ["error login"],
        }
    )
    monkeypatch.setattr(service, "_load_nps_df", lambda _: nps)
    monkeypatch.setattr(
        service,
        "_load_helix_df",
        lambda _, score_channel=None: helix if not score_channel else helix.iloc[:0],
    )
    payload = service.linking_dashboard(context=context, score_channel="App")
    diagnostics = payload["diagnostics"]
    assert not payload["available"]
    assert diagnostics["nps_total"] == 2
    assert diagnostics["nps_matchable"] == 1
    assert diagnostics["helix_total"] == 1
    assert diagnostics["helix_after_scope"] == 0
    assert diagnostics["scope_requested_n1_n2"] == ["Missing service"]
    assert diagnostics["scope_available_n1"] == ["Other"]
    assert diagnostics["scope_found_n1"] == []
