from __future__ import annotations

from pathlib import Path

import pandas as pd

from nps_lens.domain.models import UploadContext
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.nps_service import NpsService
from nps_lens.settings import Settings
from nps_lens.testing.fixtures import fixture_excel


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        database_path=tmp_path / "data" / "nps.sqlite3",
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


def test_sequential_uploads_are_accumulative_and_duplicate_safe(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    service = NpsService(SqliteNpsRepository(settings.database_path), settings)
    context = UploadContext(service_origin="BBVA México", service_origin_n1="Senda")

    jan_feb = fixture_excel("NPS Térmico Senda - 01Enero-02Febrero.xlsx")
    march = fixture_excel("NPS Térmico Senda - 03Marzo.xlsx")

    first = service.ingest_excel(
        filename=jan_feb.name,
        payload=jan_feb.read_bytes(),
        context=context,
    )
    second = service.ingest_excel(
        filename=march.name,
        payload=march.read_bytes(),
        context=context,
    )
    duplicate_attempt = service.ingest_excel(
        filename=march.name,
        payload=march.read_bytes(),
        context=context,
    )

    summary = service.summary(context)

    assert first["status"] == "completed"
    assert second["status"] == "completed"
    assert duplicate_attempt["status"] == "duplicate_upload"
    assert (
        summary["total_records"]
        == first["inserted_rows"] + second["inserted_rows"] + second["updated_rows"]
    )
    assert summary["uploads"] == 3
    assert summary["duplicates_prevented"] >= duplicate_attempt["duplicate_historical_rows"]
    assert service.list_uploads()[0]["status"] == "duplicate_upload"


def test_historical_merge_keeps_single_record_per_business_key(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    repository = SqliteNpsRepository(settings.database_path)
    service = NpsService(repository, settings)
    context = UploadContext(service_origin="BBVA México", service_origin_n1="Senda")
    march = fixture_excel("NPS Térmico Senda - 03Marzo.xlsx")

    service.ingest_excel(filename=march.name, payload=march.read_bytes(), context=context)
    frame = repository.load_records_df(context)

    assert len(frame) == len(frame["_business_key"].drop_duplicates())


def test_large_upload_is_persisted_without_hitting_sqlite_variable_limits(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    service = NpsService(SqliteNpsRepository(settings.database_path), settings)
    context = UploadContext(service_origin="BBVA México", service_origin_n1="Senda")

    row_count = 1105
    large_fixture = tmp_path / "large-nps.xlsx"
    pd.DataFrame(
        {
            "Fecha": pd.date_range("2026-01-01", periods=row_count, freq="D"),
            "ID": [f"id-{idx}" for idx in range(row_count)],
            "NPS Group": ["DETRACTOR"] * row_count,
            "NPS": [idx % 11 for idx in range(row_count)],
            "Comment": [f"comentario {idx}" for idx in range(row_count)],
            "UsuarioDecisión": ["Usuario"] * row_count,
            "Canal": ["Web"] * row_count,
            "Palanca": ["Pagos"] * row_count,
            "Subpalanca": ["Transferencias"] * row_count,
        }
    ).to_excel(large_fixture, index=False)

    result = service.ingest_excel(
        filename=large_fixture.name,
        payload=large_fixture.read_bytes(),
        context=context,
    )

    assert result["status"] == "completed"
    assert result["inserted_rows"] == row_count
    assert service.summary(context)["total_records"] == row_count


def test_persistence_failure_is_reported_without_leaving_processing_uploads(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path)
    repository = SqliteNpsRepository(settings.database_path)
    service = NpsService(repository, settings)
    context = UploadContext(service_origin="BBVA México", service_origin_n1="Senda")
    march = fixture_excel("NPS Térmico Senda - 03Marzo.xlsx")

    def _boom(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(repository, "upsert_records", _boom)

    result = service.ingest_excel(filename=march.name, payload=march.read_bytes(), context=context)
    uploads = service.list_uploads()

    assert result["status"] == "failed"
    assert any(issue["code"] == "storage_error" for issue in result["issues"])
    assert uploads[0]["status"] == "failed"
