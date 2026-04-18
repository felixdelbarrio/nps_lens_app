from __future__ import annotations

from pathlib import Path

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
