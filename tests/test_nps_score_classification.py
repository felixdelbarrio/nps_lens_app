from io import BytesIO
from pathlib import Path

import pandas as pd
import pytest

from nps_lens.core.nps_math import classify_nps_scores, grouped_focus_rates
from nps_lens.domain.models import UploadContext
from nps_lens.domain.normalization import EquivalenceRegistry
from nps_lens.ingest.nps_thermal import read_nps_thermal_excel
from nps_lens.platform.publication import build_static_data_snapshot
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.dashboard_service import DashboardService
from nps_lens.services.nps_service import NpsService
from nps_lens.settings import Settings


def source_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ID": [str(i) for i in range(11)],
            "Fecha": ["2026-09-01"] * 11,
            "NPS": list(range(11)),
            "NPS Group": ["PROMOTOR"] * 11,
            "Canal": ["Web"] * 11,
            "Palanca": ["Acceso"] * 11,
            "Subpalanca": ["Login"] * 11,
        }
    )


@pytest.mark.parametrize("header", [None, "NPS Group", "user_type", "Grupo NPS"])
def test_ingestion_ignores_manual_groups_and_uses_every_score_boundary(
    tmp_path: Path, header
) -> None:
    frame = source_frame()
    frame = (
        frame.drop(columns="NPS Group")
        if header is None
        else frame.rename(columns={"NPS Group": header})
    )
    path = tmp_path / "scores.xlsx"
    frame.to_excel(path, index=False)
    result = read_nps_thermal_excel(str(path), service_origin="Bank", service_origin_n1="Web")
    assert result.df["NPS Group"].tolist() == ["DETRACTOR"] * 7 + ["PASIVO"] * 2 + ["PROMOTOR"] * 2
    assert "NPS Group" not in result.meta["missing_optional_columns"]
    rates = grouped_focus_rates(result.df).iloc[0]
    assert rates["detractor_rate"] == pytest.approx(7 / 11)
    assert rates["passive_rate"] == pytest.approx(2 / 11)
    assert rates["promoter_rate"] == pytest.approx(2 / 11)


def test_invalid_scores_cannot_be_rescued_by_manual_group(tmp_path: Path) -> None:
    scores = [None, "invalid", -1, 11, 6.5, float("inf"), 8]
    assert classify_nps_scores(pd.Series(scores)).tolist() == [""] * 6 + ["PASIVO"]
    frame = source_frame().iloc[: len(scores)].copy()
    frame["NPS"] = scores
    path = tmp_path / "invalid.xlsx"
    frame.to_excel(path, index=False)
    result = read_nps_thermal_excel(str(path), service_origin="Bank", service_origin_n1="Web")
    assert result.df["NPS"].tolist() == [8]
    assert (
        next(issue for issue in result.issues if issue.code == "invalid_nps_dropped").details[
            "rows"
        ]
        == 6
    )


def test_historical_manual_groups_do_not_reach_snapshot(tmp_path: Path) -> None:
    settings = Settings(
        data_dir=tmp_path,
        database_path=tmp_path / "nps.sqlite3",
        equivalences_path=tmp_path / "equivalences.json",
        frontend_dist_dir=tmp_path / "frontend-dist",
        frontend_public_dir=tmp_path / "frontend-public",
        api_host="127.0.0.1",
        api_port=8000,
        default_service_origin="Bank",
        default_service_origin_n1="Web",
        allowed_service_origins=["Bank"],
        allowed_service_origin_n1={"Bank": ["Web"]},
        log_level="INFO",
    )
    repository = SqliteNpsRepository(settings.database_path)
    service = NpsService(repository, settings)
    context = UploadContext(service_origin="Bank", service_origin_n1="Web")
    payload = BytesIO()
    source_frame().to_excel(payload, index=False)
    result = service.ingest_excel(
        filename="scores.xlsx", payload=payload.getvalue(), context=context
    )
    assert result["status"] == "completed"
    with repository._connect() as connection:
        stored = connection.execute("SELECT nps_group FROM records ORDER BY nps_score").fetchall()
        assert [row[0] for row in stored] == ["DETRACTOR"] * 7 + ["PASIVO"] * 2 + ["PROMOTOR"] * 2
        connection.execute("UPDATE records SET nps_group = 'MANUAL ERROR'")
    registry = EquivalenceRegistry.from_dict(
        {"dimensions": {"NPS Group": [{"canonical": "MANUAL ERROR", "aliases": ["PASIVO"]}]}}
    )
    assert "NPS Group" not in registry.to_dict()["dimensions"]
    repository.canonicalize_records(registry)
    dashboard = DashboardService(repository=repository, settings=settings)
    nps = dashboard.dataset_rows(
        dataset_kind="nps", context=context, nps_group="Todos", score_channel="Todos"
    )
    snapshot = build_static_data_snapshot(nps, {"columns": [], "rows": []})
    pages = snapshot["datasets"]["nps"]["pages"]
    assert pages["todos|detractor"]["total_rows"] == 7
    assert pages["todos|pasivo"]["total_rows"] == 2
    assert pages["todos|promotor"]["total_rows"] == 2
    assert {row["NPS Group"] for row in pages["todos|todos"]["rows"]} == {
        "DETRACTOR",
        "PASIVO",
        "PROMOTOR",
    }
