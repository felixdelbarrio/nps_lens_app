from datetime import datetime, time

import numpy as np
import pandas as pd
import pytest

from nps_lens.analytics.causal import best_effort_ate_logit
from nps_lens.analytics.linking_diagnostics import linking_diagnostics
from nps_lens.analytics.nps_helix_link import (
    build_incident_text,
    build_incident_topic,
    link_incidents_to_nps_topics,
)
from nps_lens.domain.column_aliases import ColumnAliasRegistry
from nps_lens.domain.models import UploadContext
from nps_lens.domain.record_identity import resolve_response_identity
from nps_lens.ingest.helix_dates import incident_occurrence_dates
from nps_lens.ingest.helix_incidents import read_helix_incidents_excel
from nps_lens.ingest.nps_thermal import read_nps_thermal_excel
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.nps_service import NpsService
from nps_lens.settings import DEFAULT_UI_TOUCHPOINT_SOURCE, Settings


def test_argentina_alias_identity_and_population(tmp_path):
    path = tmp_path / "opinions.xlsx"
    pd.DataFrame(
        {
            "Date": ["2026-09-01"] * 3,
            "OPI-Sense": [2, 3, 8],
            "Channel": ["App"] * 3,
            "Id": ["shared"] * 3,
            "Opinion Identifier": ["a", "b", "c"],
            "Opinion Unique Code": ["x", "y", "z"],
            "Text": [None] * 3,
            "Verbatim valora la app SENDA": ["error transferencias", "no puedo entrar", None],
        }
    ).to_excel(path, index=False)
    result = read_nps_thermal_excel(str(path), service_origin="Company")
    assert not [issue for issue in result.issues if issue.level == "ERROR"]
    assert result.df.ID.tolist() == ["a", "b", "c"]
    assert result.df._business_key.nunique() == 3
    assert result.df.Comment.tolist() == ["error transferencias", "no puedo entrar", ""]
    assert result.df.match_status.tolist() == ["matchable", "matchable", "non_matchable"]


def test_comment_content_ambiguity_includes_canonical():
    frame = pd.DataFrame({"Comment": ["one"], "Text": ["two"]})
    assert ColumnAliasRegistry.default().resolve(frame.columns, frame=frame).ambiguities
    frame["Comment"] = ""
    resolution = ColumnAliasRegistry.default().resolve(frame.columns, frame=frame)
    assert not resolution.ambiguities
    assert resolution.rename["Text"] == "Comment"


def test_identity_quality_and_shared_id_fallback():
    frame = pd.DataFrame(
        {
            "ID": ["same", "same"],
            "Opinion Identifier": ["a", None],
            "Opinion Unique Code": ["u", "v"],
            "Comment": ["one", "two"],
            "NPS": [2, 2],
            "Fecha": ["2026-09-01"] * 2,
        }
    )
    ids, source = resolve_response_identity(frame)
    assert source == "Opinion Unique Code"
    assert ids.tolist() == ["u", "v"]
    ids, _ = resolve_response_identity(
        frame.drop(columns=["Opinion Identifier", "Opinion Unique Code"])
    )
    assert ids.nunique() == 2
    assert resolve_response_identity(pd.DataFrame({"ID": [0, 1]}))[0].tolist() == ["0", "1"]


@pytest.mark.parametrize("sentinel", [0, 1, np.nan, "nan", "none", "N/A", "1.0"])
def test_category_sentinels_use_source_service(sentinel):
    frame = pd.DataFrame(
        {
            "Product Categorization Tier 1": [sentinel],
            "BBVA_SourceServiceN2": ["Transferencias"],
            "service": ["App"],
        }
    )
    assert build_incident_topic(frame).iloc[0] == "Transferencias"


def test_semantic_text_is_unique_and_ignores_template_sections():
    frame = pd.DataFrame(
        {
            "summary": ["Error transferencias"],
            "Description": ["Error transferencias"],
            "Detailed Decription": [
                "Contacto: persona privada\nSíntoma: Error transferencias\nAdjuntos: plantilla"
            ],
        }
    )
    assert build_incident_text(frame).iloc[0] == "Error transferencias"


def test_occurrence_priority_partial_fallback_and_evidence_window():
    frame = pd.DataFrame(
        {
            "bbva_startdatetime": ["2026-09-01", None],
            "Submit Date": ["2026-09-23", "2026-09-04"],
            "Incident Number": ["i1", "i2"],
            "summary": ["error transferencias"] * 2,
        }
    )
    dates, sources = incident_occurrence_dates(frame)
    assert dates.tolist() == [pd.Timestamp("2026-09-01"), pd.Timestamp("2026-09-04")]
    assert sources.tolist() == ["bbva_startdatetime", "Submit Date"]
    nps = pd.DataFrame(
        {
            "ID": ["a", "b"],
            "Fecha": ["2026-09-01"] * 2,
            "Comment": ["error transferencias", "nan"],
            "Palanca": ["", "n/a"],
        }
    )
    _, links = link_incidents_to_nps_topics(nps, frame, max_days_apart=0)
    assert set(links.incident_id) == {"i1"}
    assert set(links.nps_id) == {"a"}


def test_multisheet_reads_all_valid_sheets_and_preserves_source(tmp_path):
    path = tmp_path / "helix.xlsx"
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame({"nota": ["summary"]}).to_excel(writer, sheet_name="Cover", index=False)
        for name in ["Helix Raw - SENDA", "Other valid"]:
            pd.DataFrame(
                {
                    "Owner Support Company": ["Company"],
                    "Incident Number": [name],
                    "Submit Date": ["2026-09-01"],
                }
            ).to_excel(writer, sheet_name=name, index=False)
    result = read_helix_incidents_excel(str(path), "Company", "", "")
    assert len(result.df) == 2
    assert result.df._source_sheet.tolist() == ["Helix Raw - SENDA", "Other valid"]
    assert (
        len(read_helix_incidents_excel(str(path), "Company", "", "", sheet_name="Other valid").df)
        == 1
    )


def test_funnel_accounts_for_each_population():
    nps = pd.DataFrame({"Comment": ["error", "", "transferencias", "login"]})
    helix = pd.DataFrame(
        {
            "BBVA_SourceServiceN1": ["Senda"] * 5,
            "BBVA_SourceServiceN2": ["App", "Web", "Web", "Web", "Other"],
        }
    )
    links = pd.DataFrame({"incident_id": ["i1", "i1"], "nps_id": ["a", "b"]})
    result = linking_diagnostics(
        nps=nps,
        focus=nps.iloc[:3],
        helix=helix,
        scoped=helix.iloc[:4],
        period=helix.iloc[:3],
        eligible=helix.iloc[:2],
        links=links,
        requested_scope=["Senda"],
    )
    assert [
        result[key]
        for key in [
            "nps_total",
            "nps_focus",
            "nps_matchable",
            "helix_total",
            "helix_after_scope",
            "helix_after_period",
            "helix_quality_eligible",
            "linked_incidents",
            "linked_nps_comments",
            "evidence_pairs",
        ]
    ] == [4, 3, 2, 5, 4, 3, 2, 1, 2, 2]
    assert result["exclusions"] == dict.fromkeys(
        [
            "nps_outside_focus",
            "non_matchable",
            "helix_outside_scope",
            "helix_outside_period_or_missing_date",
            "helix_quality",
            "helix_without_evidence",
        ],
        1,
    )
    assert result["scope_found_n2"] == ["App", "Web"]
    assert result["matchable_coverage_pct"] == 100


def test_logit_runs_without_warning_shadowing():
    rng = np.random.default_rng(42)
    treatment = rng.integers(0, 2, 1000)
    outcome = rng.binomial(1, 0.25 + treatment * 0.35)
    result = best_effort_ate_logit(
        pd.DataFrame({"treatment": treatment, "is_detractor": outcome}), "treatment", "1"
    )
    assert result.method == "logit+marginal_effect"
    assert result.effect > 0
    assert result.p_value < 0.01
    assert DEFAULT_UI_TOUCHPOINT_SOURCE == "broken_journeys"


def test_mexico_excel_temporal_extra_fields_persist(tmp_path):
    path = tmp_path / "mexico.xlsx"
    pd.DataFrame(
        {
            "Fecha": ["2026-09-01"],
            "NPS": [5],
            "Canal": ["App"],
            "ID": ["response"],
            "answer_date": [datetime(2026, 9, 1)],
            "Hora": [time(12, 30)],
        }
    ).to_excel(path, index=False)
    settings = Settings(
        data_dir=tmp_path,
        database_path=tmp_path / "nps.sqlite3",
        frontend_dist_dir=tmp_path,
        frontend_public_dir=tmp_path,
        api_host="127.0.0.1",
        api_port=8000,
        default_service_origin="BBVA México",
        default_service_origin_n1="",
        allowed_service_origins=["BBVA México"],
        allowed_service_origin_n1={},
        log_level="INFO",
        column_aliases_path=tmp_path / "aliases.json",
    )
    repository = SqliteNpsRepository(settings.database_path)
    service = NpsService(repository, settings)
    result = service.ingest_excel(
        filename=path.name,
        payload=path.read_bytes(),
        context=UploadContext(service_origin="BBVA México"),
    )
    assert result["status"] == "completed"
    with repository._connect() as connection:
        import json

        extra = json.loads(
            connection.execute("SELECT extra_payload_json FROM records").fetchone()[0]
        )
    assert extra["answer_date"] == "2026-09-01T00:00:00"
    assert extra["Hora"] == "12:30:00"
