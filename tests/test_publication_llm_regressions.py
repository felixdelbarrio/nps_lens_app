from __future__ import annotations

import json
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import pytest
from test_iteration28_taxonomy_helix import discover
from test_taxonomy_exchange import exchange_fixture as exchange_fixture

from nps_lens.reports import BusinessPptResult


def classified_dashboard(exchange):
    handler, context, frame, client = exchange
    frame.drop(index=frame.index[6:], inplace=True)
    frame["Fecha"] = pd.to_datetime(["2026-07-01"] * 3 + ["2026-08-01"] * 3)
    frame["NPS"] = [0, 10, 10, 0, 0, 10]
    frame["Canal"] = ["Web", "App", "Web", "Web", "App", "App"]
    discover(handler, context)
    state = handler.taxonomy.state(context)
    state["comment_engine"] = "llm"
    handler.taxonomy.save_state(context, state)
    return client.app.state.dashboard_service, context


def test_llm_gaps_keep_other_groups_and_channels_in_metric_population(exchange):
    service, context = classified_dashboard(exchange)
    result = service.nps_dashboard(
        context=context,
        pop_year="2026",
        pop_month="08",
        nps_group="Detractores",
        score_channel="Web",
        min_n=1,
    )
    gaps = result["gaps"]
    assert gaps["base_nps"] == pytest.approx(100 / 3)
    assert len(gaps["table"]) == 1
    row = gaps["table"][0]
    assert row["n"] == 3
    assert row["nps"] == pytest.approx(-100 / 3)
    assert row["gap_vs_base"] == pytest.approx(-200 / 3)
    history, _ = service._comment_analysis_frame(context, pop_year="2026", pop_month="08")
    static = service._build_comments_snapshot(history_df=history, pop_year="2026", pop_month="08")
    assert static["gaps"]["Web"]["Palanca"]["table"] == gaps["table"]


def test_publication_all_periods_resolves_latest_month_and_preserves_llm(
    exchange, monkeypatch, tmp_path
):
    service, context = classified_dashboard(exchange)
    monkeypatch.setattr(service, "linking_dashboard", lambda **kwargs: {})
    report = BusinessPptResult(
        "report.pptx", b"ppt", 1, compact_file_name="compact.pptx", compact_content=b"compact"
    )
    monkeypatch.setattr(service, "generate_ppt_report", lambda **kwargs: report)
    monkeypatch.setattr(service, "_persist_artifact", lambda content, name: tmp_path / name)
    artifacts = [service.generate_publication(context=context) for _ in range(2)]
    publications = []
    for artifact in artifacts:
        with ZipFile(BytesIO(artifact.content)) as archive:
            publications.append(json.loads(archive.read("publication.json")))
    first, second = publications
    assert first["scope"]["month"] == "08"
    assert first["scope"]["year"] == "2026"
    assert first["scope"]["key"] != second["scope"]["key"]
    assert first["scope"]["audience_key"] == second["scope"]["audience_key"]
    gaps = first["screens"]["comments"]["gaps"]["Web"]["Palanca"]
    assert gaps["base_nps"] == pytest.approx(100 / 3)
    assert gaps["table"][0]["value"] == "Atención"
    assert gaps["table"][0]["n"] == 3


def test_static_defaults_reference_existing_channels_and_groups(exchange):
    service, context = classified_dashboard(exchange)
    history, _ = service._comment_analysis_frame(context)
    history["Canal"] = history["Canal"].str.upper()
    history["NPS"] = 10
    controls = service._build_comments_snapshot(
        history_df=history, pop_year="2026", pop_month="08"
    )["controls"]
    assert controls["defaults"]["channel"] == "WEB"
    assert controls["defaults"]["group"] in controls["groups"]


def test_gaps_all_periods_compare_latest_month_to_prior_history(exchange):
    service, context = classified_dashboard(exchange)
    all_periods = service.nps_dashboard(context=context, nps_group="Detractores")["gaps"]
    monthly = service.nps_dashboard(
        context=context, pop_year="2026", pop_month="08", nps_group="Todos"
    )["gaps"]
    assert all_periods == monthly
    assert all_periods["table"][0]["n"] == 3
