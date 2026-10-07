"""Analytical horizon, cumulative assignments and frozen jobs across months."""

import copy
from datetime import date

import pandas as pd
import pytest
from test_iteration28_taxonomy_helix import helix_fixture as helix_fixture
from test_iteration28_taxonomy_helix import helix_response
from test_taxonomy_exchange import classifier_files, classifier_zip, designer_zip, exported, zipped
from test_taxonomy_exchange import exchange_fixture as exchange_fixture

from nps_lens.services.analysis_horizon import analysis_horizon
from nps_lens.services.taxonomy_exchange import TaxonomyExchange
from nps_lens.ui.business import default_windows, driver_delta_table, slice_by_window

SEPTEMBER = dict(pop_year="2026", pop_month="09", max_days_apart=90)
OCTOBER = {**SEPTEMBER, "pop_month": "10"}


def small_comments(frame):
    frame.drop(frame.index[4:], inplace=True)
    frame["Fecha"] = pd.to_datetime(["2026-06-12", "2026-08-10", "2026-09-20", "2026-10-05"])
    frame["Palanca"], frame["Subpalanca"] = "Atención", "Resolución"
    frame["NPS"] = [2, 10, 2, 10]


def import_comments(handler, ctx, request):
    return handler.import_response(
        ctx, classifier_zip(classifier_files(request["manifest.json"], request)), "classifier"
    )


def test_horizon_uses_exact_existing_comparison_and_no_comment_padding():
    frame = pd.DataFrame(
        {"Fecha": pd.to_datetime(["2026-06-12", "2026-08-10", "2026-09-20", "2026-10-05"])}
    )
    current, baseline = default_windows(frame, pop_year="2026", pop_month="09")
    horizon = analysis_horizon(frame, **SEPTEMBER)
    assert horizon.comment_start == baseline.start == date(2026, 6, 12)
    assert horizon.comment_end == current.end == date(2026, 9, 20)
    assert horizon.helix_start == date(2026, 6, 3)
    assert horizon.payload()["helix_end"] == "2026-09-20"
    assert horizon.mask(frame.Fecha).tolist() == [True, True, True, False]
    assert analysis_horizon(frame, pop_year="Todos", pop_month="Todos").comment_end == date(
        2026, 10, 5
    )


@pytest.mark.parametrize("mode", ["SOURCE", "DISCOVERED"])
def test_comments_incremental_preserve_outside_horizon_and_late_job(exchange, mode):
    handler, ctx, frame, client = exchange
    small_comments(frame)
    if mode == "DISCOVERED":
        handler.import_response(ctx, designer_zip(handler, ctx), "designer")
        handler.taxonomy.configure(ctx, {"accept_proposal": True})
        handler.taxonomy.configure(ctx, {"active": mode})
    request = exported(handler.export(ctx, "classifier", **SEPTEMBER)["saved_paths"])
    sent = [
        row["Comment"]
        for name, batch in request.items()
        if name.startswith("comments/")
        for row in batch["comments"]
    ]
    assert sent == frame.Comment.iloc[:3].tolist()
    assert request["manifest.json"]["scope"] == SEPTEMBER
    assert handler.progress(ctx, **SEPTEMBER)["total"] == 3
    assert handler.progress(ctx, **SEPTEMBER)["designer"]["total"] == 4
    october = exported(handler.export(ctx, "classifier", **OCTOBER)["saved_paths"])
    import_comments(handler, ctx, october)
    # Old September response merges with October's accumulation, rather than its own old artifact.
    import_comments(handler, ctx, request)
    assert len(handler.assignments(ctx, frame, mode)) == 4
    assert handler.export(ctx, "classifier", **SEPTEMBER)["stage"] == "complete"
    assert handler.progress(ctx, **OCTOBER)["pending"] == 0
    progress = client.get(
        "/api/taxonomy/discovery/progress",
        params={"service_origin": "Bank", "service_origin_n1": "Web", **SEPTEMBER},
    )
    assert progress.status_code == 200
    assert progress.json()["total"] == 3
    assert progress.json()["analysis_horizon"]["comment_start"] == "2026-06-12"
    # Current deltas see exactly the same classified rows as the equivalent full corpus.
    classified = handler.apply(ctx, frame, mode)
    current, baseline = default_windows(frame, pop_year="2026", pop_month="09")
    incremental = driver_delta_table(
        slice_by_window(classified, current),
        slice_by_window(classified, baseline),
        "Palanca",
        min_n=1,
    )
    full = driver_delta_table(
        slice_by_window(frame, current), slice_by_window(frame, baseline), "Palanca", min_n=1
    )
    pd.testing.assert_frame_equal(incremental, full)


def test_helix_horizon_reuses_categories_and_imports_after_month_change(helix, monkeypatch):
    handler, ctx, frame, incidents, client = helix
    frame.loc[frame.index[-1], "Fecha"] = pd.Timestamp("2026-10-01")
    incidents = incidents.iloc[:5].copy()
    incidents["Submit Date"] = pd.to_datetime(
        ["2026-06-02", "2026-06-03", "2026-09-01", "2026-09-02", "2026-10-01"]
    )
    inputs = handler.inputs(ctx, incidents, "SOURCE", **SEPTEMBER)
    assert {row["id"] for row in inputs["incidents"]} == {"INC-1", "INC-2"}
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    sent = {
        row["id"]
        for name, batch in request.items()
        if name.startswith("incidents/")
        for row in batch["incidents"]
    }
    assert sent == {"INC-1", "INC-2"}
    changed = handler.inputs(ctx, incidents, "SOURCE", **OCTOBER)
    response = zipped(helix_response(request, "no-link"))
    handler.import_response(ctx, changed, response)
    monkeypatch.setattr(
        client.app.state.dashboard_service, "_load_helix_df", lambda *args, **kwargs: incidents
    )
    imported = client.post(
        "/api/taxonomy/helix/import",
        params={"service_origin": "Bank", "service_origin_n1": "Web", **OCTOBER},
        files={"file": ("september-response.zip", response, "application/zip")},
    )
    assert imported.status_code == 200, imported.text
    assert imported.json()["analysis_horizon"] == inputs["analysis_horizon"]
    assert handler.status(ctx, inputs)["pending"] == 0
    assert handler.export(ctx, inputs)["saved_paths"] == []
    assert handler.status(ctx, changed)["received"] == 1
    assert handler.status(ctx, changed)["pending"] == 2
    with handler.repository._connect() as db:
        assert {
            row[0] for row in db.execute("SELECT incident FROM helix_incident_categories")
        } == sent
    # Changing the date doesn't require a new category; a new link scope still validates pairs.
    incidents.loc[incidents.index[1], "Submit Date"] = pd.Timestamp("2026-06-04")
    moved = handler.inputs(ctx, incidents, "SOURCE", **SEPTEMBER)
    assert handler.status(ctx, moved)["pending"] == 0
    assert handler.export(ctx, moved)["pending"] == 1


def test_restarted_comment_accumulation_retains_completed_period(exchange):
    handler, ctx, frame, _ = exchange
    small_comments(frame)
    request = exported(handler.export(ctx, "classifier", **OCTOBER)["saved_paths"])
    import_comments(handler, ctx, request)
    restarted = TaxonomyExchange(handler.taxonomy, handler.downloads)
    assert restarted.progress(ctx, **SEPTEMBER)["pending"] == 0
    assert len(restarted.assignments(ctx, frame, "SOURCE")) == 4


def test_month_change_exports_only_new_comments_and_preserves_future_assignments(exchange):
    handler, ctx, frame, _ = exchange
    small_comments(frame)
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"accept_proposal": True})
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    september = exported(handler.export(ctx, "classifier", **SEPTEMBER)["saved_paths"])
    import_comments(handler, ctx, september)
    assert handler.progress(ctx, **SEPTEMBER)["pending"] == 0
    october = exported(handler.export(ctx, "classifier", **OCTOBER)["saved_paths"])
    sent = [
        row["Comment"]
        for name, batch in october.items()
        if name.startswith("comments/")
        for row in batch["comments"]
    ]
    assert sent == [frame.Comment.iloc[3]]
    import_comments(handler, ctx, october)
    assert handler.progress(ctx, **SEPTEMBER)["pending"] == 0
    assert len(handler.assignments(ctx, frame, "DISCOVERED")) == 4
    # Empty comments are assigned locally without replacing October's accumulated assignment.
    frame.loc[frame.index[0], "Comment"] = ""
    handler.export(ctx, "classifier", **SEPTEMBER)
    assert len(handler.assignments(ctx, frame, "DISCOVERED")) == 4


def test_late_jobs_keep_separate_taxonomy_fingerprints(exchange):
    handler, ctx, frame, _ = exchange
    small_comments(frame)
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"accept_proposal": True})
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    old_catalog = handler.taxonomy.state(ctx)["discovered_taxonomy"]
    old_job = exported(handler.export(ctx, "classifier", **SEPTEMBER)["saved_paths"])
    new_catalog = copy.deepcopy(old_catalog)
    new_catalog["taxonomy"][0]["sublevers"][0]["criterion"] += " Criterio revisado."
    state = handler.taxonomy.state(ctx)
    state["discovered_taxonomy"] = new_catalog
    handler.taxonomy.save_state(ctx, state)
    new_job = exported(handler.export(ctx, "classifier", **OCTOBER)["saved_paths"])
    import_comments(handler, ctx, new_job)
    current_signature = handler.taxonomy.state(ctx)["artifacts"]["DISCOVERED"]
    import_comments(handler, ctx, old_job)
    assert handler.taxonomy.state(ctx)["artifacts"]["DISCOVERED"] == current_signature
    assert handler.progress(ctx, **OCTOBER)["pending"] == 0
    state = handler.taxonomy.state(ctx)
    state["discovered_taxonomy"] = old_catalog
    handler.taxonomy.save_state(ctx, state)
    assert handler.progress(ctx, **SEPTEMBER)["pending"] == 0
    assert handler.progress(ctx, **OCTOBER)["pending"] == 1


@pytest.mark.parametrize("partial", [False, True])
def test_compare_requires_classified_intersection(exchange, partial, monkeypatch):
    handler, ctx, frame, _ = exchange
    small_comments(frame)
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"accept_proposal": True})
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    if partial:
        monkeypatch.setattr("nps_lens.services.taxonomy_exchange.CLASSIFICATION_BATCH_ROWS", 1)
        request = exported(handler.export(ctx, "classifier")["saved_paths"])
        response = classifier_files(request["manifest.json"], request)
        response["results"] = dict(list(response["results"].items())[:2])
        response["manifest"]["batches"] = [
            batch for batch in response["manifest"]["batches"] if batch["id"] in response["results"]
        ]
        handler.import_response(ctx, classifier_zip(response), "classifier")
        # The other lens is also partial, including a semantic placeholder.
        frame.loc[0, "Subpalanca"] = "Sin clasificar"
        frame.loc[3, "Palanca"] = ""
    result = handler.taxonomy.compare(ctx, "SOURCE", "DISCOVERED")
    assert result["total"] == 4
    assert result["comparable"] == int(partial)
    assert result["left_classified"] == (2 if partial else 4)
    assert result["right_classified"] == (2 if partial else 0)
    if partial:
        assert sum(row["volume"] for row in result["rows"]) == 1
        assert result["rows"][0]["share"] == 1
        assert all(
            value not in ("", "Sin clasificar") for row in result["rows"] for value in row.values()
        )
    else:
        assert result["rows"] == []
        assert (
            result["note"]
            == "No hay respuestas comparables. Clasifica primero los comentarios con ambas taxonomías."
        )


def test_comment_readiness_progress_export_require_baseline(exchange, monkeypatch):
    handler, ctx, frame, client = exchange
    small_comments(frame)
    handler.taxonomy.configure(ctx, {"comment_engine": "llm"})
    monkeypatch.setattr("nps_lens.services.taxonomy_exchange.CLASSIFICATION_BATCH_ROWS", 1)
    request = exported(handler.export(ctx, "classifier", **SEPTEMBER)["saved_paths"])
    response = classifier_files(request["manifest.json"], request)
    response["results"] = {
        name: batch
        for name, batch in response["results"].items()
        if request[f"comments/{name}.json"]["comments"][0]["Comment"] == frame.Comment.iloc[2]
    }
    response["manifest"]["batches"] = [
        batch for batch in response["manifest"]["batches"] if batch["id"] in response["results"]
    ]
    handler.import_response(ctx, classifier_zip(response), "classifier")
    dashboard = client.app.state.dashboard_service
    scope = {**SEPTEMBER, "score_channel": "Web", "nps_group": "Detractores"}
    status = dashboard.classification_status("comments", ctx, **scope)
    progress = handler.progress(ctx, **scope)
    assert (
        {key: status[key] for key in ("total", "received", "pending")}
        == {key: progress[key] for key in ("total", "received", "pending")}
        == {"total": 3, "received": 1, "pending": 2}
    )
    assert status["ready"] is False
    pending = exported(handler.export(ctx, "classifier", **scope)["saved_paths"])
    sent = [
        row["Comment"]
        for name, batch in pending.items()
        if name.startswith("comments/")
        for row in batch["comments"]
    ]
    assert sent == frame.Comment.iloc[:2].tolist()
    import_comments(handler, ctx, pending)
    assert dashboard.classification_status("comments", ctx, **scope)["ready"] is True
    assert handler.export(ctx, "classifier", **scope)["stage"] == "complete"


def test_august_helix_window_does_not_inherit_baseline():
    frame = pd.DataFrame(
        {"Fecha": pd.to_datetime(["2020-01-01", "2026-08-10", "2026-08-31", "2026-09-10"])}
    )
    scope = dict(pop_year="2026", pop_month="08", max_days_apart=90)
    horizon = analysis_horizon(frame, **scope)
    assert horizon.comment_start == date(2020, 1, 1)
    assert horizon.link_comment_start == date(2026, 8, 1)
    assert horizon.helix_start == date(2026, 5, 3)
    assert horizon.link_comment_end == date(2026, 8, 31)
    assert analysis_horizon(frame.iloc[1:], **scope).helix_start == horizon.helix_start
    incidents = pd.Series(pd.to_datetime(["2026-05-02", "2026-05-03", "2026-08-31", "2026-09-01"]))
    assert horizon.mask(incidents, helix=True).tolist() == [False, True, True, False]


def test_temporal_pair_policy_remains_stricter_than_prefilter():
    from nps_lens.analytics.linking_policy import temporal_mask

    assert temporal_mask(
        "2026-08-01", pd.Series(["2026-08-02", "2026-05-02", "2026-05-03", "2026-08-01"]), 90
    ).tolist() == [False, False, True, True]


def test_helix_classification_and_linking_share_ids_and_retain_categories(helix, monkeypatch):
    handler, ctx, frame, incidents, client = helix
    frame.drop(frame.index[2:], inplace=True)
    frame["Fecha"] = pd.to_datetime(["2026-08-01", "2026-08-31"])
    frame["Canal"] = ["Web", "App"]
    incidents = incidents.iloc[:6].copy()
    incidents["Submit Date"] = pd.to_datetime(
        ["2026-05-03", "2026-08-10", "2026-08-10", "2026-08-10", "2026-05-02", "2026-09-01"]
    )
    incidents["BBVA_SourceServiceN2"] = ["Web", "App", "Other", "Web", "Web", "Web"]
    incidents.loc[3, "Detailed Description"] = ""
    dashboard = client.app.state.dashboard_service
    dashboard.settings.service_origin_n2_map[ctx.service_origin] = {"Web": ["Web"], "App": ["App"]}
    monkeypatch.setattr(dashboard, "_load_helix_df", lambda context: incidents)
    scope = dict(pop_year="2026", pop_month="08", max_days_apart=90, score_channel="Web")
    inputs = handler.inputs(ctx, incidents, "SOURCE", channel_assignments=["Web"], **scope)
    ids = {row["id"] for row in inputs["incidents"]}
    assert ids == {"INC-0"}
    _, _, linking, _ = dashboard.causal_scope(ctx, dashboard._load_nps_df(ctx), **scope)
    assert set(linking["Incident Number"]) == ids
    diagnostic = dashboard.linking_dashboard(context=ctx, **scope)["diagnostics"]
    assert diagnostic["helix_after_period"] == 2
    assert diagnostic["helix_quality_eligible"] == 1
    assert diagnostic["exclusions"]["helix_quality"] == 1
    params = {"service_origin": "Bank", "service_origin_n1": "Web", **scope}
    assert (
        client.get("/api/taxonomy/helix", params=params).json()["total"]
        == dashboard.classification_status("helix", ctx, **scope)["total"]
        == 1
    )
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    sent = {
        row["id"]
        for name, batch in request.items()
        if name.startswith("incidents/")
        for row in batch["incidents"]
    }
    assert sent == ids
    handler.import_response(ctx, inputs, zipped(helix_response(request, "no-link")))
    app_scope = {**scope, "score_channel": "App"}
    changed = handler.inputs(ctx, incidents, "SOURCE", channel_assignments=["App"], **app_scope)
    assert {row["id"] for row in changed["incidents"]} == {"INC-1"}
    assert inputs["scopes"] != changed["scopes"]
    assert inputs["classification_scopes"] == changed["classification_scopes"]
    app_request = exported(handler.export(ctx, changed)["saved_paths"])
    handler.import_response(ctx, changed, zipped(helix_response(app_request, "no-link")))
    assert handler.status(ctx, inputs)["received"] == handler.status(ctx, changed)["received"] == 1
    assert handler.export(ctx, inputs)["saved_paths"] == []
    assert (
        client.get("/api/taxonomy/helix", params={**params, "score_channel": "App"}).json()[
            "received"
        ]
        == 1
    )
    with handler.repository._connect() as db:
        assert {row[0] for row in db.execute("SELECT incident FROM helix_incident_categories")} == {
            "INC-0",
            "INC-1",
        }
