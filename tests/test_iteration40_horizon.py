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
    assert horizon.helix_start == date(2026, 3, 14)
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


def test_helix_horizon_reuses_categories_and_imports_after_month_change(helix):
    handler, ctx, frame, incidents, _ = helix
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
    handler.import_response(ctx, changed, zipped(helix_response(request, "no-link")))
    assert handler.status(ctx, inputs)["pending"] == 0
    assert handler.export(ctx, inputs)["saved_paths"] == []
    assert handler.status(ctx, changed)["received"] == 2
    assert handler.status(ctx, changed)["pending"] == 2
    with handler.repository._connect() as db:
        assert {
            row[0] for row in db.execute("SELECT incident FROM helix_incident_categories")
        } == sent
    # Changing the date doesn't require a new category; a new link scope still validates pairs.
    incidents.loc[incidents.index[1], "Submit Date"] = pd.Timestamp("2026-06-04")
    moved = handler.inputs(ctx, incidents, "SOURCE", **SEPTEMBER)
    assert handler.status(ctx, moved)["pending"] == 0
    assert handler.export(ctx, moved)["saved_paths"] == []
    assert handler.export(ctx, moved, only_linking=True)["pending"] == 1


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
