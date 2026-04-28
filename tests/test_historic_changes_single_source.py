from __future__ import annotations

import pandas as pd

from nps_lens.reports.executive_ppt import _build_dimension_view_model
from nps_lens.ui.historic_changes import get_changes_vs_historic


def _sample_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    palancas = ["Uso"] * 12 + ["Pagos"] * 12 + ["Seguridad"] * 12
    subpalancas = ["UX"] * 12 + ["Transferencias"] * 12 + ["Login"] * 12
    current_scores = [6, 7, 8, 9] * 3 + [6, 7, 8, 8] * 3 + [3, 4, 5, 6] * 3
    baseline_scores = [8, 8, 9, 9] * 3 + [9, 9, 10, 8] * 3 + [6, 7, 7, 8] * 3
    current = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2026-03-01"] * len(palancas)),
            "NPS": current_scores,
            "Palanca": palancas,
            "Subpalanca": subpalancas,
        }
    )
    baseline = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2026-02-01"] * len(palancas)),
            "NPS": baseline_scores,
            "Palanca": palancas,
            "Subpalanca": subpalancas,
        }
    )
    return current, baseline


def test_ppt_dimension_changes_use_insights_single_source_dataset() -> None:
    current, baseline = _sample_frames()
    insights_df = get_changes_vs_historic(
        current,
        baseline,
        dimension="Palanca",
        min_n=1,
    )
    vm = _build_dimension_view_model(
        dimension="Palanca",
        slide_number=5,
        selected_raw=current,
        current_source_period=current,
        baseline_source_period=baseline,
        current_label="2026-03-01 -> 2026-03-01",
        baseline_label="2026-02-01 -> 2026-02-01",
    )
    ppt_df = vm.change_df.reset_index(drop=True)
    pd.testing.assert_frame_equal(
        ppt_df.loc[:, insights_df.columns],
        insights_df,
        check_dtype=False,
        rtol=0,
        atol=0,
    )


def test_historic_changes_ranking_and_columns_are_deterministic() -> None:
    current, baseline = _sample_frames()
    out = get_changes_vs_historic(current, baseline, dimension="Subpalanca", min_n=1)
    assert list(out.columns) == [
        "value",
        "delta_nps",
        "nps_current",
        "nps_baseline",
        "n_current",
        "n_baseline",
    ]
    assert out["delta_nps"].tolist() == sorted(out["delta_nps"].tolist())
