from dataclasses import asdict

import pandas as pd
import pytest

from nps_lens.analytics.drivers import driver_table
from nps_lens.analytics.nps_gaps import rank_nps_gaps
from nps_lens.ui.narratives import explain_nps_gaps


def test_gaps_use_observed_counts_and_classic_nps_base() -> None:
    frame = pd.DataFrame(
        {
            "Palanca": ["A", "A", "B", "B", "B", "", None],
            "NPS": [0, 7, 10, 10, 8, 9, None],
        }
    )
    rows = rank_nps_gaps(frame, ["Palanca", "missing"], min_n=1)
    assert len(rows) == 1
    gap = rows[0]
    assert gap.value == "A"
    assert gap.nps == -50
    assert gap.gap_vs_base == pytest.approx(-50 - 100 / 3)
    assert gap.n == gap.valid_n == 2
    assert gap.detractors == 1
    assert gap.sample_share == pytest.approx(2 / 7)
    assert {"confidence", "priority", "potential_uplift"}.isdisjoint(asdict(gap))
    text = " ".join(explain_nps_gaps(pd.DataFrame([asdict(gap)]))).lower()
    assert "diferencia frente a la base" in text
    assert "1 detractores" in text
    assert not any(word in text for word in ["confianza", "estima", "recuperable", "potencial"])


def test_gaps_order_by_difference_then_sample_size_without_combining_scores() -> None:
    frame = pd.DataFrame({"Palanca": ["B", "A", "A", "C"], "NPS": [0, 0, 0, 10]})
    assert [r.value for r in rank_nps_gaps(frame, ["Palanca"], min_n=1)] == ["A", "B"]
    assert [r.value for r in rank_nps_gaps(frame, ["Palanca"], min_n=2)] == ["A"]
    assert rank_nps_gaps(frame, ["Palanca"], min_n=3) == []
    assert rank_nps_gaps(frame.iloc[:0], ["Palanca"], min_n=1) == []


def test_missing_scores_do_not_count_as_detractors() -> None:
    rows = driver_table(pd.DataFrame({"Palanca": ["A", "A"], "NPS": [None, 10]}), "Palanca")
    assert rows[0].n == 2
    assert rows[0].valid_n == 1
    assert rows[0].detractors == 0
    assert rows[0].nps == 100
