from __future__ import annotations

from nps_lens.design.tokens import (
    DesignTokens,
    nps_group_band,
    nps_score_color,
    plotly_nps_score_scale,
)


def test_nps_score_color_respects_absolute_business_thresholds() -> None:
    tokens = DesignTokens.default()

    assert nps_score_color(tokens, "light", 1) == "#c30a0a"
    assert nps_score_color(tokens, "light", 4) == "#ff5252"
    assert nps_score_color(tokens, "light", 7.5) == "#ffe761"
    assert nps_score_color(tokens, "light", 9.1) == "#42a64c"


def test_nps_group_band_normalizes_labels_before_falling_back_to_score() -> None:
    assert nps_group_band("Promotores", 2) == "promoter"
    assert nps_group_band("neutro", 10) == "passive"
    assert nps_group_band("", 5) == "detractor"


def test_plotly_nps_score_scale_uses_absolute_stops() -> None:
    tokens = DesignTokens.default()

    scale = plotly_nps_score_scale(tokens, "light")

    assert scale[0] == [0.0, "#c30a0a"]
    assert [0.6, "#ff5252"] in scale
    assert [0.8, "#ffe761"] in scale
    assert scale[-1] == [1.0, "#42a64c"]
