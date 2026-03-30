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


def test_dark_palette_exposes_chart_and_table_tokens() -> None:
    tokens = DesignTokens.default()
    dark = tokens.colors_dark

    assert dark["color.app.chart.paper"] == "#11192d"
    assert dark["color.app.chart.plot"] == "#16203a"
    assert dark["color.app.chart.grid"] == "#3a4761"
    assert dark["color.app.table.bg"] == "#11192d"
    assert dark["color.app.table.header.bg"] == "#1b2844"
    assert dark["color.app.table.border"] == "#334056"
    assert dark["color.app.control.bg"] == "#16203a"
    assert dark["color.app.control.icon"] == "#85c8ff"
    assert dark["color.app.control.menu.item.selected"] == "#2b3f66"
