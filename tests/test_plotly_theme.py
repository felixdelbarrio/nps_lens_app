from __future__ import annotations

from nps_lens.ui.plotly_theme import build_plotly_template, build_plotly_theme
from nps_lens.ui.theme import get_theme


def test_build_plotly_theme_uses_chart_tokens_from_theme() -> None:
    theme = get_theme("dark")

    pt = build_plotly_theme(theme)

    assert pt.paper_bg == theme.chart_paper
    assert pt.plot_bg == theme.chart_plot
    assert pt.grid == theme.chart_grid
    assert pt.zero_line == theme.chart_zero_line


def test_build_plotly_template_uses_tokenized_surfaces() -> None:
    theme = get_theme("dark")
    tpl = build_plotly_template(theme)

    layout = tpl["layout"]
    assert layout["paper_bgcolor"] == theme.chart_paper
    assert layout["plot_bgcolor"] == theme.chart_plot
    assert layout["xaxis"]["gridcolor"] == theme.chart_grid
    assert layout["yaxis"]["gridcolor"] == theme.chart_grid
