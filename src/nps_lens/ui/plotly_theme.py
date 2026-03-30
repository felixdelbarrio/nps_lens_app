"""Plotly theming utilities.

Single source of truth for Plotly styling (dark/light) driven by DesignTokens.
This ensures that exported images keep the same look (template attached to figure),
not just in-browser CSS.

Design goals:
- Lazy Plotly imports for fast Streamlit cold-start.
- Best-effort application: theming must never crash rendering.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from nps_lens.design.tokens import DesignTokens, primary_accent
from nps_lens.ui.theme import Theme


@dataclass(frozen=True)
class PlotlyTheme:
    accent: str
    detractor: str
    passive: str
    promoter: str
    text: str
    muted: str
    grid: str
    zero_line: str
    paper_bg: str
    plot_bg: str
    surface: str


def build_plotly_theme(theme: Theme) -> PlotlyTheme:
    toks = DesignTokens.default()
    pal = toks.colors_dark if theme.mode == "dark" else toks.colors_light
    detr = pal["color.primary.bg.alert"]
    pas = pal["color.primary.bg.warning"]
    pro = pal["color.primary.bg.success"]

    return PlotlyTheme(
        accent=primary_accent(toks, theme.mode),
        detractor=detr,
        passive=pas,
        promoter=pro,
        text=theme.text,
        muted=theme.muted,
        grid=theme.chart_grid,
        zero_line=theme.chart_zero_line,
        # Keep charts on tokenized chart surfaces (light/dark aware).
        paper_bg=theme.chart_paper,
        plot_bg=theme.chart_plot,
        surface=theme.chart_paper,
    )


def build_plotly_template(theme: Theme) -> Dict[str, Any]:
    """Return a Plotly template dict derived from tokens."""

    pt = build_plotly_theme(theme)
    return {
        "layout": {
            "paper_bgcolor": pt.paper_bg,
            "plot_bgcolor": pt.plot_bg,
            "font": {
                "color": pt.text,
                "family": "Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial",
            },
            "title": {"font": {"color": pt.text}},
            # Keep an explicit top gutter for the modebar (camera/zoom/etc.).
            "margin": {"l": 16, "r": 16, "t": 62, "b": 16},
            "colorway": [pt.accent, pt.detractor, pt.passive, pt.promoter],
            "legend": {
                "bgcolor": "rgba(0,0,0,0)",
                "bordercolor": pt.grid,
                "font": {"color": pt.text},
                "title": {"font": {"color": pt.text}},
            },
            "hoverlabel": {"bgcolor": pt.paper_bg, "font": {"color": pt.text}},
            "xaxis": {
                "title": {"font": {"color": pt.text}},
                "tickfont": {"color": pt.text},
                "gridcolor": pt.grid,
                "linecolor": pt.grid,
                "zerolinecolor": pt.zero_line,
                "automargin": True,
            },
            "yaxis": {
                "title": {"font": {"color": pt.text}},
                "tickfont": {"color": pt.text},
                "gridcolor": pt.grid,
                "linecolor": pt.grid,
                "zerolinecolor": pt.zero_line,
                "automargin": True,
            },
        }
    }


def apply_plotly_theme(fig: Any, theme: Theme, *, template_name: str = "nps_lens") -> Any:
    """Attach the token-driven template to a Plotly figure (best-effort)."""

    # Lazy import to keep cold-start fast in Streamlit.
    import plotly.io as pio  # type: ignore

    tpl = build_plotly_template(theme)
    pt = build_plotly_theme(theme)

    try:
        # Register named template so other modules can reference it.
        pio.templates[template_name] = tpl  # type: ignore[index]

        fig.update_layout(
            template=tpl,
            paper_bgcolor=pt.paper_bg,
            plot_bgcolor=pt.plot_bg,
            font=dict(color=pt.text),
            legend=dict(
                bgcolor="rgba(0,0,0,0)",
                bordercolor=pt.grid,
                font=dict(color=pt.text),
                title=dict(font=dict(color=pt.text)),
            ),
            hoverlabel=dict(bgcolor=pt.paper_bg, font=dict(color=pt.text)),
        )  # type: ignore[attr-defined]
        fig.update_xaxes(
            showgrid=True,
            gridcolor=pt.grid,
            zerolinecolor=pt.zero_line,
            showline=True,
            linecolor=pt.grid,
            tickfont=dict(color=pt.text),
            title_font=dict(color=pt.text),
        )  # type: ignore[attr-defined]
        fig.update_yaxes(
            showgrid=True,
            gridcolor=pt.grid,
            zerolinecolor=pt.zero_line,
            showline=True,
            linecolor=pt.grid,
            tickfont=dict(color=pt.text),
            title_font=dict(color=pt.text),
        )  # type: ignore[attr-defined]
    except Exception:
        return fig
    return fig


def themed_plotly_chart(
    st_mod: Any,
    fig: Any,
    theme: Theme,
    *,
    use_container_width: bool = True,
    config: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> None:
    """Render a Plotly figure in Streamlit with token-driven theming applied.

    Pass `st` as `st_mod` so this module stays UI-framework-agnostic.
    """

    fig2 = apply_plotly_theme(fig, theme)
    st_mod.plotly_chart(
        fig2,
        use_container_width=use_container_width,
        config=config,
        theme=None,
        **kwargs,
    )
