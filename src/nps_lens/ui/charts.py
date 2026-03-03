from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from nps_lens.design.tokens import DesignTokens, primary_accent
from nps_lens.ui.theme import Theme


@dataclass(frozen=True)
class ChartTheme:
    accent: str
    text: str
    grid: str
    paper_bg: str
    plot_bg: str


def chart_theme(theme: Theme) -> ChartTheme:
    toks = DesignTokens.default()
    return ChartTheme(
        accent=primary_accent(toks, theme.mode),
        text=theme.text,
        grid=theme.border,
        paper_bg="rgba(0,0,0,0)",
        plot_bg="rgba(0,0,0,0)",
    )


def _layout_common(fig, th: ChartTheme, *, height: int) -> None:
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=10, b=10),
        font=dict(color=th.text),
        paper_bgcolor=th.paper_bg,
        plot_bgcolor=th.plot_bg,
    )
    fig.update_xaxes(showgrid=True, gridcolor=th.grid, zeroline=False)
    fig.update_yaxes(showgrid=False, zeroline=False)


def chart_nps_trend(df: pd.DataFrame, theme: Theme, freq: str = "W"):
    """NPS trend over time (business-friendly)."""
    tmp = df.dropna(subset=["Fecha", "NPS"]).copy()
    if tmp.empty:
        return None

    th = chart_theme(theme)
    import plotly.express as px  # lazy import for faster cold-start
    tmp["period"] = tmp["Fecha"].dt.to_period(freq).dt.start_time
    agg = tmp.groupby("period", as_index=False).agg(n=("NPS", "size"), nps=("NPS", "mean"))
    fig = px.line(
        agg,
        x="period",
        y="nps",
        markers=True,
        hover_data={"n": True, "nps": ":.2f"},
    )
    fig.update_traces(line=dict(width=3), marker=dict(size=8))
    fig.update_traces(line_color=th.text, marker_color=th.accent)
    fig.update_layout(
        yaxis_title="NPS (media del score 0-10)",
        xaxis_title="Periodo",
        showlegend=False,
    )
    _layout_common(fig, th, height=320)
    return fig


def chart_driver_bar(driver_df: pd.DataFrame, theme: Theme, top_k: int = 12):
    """Bar chart for driver gaps vs overall.

    Requires 'gap_vs_overall' column in driver_df.
    """
    if driver_df.empty:
        return None
    th = chart_theme(theme)
    import plotly.express as px  # lazy import for faster cold-start
    d = driver_df.head(top_k).copy()
    if "gap_vs_overall" not in d.columns:
        raise ValueError("driver_df must include gap_vs_overall")
    fig = px.bar(
        d,
        x="gap_vs_overall",
        y="value",
        orientation="h",
        hover_data={"n": True, "nps": ":.2f", "gap_vs_overall": ":.2f"},
    )
    fig.update_traces(marker_color=th.accent)
    fig.update_layout(
        xaxis_title="Diferencia vs NPS global (puntos)",
        yaxis_title="",
        showlegend=False,
    )
    _layout_common(fig, th, height=360)
    return fig


def chart_topic_bars(topics_df: pd.DataFrame, theme: Theme, top_k: int = 10):
    """Topic clusters by size."""
    if topics_df.empty:
        return None
    th = chart_theme(theme)
    import plotly.express as px  # lazy import for faster cold-start
    d = topics_df.sort_values("n", ascending=False).head(top_k).copy()

    def _topic_label(row: pd.Series) -> str:
        terms = list(row["top_terms"])[:3]
        cid = int(row["cluster_id"])
        return f"#{cid}: {', '.join(terms)}"

    d["label"] = d.apply(_topic_label, axis=1)
    fig = px.bar(d, x="n", y="label", orientation="h")
    fig.update_traces(marker_color=th.accent)
    fig.update_layout(xaxis_title="Volumen (n comentarios)", yaxis_title="", showlegend=False)
    _layout_common(fig, th, height=360)
    return fig


def chart_driver_delta(delta_df: pd.DataFrame, theme: Theme, top_k: int = 12):
    """Bar chart: biggest deteriorations/improvements vs baseline by driver."""
    if delta_df.empty:
        return None
    th = chart_theme(theme)
    import plotly.express as px  # lazy import for faster cold-start
    d = delta_df.head(top_k).copy()
    fig = px.bar(
        d,
        x="delta_nps",
        y="value",
        orientation="h",
        hover_data={
            "n_current": True,
            "n_baseline": True,
            "nps_current": ":.2f",
            "nps_baseline": ":.2f",
        },
    )
    fig.update_traces(marker_color=th.accent)
    fig.update_layout(xaxis_title="Delta NPS (actual - base)", yaxis_title="", showlegend=False)
    _layout_common(fig, th, height=360)
    return fig


def chart_cohort_heatmap(
    df: pd.DataFrame,
    theme: Theme,
    row_dim: str,
    col_dim: str,
    score_col: str = "NPS",
    min_n: int = 30,
):
    """Heatmap: identify friction pockets across cohorts.

    Filters out low-sample cells (min_n) to avoid over-reading noise.
    """
    if row_dim not in df.columns or col_dim not in df.columns or score_col not in df.columns:
        return None
    th = chart_theme(theme)
    import plotly.express as px  # lazy import for faster cold-start
    tmp = df.dropna(subset=[row_dim, col_dim, score_col]).copy()
    if tmp.empty:
        return None

    tmp[row_dim] = tmp[row_dim].astype(str)
    tmp[col_dim] = tmp[col_dim].astype(str)

    agg = tmp.groupby([row_dim, col_dim], as_index=False).agg(
        n=(score_col, "size"),
        nps=(score_col, "mean"),
    )
    agg = agg.loc[agg["n"] >= int(min_n)].copy()
    if agg.empty:
        return None

    pivot = agg.pivot(index=row_dim, columns=col_dim, values="nps")
    fig = px.imshow(
        pivot,
        aspect="auto",
        labels=dict(x=col_dim, y=row_dim, color="NPS"),
    )
    fig.update_layout(coloraxis_colorbar=dict(title="NPS"))
    _layout_common(fig, th, height=420)
    return fig