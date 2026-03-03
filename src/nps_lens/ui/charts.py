from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import plotly.express as px

from nps_lens.design.tokens import DesignTokens, primary_color


@dataclass(frozen=True)
class ChartTheme:
    accent: str
    navy: str


def _theme(repo_root) -> ChartTheme:
    t = DesignTokens.load(repo_root)
    return ChartTheme(accent=primary_color(t), navy=t.core.get("bbva_navy_900", "#070E46"))


def chart_nps_trend(df: pd.DataFrame, repo_root, freq: str = "W"):
    """NPS trend over time (business-friendly)."""
    th = _theme(repo_root)
    tmp = df.copy()
    tmp = tmp.dropna(subset=["Fecha", "NPS"])
    if tmp.empty:
        return None
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
    fig.update_layout(
        height=320,
        margin=dict(l=10, r=10, t=10, b=10),
        yaxis_title="NPS (media del score 0–10)",
        xaxis_title="Periodo",
        showlegend=False,
    )
    # Token-aligned accent
    fig.update_traces(line_color=th.navy, marker_color=th.accent)
    return fig


def chart_driver_bar(driver_df: pd.DataFrame, repo_root, top_k: int = 12):
    """Bar chart for driver gaps vs overall."""
    if driver_df.empty:
        return None
    th = _theme(repo_root)
    d = driver_df.head(top_k).copy()
    # DriverStat has: value, n, nps, gap_vs_overall
    fig = px.bar(
        d,
        x="gap_vs_overall",
        y="value",
        orientation="h",
        hover_data={"n": True, "nps": ":.2f", "gap_vs_overall": ":.2f"},
    )
    fig.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title="Diferencia vs NPS global (puntos)",
        yaxis_title="",
        showlegend=False,
    )
    fig.update_traces(marker_color=th.accent)
    return fig


def chart_topic_bars(topics_df: pd.DataFrame, repo_root, top_k: int = 10):
    """Topic clusters by size."""
    if topics_df.empty:
        return None
    th = _theme(repo_root)
    d = topics_df.sort_values("n", ascending=False).head(top_k).copy()
    d["label"] = d.apply(lambda r: f"#{int(r['cluster_id'])}: {', '.join(list(r['top_terms'])[:3])}", axis=1)
    fig = px.bar(d, x="n", y="label", orientation="h")
    fig.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title="Volumen (n comentarios)",
        yaxis_title="",
        showlegend=False,
    )
    fig.update_traces(marker_color=th.accent)
    return fig


def chart_cohort_heatmap(
    df: pd.DataFrame,
    repo_root,
    row_dim: str,
    col_dim: str,
    score_col: str = "NPS",
    min_n: int = 30,
):
    """Heatmap for business users: where NPS is weaker by cohort.

    Cells with low sample size are filtered out (min_n) to avoid over-reading noise.
    """
    if row_dim not in df.columns or col_dim not in df.columns or score_col not in df.columns:
        return None
    th = _theme(repo_root)
    tmp = df.dropna(subset=[row_dim, col_dim, score_col]).copy()
    if tmp.empty:
        return None

    tmp[row_dim] = tmp[row_dim].astype(str)
    tmp[col_dim] = tmp[col_dim].astype(str)

    agg = (
        tmp.groupby([row_dim, col_dim], as_index=False)
        .agg(n=(score_col, "size"), nps=(score_col, "mean"))
    )
    agg = agg.loc[agg["n"] >= int(min_n)].copy()
    if agg.empty:
        return None

    # Pivot to matrix
    pivot = agg.pivot(index=row_dim, columns=col_dim, values="nps").fillna(0.0)
    fig = px.imshow(
        pivot,
        aspect="auto",
        labels=dict(x=col_dim, y=row_dim, color="NPS medio"),
    )
    fig.update_layout(
        height=420,
        margin=dict(l=10, r=10, t=10, b=10),
        coloraxis_colorbar=dict(title="NPS"),
    )
    # token accent (title / axes)
    fig.update_layout(font=dict(color=th.navy))
    return fig


def chart_driver_delta(
    delta_df: pd.DataFrame,
    repo_root,
    top_k: int = 12,
):
    """Bar chart: biggest deteriorations/improvements vs baseline by driver."""
    if delta_df.empty:
        return None
    th = _theme(repo_root)
    d = delta_df.head(top_k).copy()
    fig = px.bar(
        d,
        x="delta_nps",
        y="value",
        orientation="h",
        hover_data={"n_current": True, "n_baseline": True, "nps_current": ":.2f", "nps_baseline": ":.2f"},
    )
    fig.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title="Δ NPS (periodo actual - periodo base)",
        yaxis_title="",
        showlegend=False,
    )
    fig.update_traces(marker_color=th.accent)
    return fig
