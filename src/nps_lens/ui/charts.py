from __future__ import annotations

import contextlib
from dataclasses import dataclass

import numpy as np
import pandas as pd

from nps_lens.core.nps_math import daily_metrics as shared_daily_metrics
from nps_lens.design.tokens import (
    DesignTokens,
    nps_score_color,
    palette,
    plotly_nps_score_scale,
    plotly_risk_scale,
)
from nps_lens.ui.plotly_theme import apply_plotly_theme
from nps_lens.ui.theme import Theme


def apply_plotly_template(fig: object, theme: Theme) -> object:
    """Apply the project Plotly template (token-driven)."""

    return apply_plotly_theme(fig, theme)


@dataclass(frozen=True)
class ChartTheme:
    """Minimal layout theme used by a few chart helpers.

    Plotly visuals are primarily styled via the token-driven template in
    ``nps_lens.ui.plotly_theme``. This dataclass only provides a small set of
    colors for layout defaults in charts that still apply explicit layout.
    """

    paper_bg: str
    plot_bg: str
    text: str
    grid: str
    accent: str


def chart_theme(theme: Theme) -> ChartTheme:
    """Derive minimal layout colors from design tokens."""

    return ChartTheme(
        paper_bg=theme.chart_paper,
        plot_bg=theme.chart_plot,
        text=theme.text,
        grid=theme.chart_grid,
        accent=theme.accent,
    )


def _status_colors(theme: Theme) -> tuple[str, str, str]:
    """Return semantic status colors (detractor, passive/warn, promoter)."""

    tokens = DesignTokens.default()
    p = palette(tokens, theme.mode)
    detr = p["color.primary.bg.alert"]
    warn = p["color.primary.bg.warning"]
    prom = p["color.primary.bg.success"]
    return detr, warn, prom


def _to_hex(color: str) -> str:
    """Normalize a color string to #rrggbb.

    Supports:
    - #RGB / #RRGGBB
    - rgb(r,g,b)
    - rgba(r,g,b,a)  (alpha ignored)

    Falls back to a safe neutral if the input is malformed.
    """

    c = (color or "").strip()
    if not c:
        return "#000000"

    if c.startswith("#"):
        h = c[1:]
        if len(h) == 3:
            return "#" + "".join([ch * 2 for ch in h.lower()])
        if len(h) >= 6:
            return "#" + h[0:6].lower()

    low = c.lower()
    if low.startswith("rgb(") or low.startswith("rgba("):
        inside = low.split("(", 1)[1].rsplit(")", 1)[0]
        parts = [p.strip() for p in inside.split(",")]
        if len(parts) >= 3:
            try:
                r = max(0, min(255, int(float(parts[0]))))
                g = max(0, min(255, int(float(parts[1]))))
                b = max(0, min(255, int(float(parts[2]))))
                return f"#{r:02x}{g:02x}{b:02x}"
            except Exception:
                return "#000000"

    # Unknown format (named colors, css vars). Return a safe neutral.
    return "#000000"


def _mix_hex(a: str, b: str, t: float) -> str:
    """Linear mix between two colors (inputs normalized to hex)."""
    a_hex = _to_hex(a).lstrip("#")
    b_hex = _to_hex(b).lstrip("#")
    ar, ag, ab = int(a_hex[0:2], 16), int(a_hex[2:4], 16), int(a_hex[4:6], 16)
    br, bg, bb = int(b_hex[0:2], 16), int(b_hex[2:4], 16), int(b_hex[4:6], 16)
    r = int(ar + (br - ar) * t)
    g = int(ag + (bg - ag) * t)
    b_ = int(ab + (bb - ab) * t)
    return f"#{r:02x}{g:02x}{b_:02x}"


def _shade(base: str, *, toward: str, t: float) -> str:
    """Lighten/darken base by mixing towards another color."""
    return _mix_hex(base, toward, max(0.0, min(1.0, float(t))))


def _diverging_colors(theme: Theme, values: pd.Series) -> list[str]:
    """Map signed values to semantic colors (red/yellow/green) with intensity.

    - Positive deltas -> green shades
    - Negative deltas -> red shades
    - Near zero -> yellow/neutral

    Intensity scales with |value| within the current series.
    """
    detr_c, pas_c, pro_c = _status_colors(theme)
    v = pd.to_numeric(values, errors="coerce").fillna(0.0)
    vmax = float(v.abs().max()) if len(v) else 0.0
    vmax = vmax if vmax > 1e-9 else 1.0

    out: list[str] = []
    for x in v.tolist():
        strength = min(1.0, abs(float(x)) / vmax)
        if x > 0:
            # darker green for stronger improvements
            out.append(_shade(pro_c, toward="#001a0f", t=0.35 * strength))
        elif x < 0:
            # darker red for stronger deteriorations
            out.append(_shade(detr_c, toward="#1a0000", t=0.35 * strength))
        else:
            out.append(pas_c)
    return out


def _colorscale_rgy(theme: Theme) -> list[list[object]]:
    tokens = DesignTokens.default()
    return plotly_nps_score_scale(tokens, theme.mode)


def _nps_score_colors(theme: Theme, values: pd.Series) -> list[str]:
    tokens = DesignTokens.default()
    return [nps_score_color(tokens, theme.mode, value) for value in values.tolist()]


def _layout_common(fig, th: ChartTheme, *, height: int) -> None:
    fig.update_layout(
        height=height,
        # Reserve a top lane so Plotly's modebar never overlaps chart data.
        margin=dict(l=10, r=10, t=62, b=10),
        font=dict(color=th.text),
        paper_bgcolor=th.paper_bg,
        plot_bgcolor=th.plot_bg,
    )
    fig.update_xaxes(showgrid=True, gridcolor=th.grid, zeroline=False)
    fig.update_yaxes(showgrid=False, zeroline=False)


def _compact_axis_label(
    value: object,
    *,
    width: int = 18,
    max_lines: int = 2,
    max_chars: int = 40,
) -> str:
    clean = " ".join(str(value or "").split())
    if not clean:
        return ""
    if len(clean) > int(max_chars):
        clean = clean[: max(int(max_chars) - 1, 1)].rstrip() + "…"
    words = clean.split()
    if not words:
        return clean

    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}".strip()
        if len(candidate) <= max(int(width), 8):
            current = candidate
            continue
        lines.append(current)
        current = word
        if len(lines) >= max(int(max_lines) - 1, 0):
            break
    if len(lines) < int(max_lines):
        lines.append(current)
    if len(lines) > int(max_lines):
        lines = lines[: int(max_lines)]
    if len(lines[-1]) > int(width):
        lines[-1] = lines[-1][: max(int(width) - 1, 1)].rstrip() + "…"
    return "<br>".join(lines[: int(max_lines)])


def chart_nps_trend(df: pd.DataFrame, theme: Theme, freq: str = "W"):
    """NPS trend over time (business-friendly)."""
    tmp = df.dropna(subset=["Fecha", "NPS"]).copy()
    if tmp.empty:
        return None

    th = chart_theme(theme)
    import plotly.graph_objects as go  # lazy import for faster cold-start

    tmp["period"] = tmp["Fecha"].dt.to_period(freq).dt.start_time
    agg = tmp.groupby("period", as_index=False).agg(n=("NPS", "size"), nps=("NPS", "mean"))
    marker_colors = _nps_score_colors(theme, agg["nps"])
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=agg["period"],
            y=agg["nps"],
            mode="lines+markers",
            line=dict(width=3, color=th.accent),
            marker=dict(
                size=8,
                color=marker_colors,
                line=dict(color=th.paper_bg, width=1),
            ),
            customdata=agg["n"],
            hovertemplate="Periodo=%{x}<br>NPS=%{y:.2f}<br>Muestras=%{customdata}<extra></extra>",
        )
    )
    fig.update_layout(
        yaxis_title="NPS (media del score 0-10)",
        xaxis_title="Periodo",
        showlegend=False,
    )
    _layout_common(fig, th, height=320)
    return apply_plotly_template(fig, theme)


def chart_daily_score_ladder(
    df: pd.DataFrame,
    theme: Theme,
    *,
    days: int = 60,
    date_col: str = "Fecha",
    score_col: str = "NPS",
):
    """Daily 'ladder' view of the raw 0-10 scores.

    Business goal: see the real distribution per day (not only averages).
    Implementation: heatmap day x score with counts.
    """

    if date_col not in df.columns or score_col not in df.columns:
        return None

    tmp = df.dropna(subset=[date_col, score_col]).copy()
    if tmp.empty:
        return None

    # Normalize to day and keep a bounded window for UI performance.
    day = pd.to_datetime(tmp[date_col], errors="coerce")
    # Strip timezone to avoid window comparisons dropping all rows.
    with contextlib.suppress(Exception):
        day = day.dt.tz_localize(None)
    tmp["day"] = day.dt.floor("D")
    tmp = tmp.dropna(subset=["day"]).copy()
    if tmp.empty:
        return None

    end = tmp["day"].max()
    start = end - pd.Timedelta(days=int(days) - 1)
    tmp = tmp.loc[tmp["day"] >= start].copy()
    if tmp.empty:
        return None

    # Clamp to 0..10, coerce non-numeric safely.
    scores = pd.to_numeric(tmp[score_col], errors="coerce")
    tmp["score"] = scores.clip(lower=0, upper=10).round().astype("Int64")
    tmp = tmp.dropna(subset=["score"]).copy()
    if tmp.empty:
        return None

    agg = tmp.groupby(["day", "score"], as_index=False).size().rename(columns={"size": "count"})
    # Ensure stable 0..10 columns.
    pivot = agg.pivot(index="day", columns="score", values="count").fillna(0.0)
    for s in range(0, 11):
        if s not in pivot.columns:
            pivot[s] = 0.0
    pivot = pivot[sorted(pivot.columns)].copy()

    th = chart_theme(theme)
    import plotly.express as px  # lazy import

    fig = px.imshow(
        pivot.T,
        aspect="auto",
        labels=dict(x="Dia", y="Score (0-10)", color="n"),
    )
    fig.update_layout(
        xaxis_title="Dia",
        yaxis_title="Score (0-10)",
    )
    _layout_common(fig, th, height=360)
    return apply_plotly_template(fig, theme)


def _weekday_letter_es(ts: pd.Timestamp) -> str:
    # Monday=0 .. Sunday=6
    letters = ["L", "M", "X", "J", "V", "S", "D"]
    return letters[int(ts.weekday())]


def _apply_day_ticks(
    fig,
    days: list[pd.Timestamp],
    *,
    max_ticks: int = 21,
    side: str = "bottom",
) -> None:
    """Apply business-friendly day ticks: weekday letter + date.

    We avoid clutter by subsampling if there are too many days.
    """

    if not days:
        return

    ordered_days: list[pd.Timestamp] = []
    seen: set[pd.Timestamp] = set()
    for raw_day in days:
        day = pd.Timestamp(raw_day).normalize()
        if day in seen:
            continue
        ordered_days.append(day)
        seen.add(day)
    if not ordered_days:
        return

    step = max(1, int(np.ceil(len(ordered_days) / max(max_ticks, 1))))
    tick_days = ordered_days[::step]
    ticktext = [f"{_weekday_letter_es(d)}<br>{d.strftime('%d/%m')}" for d in tick_days]
    tick_position = "outside top" if str(side).lower() == "top" else "outside bottom"
    fig.update_xaxes(
        tickmode="array",
        tickvals=tick_days,
        ticktext=ticktext,
        tickangle=0,
        ticklabelposition=tick_position,
        side=side,
        automargin=True,
    )


def chart_daily_kpis(
    df: pd.DataFrame,
    theme: Theme,
    *,
    days: int = 60,
    metrics: pd.DataFrame | None = None,
):
    """Daily time-series for detractor rate and classic NPS.

    Complements the ladder heatmap: the heatmap shows distribution, this chart shows
    % detractors and classic NPS (promoters - detractors) per day.
    """
    if metrics is not None and not metrics.empty:
        required = {"day", "n", "classic_nps", "det_pct"}
        if not required.issubset(set(metrics.columns)):
            return None
        agg = metrics[["day", "n", "classic_nps", "det_pct"]].copy()
        agg = agg.sort_values("day")
    else:
        agg = shared_daily_metrics(df, days=int(days))
        if agg.empty:
            return None

    th = chart_theme(theme)
    import plotly.graph_objects as go  # lazy import
    from plotly.subplots import make_subplots  # lazy import

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(
            x=agg["day"],
            y=agg["classic_nps"],
            mode="lines+markers",
            name="NPS clásico",
            hovertemplate=(
                "Día=%{x|%Y-%m-%d}<br>" "NPS clásico=%{y:.1f}<br>" "n=%{customdata}<extra></extra>"
            ),
            customdata=agg["n"],
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=agg["day"],
            y=agg["det_pct"],
            mode="lines",
            name="% detractores",
            hovertemplate=(
                "Día=%{x|%Y-%m-%d}<br>"
                "% detractores=%{y:.1f}%<br>"
                "n=%{customdata}<extra></extra>"
            ),
            customdata=agg["n"],
        ),
        secondary_y=True,
    )

    fig.update_traces(selector=dict(name="NPS clásico"), line=dict(width=3))
    fig.update_traces(selector=dict(name="% detractores"), line=dict(width=2, dash="dot"))
    fig.update_traces(selector=dict(name="NPS clásico"), line_color=th.accent)
    fig.update_traces(selector=dict(name="% detractores"), line_color=th.text)

    _layout_common(fig, th, height=300)
    fig.update_layout(
        showlegend=True,
        hovermode="x unified",
        legend=dict(orientation="h", x=0, y=1.18, title_text=""),
        margin=dict(l=10, r=10, t=92, b=18),
    )
    fig.update_xaxes(title_text="Día")
    fig.update_yaxes(title_text="NPS clásico (pp)", secondary_y=False, rangemode="tozero")
    fig.update_yaxes(title_text="% detractores", secondary_y=True, ticksuffix="%")
    _apply_day_ticks(
        fig,
        [pd.Timestamp(d) for d in agg["day"].tolist()],
        max_ticks=16,
        side="top",
    )
    return apply_plotly_template(fig, theme)


def chart_daily_mix_business(
    df: pd.DataFrame,
    theme: Theme,
    *,
    days: int = 60,
    metrics: pd.DataFrame | None = None,
):
    """Business-friendly daily view: promoters/passives/detractors mix.

    This is intentionally easier to interpret than the heatmap ladder:
    - Detractors (0-6) penalize NPS
    - Passives (7-8) are neutral
    - Promoters (9-10) are favorable

    Output: 100% stacked bars by day.
    """

    agg = metrics.copy() if metrics is not None else shared_daily_metrics(df, days=int(days))
    if agg.empty:
        return None
    agg = agg.rename(
        columns={
            "det_pct": "detractors",
            "pas_pct": "passives",
            "pro_pct": "promoters",
        }
    )[["day", "n", "detractors", "passives", "promoters"]].copy()

    th = chart_theme(theme)
    detr_c, pas_c, pro_c = _status_colors(theme)

    import plotly.graph_objects as go  # lazy import

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=agg["day"],
            y=agg["detractors"],
            name="Detractores (0-6)",
            marker_color=detr_c,
            customdata=agg[["n"]],
            hovertemplate=(
                "Día=%{x|%Y-%m-%d}<br>"
                "Detractores=%{y:.1f}%<br>"
                "n=%{customdata[0]}<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Bar(
            x=agg["day"],
            y=agg["passives"],
            name="Pasivos (7-8)",
            marker_color=pas_c,
            customdata=agg[["n"]],
            hovertemplate=(
                "Día=%{x|%Y-%m-%d}<br>" "Pasivos=%{y:.1f}%<br>" "n=%{customdata[0]}<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Bar(
            x=agg["day"],
            y=agg["promoters"],
            name="Promotores (9-10)",
            marker_color=pro_c,
            customdata=agg[["n"]],
            hovertemplate=(
                "Día=%{x|%Y-%m-%d}<br>"
                "Promotores=%{y:.1f}%<br>"
                "n=%{customdata[0]}<extra></extra>"
            ),
        )
    )

    _layout_common(fig, th, height=320)
    fig.update_layout(
        barmode="stack",
        yaxis_title="Mix diario (% de respuestas)",
        xaxis_title="Día",
        legend=dict(orientation="h", x=0, y=1.18, title_text=""),
        hovermode="x unified",
        bargap=0.14,
        margin=dict(l=10, r=10, t=92, b=18),
    )
    _apply_day_ticks(fig, [pd.Timestamp(d) for d in agg["day"].tolist()], max_ticks=31)
    fig.update_yaxes(range=[0, 100], ticksuffix="%")
    return apply_plotly_template(fig, theme)


def chart_daily_volume(
    df: pd.DataFrame,
    theme: Theme,
    *,
    days: int = 60,
    metrics: pd.DataFrame | None = None,
):
    """Daily response volume (n). Useful to avoid over-reading low-N days."""
    source = metrics if metrics is not None else shared_daily_metrics(df, days=int(days))
    if source.empty:
        return None
    agg = source[["day", "n"]].copy()

    th = chart_theme(theme)
    import plotly.graph_objects as go  # lazy import

    fig = go.Figure(
        data=[
            go.Bar(
                x=agg["day"],
                y=agg["n"],
                marker_color=th.grid,
                hovertemplate="Día=%{x|%Y-%m-%d}<br>Respuestas=%{y}<extra></extra>",
            )
        ]
    )
    fig.update_layout(xaxis_title="Día", yaxis_title="Respuestas (n)", showlegend=False)
    _layout_common(fig, th, height=220)
    _apply_day_ticks(
        fig,
        [pd.Timestamp(d) for d in agg["day"].tolist()],
        max_ticks=14,
        side="top",
    )
    return apply_plotly_template(fig, theme)


def chart_driver_bar(driver_df: pd.DataFrame, theme: Theme, top_k: int = 12):
    """Bar chart for driver gaps vs overall.

    Requires 'gap_vs_overall' column in driver_df.
    """
    if driver_df.empty:
        return None
    th = chart_theme(theme)
    import plotly.express as px  # lazy import for faster cold-start

    d = driver_df.copy()
    if "gap_vs_overall" not in d.columns:
        raise ValueError("driver_df must include gap_vs_overall")
    d = d.sort_values(["gap_vs_overall", "n"], ascending=[True, False]).head(top_k).copy()
    plot_df = d.iloc[::-1].copy()
    fig = px.bar(
        plot_df,
        x="gap_vs_overall",
        y="value",
        orientation="h",
        hover_data={"n": True, "nps": ":.2f", "gap_vs_overall": ":.2f"},
    )
    fig.update_traces(marker_color=_diverging_colors(theme, plot_df["gap_vs_overall"]))
    fig.update_layout(
        xaxis_title="Diferencia vs NPS global (puntos)",
        yaxis_title="",
        showlegend=False,
    )
    fig.update_yaxes(categoryorder="array", categoryarray=plot_df["value"].tolist())
    _layout_common(fig, th, height=360)
    return apply_plotly_template(fig, theme)


def chart_opportunities_bar(opp_df: pd.DataFrame, theme: Theme, top_k: int = 12):
    """Bar chart for prioritized opportunities (impact) with confidence intensity."""
    if opp_df.empty:
        return None
    th = chart_theme(theme)
    import plotly.express as px

    d = (
        opp_df.sort_values(["potential_uplift", "confidence"], ascending=[False, False])
        .head(top_k)
        .copy()
    )
    if "label" not in d.columns:
        d["label"] = d.apply(lambda r: f"{r.get('dimension')}={r.get('value')}", axis=1)

    # Intensity: mix accent toward background based on confidence
    conf = pd.to_numeric(d.get("confidence", 0.0), errors="coerce").fillna(0.0).clip(0.0, 1.0)
    colors: list[str] = []
    for c in conf.tolist():
        # higher confidence -> closer to accent
        colors.append(_shade(th.accent, toward=th.plot_bg, t=0.65 * (1.0 - float(c))))

    fig = px.bar(
        d,
        x="potential_uplift",
        y="label",
        orientation="h",
        hover_data={"n": True, "confidence": ":.2f", "potential_uplift": ":.1f"},
    )
    fig.update_traces(marker_color=colors)
    fig.update_layout(xaxis_title="Impacto estimado (puntos NPS)", yaxis_title="", showlegend=False)
    _layout_common(fig, th, height=360)
    return apply_plotly_template(fig, theme)


def chart_broken_journeys_bar(journey_df: pd.DataFrame, theme: Theme, top_k: int = 10):
    """Horizontal ranking of detected broken journeys."""

    if journey_df.empty:
        return None

    tmp = journey_df.copy()
    tmp["linked_pairs"] = pd.to_numeric(tmp.get("linked_pairs"), errors="coerce").fillna(0.0)
    tmp["avg_nps"] = pd.to_numeric(tmp.get("avg_nps"), errors="coerce")
    tmp["semantic_cohesion"] = pd.to_numeric(tmp.get("semantic_cohesion"), errors="coerce").fillna(
        0.0
    )
    tmp = tmp.sort_values(
        ["linked_pairs", "semantic_cohesion", "avg_nps"],
        ascending=[False, False, True],
    ).head(int(top_k))
    if tmp.empty:
        return None

    th = chart_theme(theme)
    import plotly.express as px

    tmp = tmp.iloc[::-1].copy()
    fig = px.bar(
        tmp,
        x="linked_pairs",
        y="journey_label",
        orientation="h",
        color="avg_nps",
        color_continuous_scale=_colorscale_rgy(theme),
        range_color=(0.0, 10.0),
        text="linked_pairs",
        hover_data={
            "touchpoint": True,
            "palanca": True,
            "subpalanca": True,
            "journey_keywords": True,
            "semantic_cohesion": ":.2f",
            "avg_nps": ":.2f",
        },
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        xaxis_title="Links validados Helix↔VoC",
        yaxis_title="Journey roto",
        coloraxis=dict(
            cmin=0.0,
            cmax=10.0,
            colorbar=dict(
                title="NPS medio",
                tickmode="array",
                tickvals=[0, 2, 6, 8, 10],
            ),
        ),
    )
    fig.update_yaxes(categoryorder="array", categoryarray=tmp["journey_label"].tolist())
    _layout_common(fig, th, height=max(320, 56 * len(tmp) + 80))
    return apply_plotly_template(fig, theme)


def chart_causal_entity_bar(
    summary_df: pd.DataFrame,
    theme: Theme,
    *,
    entity_label: str,
    top_k: int = 10,
):
    """Horizontal ranking for the active causal entity summary."""

    if summary_df.empty:
        return None

    tmp = summary_df.copy()
    for column in [
        "entity_label",
        "touchpoint",
        "palanca",
        "subpalanca",
        "anchor_topic",
    ]:
        if column not in tmp.columns:
            tmp[column] = ""
    tmp["linked_pairs"] = pd.to_numeric(tmp.get("linked_pairs"), errors="coerce").fillna(0.0)
    tmp["nps_points_at_risk"] = pd.to_numeric(
        tmp.get("nps_points_at_risk"), errors="coerce"
    ).fillna(0.0)
    tmp["avg_nps"] = pd.to_numeric(tmp.get("avg_nps"), errors="coerce")
    tmp = tmp.sort_values(
        ["linked_pairs", "nps_points_at_risk", "avg_nps"],
        ascending=[False, False, True],
    ).head(int(top_k))
    if tmp.empty:
        return None

    th = chart_theme(theme)
    import plotly.express as px

    plot_df = tmp.iloc[::-1].copy()
    plot_df["entity_label"] = plot_df["entity_label"].astype(str)
    fig = px.bar(
        plot_df,
        x="linked_pairs",
        y="entity_label",
        orientation="h",
        color="nps_points_at_risk",
        color_continuous_scale=_colorscale_rgy(theme),
        text="linked_pairs",
        hover_data={
            "touchpoint": True,
            "palanca": True,
            "subpalanca": True,
            "anchor_topic": True,
            "avg_nps": ":.2f",
            "nps_points_at_risk": ":.2f",
        },
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        xaxis_title="Links validados Helix↔VoC",
        yaxis_title=entity_label,
        coloraxis=dict(
            colorbar=dict(
                title="NPS en riesgo",
                tickfont=dict(size=10),
            )
        ),
    )
    fig.update_yaxes(categoryorder="array", categoryarray=plot_df["entity_label"].tolist())
    _layout_common(fig, th, height=max(320, 56 * len(plot_df) + 80))
    return apply_plotly_template(fig, theme)


def chart_incident_priority_matrix(
    rationale_df: pd.DataFrame,
    theme: Theme,
    *,
    top_k: int = 12,
):
    """Readable priority ranking (bar + confidence marker) for incident-driven topics."""
    if rationale_df.empty:
        return None
    required = {"nps_topic", "confidence", "nps_points_at_risk", "incidents", "priority"}
    if not required.issubset(set(rationale_df.columns)):
        return None

    d = (
        rationale_df.sort_values(["priority", "nps_points_at_risk"], ascending=False)
        .head(top_k)
        .copy()
    )
    if d.empty:
        return None

    d["short_topic"] = (
        d["nps_topic"]
        .astype(str)
        .map(lambda value: _compact_axis_label(value, width=20, max_lines=2, max_chars=36))
    )
    d["confidence"] = pd.to_numeric(d["confidence"], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    d["nps_points_at_risk"] = pd.to_numeric(d["nps_points_at_risk"], errors="coerce").fillna(0.0)
    d["priority"] = pd.to_numeric(d["priority"], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    d["incidents"] = pd.to_numeric(d["incidents"], errors="coerce").fillna(0.0).clip(lower=0.0)
    d = d.reset_index(drop=True)
    d["rank"] = np.arange(1, len(d) + 1)
    d["topic_label"] = d.apply(
        lambda r: (
            f"TOP {int(r['rank'])} · {r['short_topic']}"
            if int(r["rank"]) <= 3
            else str(r["short_topic"])
        ),
        axis=1,
    )
    d = d.iloc[::-1].copy()

    th = chart_theme(theme)
    detr_c, warn_c, pro_c = _status_colors(theme)
    bar_colors = []
    for rk in d["rank"].tolist():
        if int(rk) == 1:
            bar_colors.append(detr_c)
        elif int(rk) == 2:
            bar_colors.append(warn_c)
        elif int(rk) == 3:
            bar_colors.append(pro_c)
        else:
            bar_colors.append(th.grid)

    import plotly.graph_objects as go

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=d["priority"],
            y=d["topic_label"],
            orientation="h",
            marker=dict(color=bar_colors),
            name="Prioridad",
            text=[f"{v:.2f}" for v in d["priority"].tolist()],
            textposition="outside",
            cliponaxis=False,
            customdata=np.column_stack([d["confidence"], d["nps_points_at_risk"], d["incidents"]]),
            hovertemplate=(
                "Tópico=%{y}<br>Prioridad=%{x:.2f}<br>"
                "Confianza=%{customdata[0]:.2f}<br>"
                "NPS en riesgo=%{customdata[1]:.2f}<br>"
                "Incidencias=%{customdata[2]:.0f}<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=d["confidence"],
            y=d["topic_label"],
            mode="markers",
            name="Confianza",
            marker=dict(color=th.accent, size=11, symbol="diamond"),
            hovertemplate="Tópico=%{y}<br>Confianza=%{x:.2f}<extra></extra>",
        )
    )
    fig.update_layout(
        xaxis_title="Indice 0-1 (barra=prioridad, rombo=confianza)",
        yaxis_title="",
        barmode="overlay",
        legend=dict(orientation="h", y=1.08, x=0),
    )
    fig.update_xaxes(range=[0, 1], dtick=0.1, gridcolor=th.grid)
    fig.update_yaxes(
        automargin=True,
        categoryorder="array",
        categoryarray=d["topic_label"].tolist(),
    )
    _layout_common(fig, th, height=max(430, 240 + 38 * min(int(top_k), len(d))))
    return apply_plotly_template(fig, theme)


def chart_incident_risk_recovery(
    rationale_df: pd.DataFrame,
    theme: Theme,
    *,
    top_k: int = 8,
):
    """Risk vs recoverable comparison using dumbbell chart (readable for committees)."""
    if rationale_df.empty:
        return None
    required = {"nps_topic", "nps_points_at_risk", "nps_points_recoverable"}
    if not required.issubset(set(rationale_df.columns)):
        return None

    d = rationale_df.sort_values(["priority", "nps_points_at_risk"], ascending=False).copy()
    if d.empty:
        return None

    d["nps_points_at_risk"] = pd.to_numeric(d["nps_points_at_risk"], errors="coerce").fillna(0.0)
    d["nps_points_recoverable"] = pd.to_numeric(
        d["nps_points_recoverable"], errors="coerce"
    ).fillna(0.0)
    d["signal"] = d[["nps_points_at_risk", "nps_points_recoverable"]].max(axis=1)
    d = d[d["signal"] > 0.0005].copy()
    if d.empty:
        return None
    d = d.head(top_k).copy()

    d["topic"] = (
        d["nps_topic"]
        .astype(str)
        .map(lambda value: _compact_axis_label(value, width=18, max_lines=2, max_chars=34))
    )
    d["gap"] = d["nps_points_at_risk"] - d["nps_points_recoverable"]
    d = d.sort_values(["nps_points_at_risk", "gap"], ascending=[True, True]).copy()

    detr_c, _, pro_c = _status_colors(theme)

    th = chart_theme(theme)
    import plotly.graph_objects as go

    fig = go.Figure()
    for _, row in d.iterrows():
        fig.add_shape(
            type="line",
            x0=float(row["nps_points_recoverable"]),
            y0=str(row["topic"]),
            x1=float(row["nps_points_at_risk"]),
            y1=str(row["topic"]),
            line=dict(color=th.grid, width=3),
        )
    fig.add_trace(
        go.Scatter(
            x=d["nps_points_at_risk"],
            y=d["topic"],
            mode="markers+text",
            name="NPS en riesgo",
            marker=dict(color=detr_c, size=11),
            text=[f"{v:.2f}" for v in d["nps_points_at_risk"].tolist()],
            textposition="middle right",
            cliponaxis=False,
            hovertemplate="Tópico=%{y}<br>NPS en riesgo=%{x:.2f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=d["nps_points_recoverable"],
            y=d["topic"],
            mode="markers+text",
            name="NPS recuperable",
            marker=dict(color=pro_c, size=11, symbol="diamond"),
            text=[f"{v:.2f}" for v in d["nps_points_recoverable"].tolist()],
            textposition="middle left",
            cliponaxis=False,
            hovertemplate="Tópico=%{y}<br>NPS recuperable=%{x:.2f}<extra></extra>",
        )
    )
    fig.update_layout(
        xaxis_title="Puntos NPS",
        yaxis_title="",
        legend_title_text="Metricas",
        legend=dict(orientation="h", y=1.08, x=0),
    )
    fig.update_yaxes(automargin=True, categoryorder="array", categoryarray=d["topic"].tolist())
    xmax = float(max(d["nps_points_at_risk"].max(), d["nps_points_recoverable"].max()))
    upper = xmax * 1.18 if xmax > 0 else 1.0
    dtick = 0.1 if upper <= 1.25 else 0.2 if upper <= 2.5 else None
    fig.update_xaxes(range=[0.0, upper], dtick=dtick)
    _layout_common(fig, th, height=max(420, 240 + 42 * min(int(top_k), len(d))))
    return apply_plotly_template(fig, theme)


def chart_case_incident_heatmap(
    by_topic_daily: pd.DataFrame,
    theme: Theme,
    *,
    topic: str,
):
    """Single-topic heat strip used in app detail tabs and PPT exports."""
    topic_key = str(topic or "").strip()
    if by_topic_daily is None or by_topic_daily.empty or not topic_key:
        return None

    g_heat = by_topic_daily[by_topic_daily["nps_topic"].astype(str).str.strip() == topic_key].copy()
    if g_heat.empty:
        return None
    g_heat["date"] = pd.to_datetime(g_heat["date"], errors="coerce")
    g_heat = g_heat.dropna(subset=["date"]).sort_values("date")
    if g_heat.empty:
        return None

    incidents = pd.to_numeric(g_heat.get("incidents"), errors="coerce").fillna(0.0).astype(float)
    th = chart_theme(theme)
    import plotly.graph_objects as go

    fig = go.Figure(
        data=[
            go.Heatmap(
                x=g_heat["date"].tolist(),
                y=["Incidencias"],
                z=[incidents.tolist()],
                zmin=0.0,
                xgap=2,
                ygap=2,
                colorscale=plotly_risk_scale(DesignTokens.default(), theme.mode),
                colorbar=dict(
                    title="Incidencias",
                    thickness=14,
                    len=0.72,
                    y=0.5,
                ),
                hovertemplate="Fecha=%{x|%Y-%m-%d}<br>Incidencias=%{z:.0f}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        xaxis_title="Día",
        yaxis_title="",
        showlegend=False,
    )
    _apply_day_ticks(fig, list(g_heat["date"].dt.normalize()), max_ticks=8)
    _layout_common(fig, th, height=280)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    return apply_plotly_template(fig, theme)


def chart_case_lag_days(
    by_topic_daily: pd.DataFrame,
    lag_days_by_topic: pd.DataFrame,
    theme: Theme,
    *,
    topic: str,
    focus_name: str,
):
    """Shared daily lag chart used by app tabs and PPT exports."""
    topic_key = str(topic or "").strip()
    if by_topic_daily is None or by_topic_daily.empty or not topic_key:
        return None
    if lag_days_by_topic is None or lag_days_by_topic.empty:
        return None
    if (
        "nps_topic" not in lag_days_by_topic.columns
        or "best_lag_days" not in lag_days_by_topic.columns
    ):
        return None

    active_lag_days = lag_days_by_topic[
        lag_days_by_topic["nps_topic"].astype(str).str.strip() == topic_key
    ].copy()
    if active_lag_days.empty:
        return None

    lagd_raw = pd.to_numeric(active_lag_days.iloc[0]["best_lag_days"], errors="coerce")
    if pd.isna(lagd_raw):
        return None
    lagd = int(lagd_raw)
    data = by_topic_daily[by_topic_daily["nps_topic"].astype(str).str.strip() == topic_key].copy()
    if data.empty:
        return None
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data.dropna(subset=["date"]).sort_values("date")
    if data.empty:
        return None

    data["focus_rate"] = pd.to_numeric(data.get("focus_rate"), errors="coerce").fillna(0.0)
    data["incidents"] = pd.to_numeric(data.get("incidents"), errors="coerce").fillna(0.0)
    data["incidents_shifted"] = data["incidents"].shift(lagd)

    th = chart_theme(theme)
    detr_c, _, _ = _status_colors(theme)
    import plotly.graph_objects as go

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=data["date"],
            y=data["focus_rate"],
            name=f"% {focus_name}",
            mode="lines",
            line=dict(color=detr_c, width=2),
        )
    )
    fig.add_trace(
        go.Bar(
            x=data["date"],
            y=data["incidents_shifted"],
            name=f"# incidencias (shift {lagd}d)",
            yaxis="y2",
            opacity=0.70,
            marker=dict(color=th.accent),
        )
    )
    fig.update_layout(
        xaxis_title="Día",
        yaxis=dict(title=f"% {focus_name}", tickformat=".0%"),
        yaxis2=dict(title="Incidencias (shifted)", overlaying="y", side="right"),
        legend=dict(orientation="h", x=0.0, y=1.08),
    )
    _apply_day_ticks(fig, list(data["date"].dt.normalize()), max_ticks=8)
    _layout_common(fig, th, height=360)
    return apply_plotly_template(fig, theme)


def chart_nps_timeseries_with_changepoints(
    ts: pd.Series,
    theme: Theme,
    points: list[pd.Timestamp],
    levels: list[str],
):
    """Line chart with changepoint markers colored by significance levels."""
    if ts is None or ts.empty:
        return None
    th = chart_theme(theme)
    import plotly.graph_objects as go

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=ts.index,
            y=ts.values,
            mode="lines+markers",
            name="NPS",
            line={"color": th.accent, "width": 2},
        )
    )

    detr_c, warn_c, pro_c = _status_colors(theme)
    neutral = th.grid

    def _cp_color(lvl: str) -> str:
        if str(lvl).lower().startswith("high"):
            return detr_c
        if str(lvl).lower().startswith("med"):
            return warn_c
        return neutral

    for p, lvl in zip(points, levels):
        fig.add_vline(
            x=p,
            line_width=2,
            line_dash="dot",
            line_color=_cp_color(lvl),
            opacity=0.9,
        )

    fig.update_layout(xaxis_title="Fecha", yaxis_title="NPS", showlegend=False)
    _layout_common(fig, th, height=320)
    return apply_plotly_template(fig, theme)


def chart_topic_bars(topics_df: pd.DataFrame, theme: Theme, top_k: int = 10):
    """Topic clusters by size."""
    if topics_df.empty:
        return None
    th = chart_theme(theme)
    import plotly.graph_objects as go  # lazy import for faster cold-start

    d = topics_df.sort_values(["n", "cluster_id"], ascending=[False, True]).head(top_k).copy()

    def _topic_label(row: pd.Series) -> str:
        terms = list(row["top_terms"])[:3]
        cid = int(row["cluster_id"])
        return f"#{cid}: {', '.join(terms)}"

    d["label"] = d.apply(_topic_label, axis=1)
    plot_df = d.iloc[::-1].copy()
    fig = go.Figure(
        data=[
            go.Bar(
                x=plot_df["n"],
                y=plot_df["label"],
                orientation="h",
                marker_color=th.grid,
                hovertemplate="%{y}<br>Volumen=%{x}<extra></extra>",
            )
        ]
    )
    fig.update_layout(xaxis_title="Volumen (n comentarios)", yaxis_title="", showlegend=False)
    fig.update_yaxes(categoryorder="array", categoryarray=plot_df["label"].tolist())
    _layout_common(fig, th, height=360)
    return apply_plotly_template(fig, theme)


def chart_driver_delta(delta_df: pd.DataFrame, theme: Theme, top_k: int = 12):
    """Bar chart: biggest deteriorations/improvements vs baseline by driver."""
    if delta_df.empty:
        return None
    th = chart_theme(theme)
    import plotly.express as px  # lazy import for faster cold-start

    d = (
        delta_df.copy()
        .sort_values(["delta_nps", "n_current", "n_baseline"], ascending=[False, False, False])
        .head(top_k)
        .copy()
    )
    plot_df = d.iloc[::-1].copy()
    fig = px.bar(
        plot_df,
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
    fig.update_traces(marker_color=_diverging_colors(theme, plot_df["delta_nps"]))
    fig.update_layout(xaxis_title="Delta NPS (actual - base)", yaxis_title="", showlegend=False)
    fig.update_yaxes(categoryorder="array", categoryarray=plot_df["value"].tolist())
    _layout_common(fig, th, height=360)
    return apply_plotly_template(fig, theme)


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
    pivot.index = [
        _compact_axis_label(value, width=16, max_lines=2, max_chars=30) for value in pivot.index
    ]
    pivot.columns = [
        _compact_axis_label(value, width=12, max_lines=2, max_chars=22) for value in pivot.columns
    ]
    fig = px.imshow(
        pivot,
        aspect="auto",
        labels=dict(x=col_dim, y=row_dim, color="NPS"),
        color_continuous_scale=_colorscale_rgy(theme),
        zmin=0,
        zmax=10,
    )
    fig.update_layout(
        coloraxis=dict(
            cmin=0.0,
            cmax=10.0,
            colorbar=dict(
                title="NPS",
                tickmode="array",
                tickvals=[0, 2, 6, 8, 10],
                len=0.78,
                y=0.5,
                thickness=14,
            ),
        )
    )
    fig.update_xaxes(tickangle=-28, side="bottom", automargin=True)
    fig.update_yaxes(automargin=True)
    _layout_common(fig, th, height=420)
    return apply_plotly_template(fig, theme)
