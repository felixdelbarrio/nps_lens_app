from __future__ import annotations

import contextlib
from dataclasses import dataclass

import pandas as pd

from nps_lens.design.tokens import DesignTokens, palette, primary_accent
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

    tokens = DesignTokens.default()
    p = palette(tokens, theme.mode)
    return ChartTheme(
        paper_bg=p["color.app.surface.default"],
        plot_bg=p["color.app.surface.raised"],
        text=p["color.primary.text.primary"],
        grid=p["color.primary.bg.bar"],
        accent=primary_accent(tokens, theme.mode),
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
    detr_c, pas_c, pro_c = _status_colors(theme)
    return [[0.0, detr_c], [0.5, pas_c], [1.0, pro_c]]


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
    _, _, pro_c = _status_colors(theme)
    fig.update_traces(line_color=pro_c, marker_color=pro_c)
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


def _apply_day_ticks(fig, days: list[pd.Timestamp], *, max_ticks: int = 21) -> None:
    """Apply business-friendly day ticks: weekday letter + date.

    We avoid clutter by subsampling if there are too many days.
    """

    if not days:
        return

    step = max(1, int(len(days) / max_ticks))
    tick_days = days[::step]
    ticktext = [f"{_weekday_letter_es(d)}<br>{d.strftime('%b %d')}" for d in tick_days]
    fig.update_xaxes(tickmode="array", tickvals=tick_days, ticktext=ticktext)


def chart_daily_score_semaforo(
    df: pd.DataFrame,
    theme: Theme,
    *,
    days: int = 60,
    date_col: str = "Fecha",
    score_col: str = "NPS",
):
    """Daily distribution as a traffic-light heatmap (business-first).

    Instead of an 11-row ladder, we aggregate scores into the standard NPS groups:
    - Detractores (0-6)
    - Pasivos (7-8)
    - Promotores (9-10)

    This makes it immediately clear what is "bad" vs "good" while retaining
    intensity (count) per day.
    """

    if date_col not in df.columns or score_col not in df.columns:
        return None

    tmp = df.dropna(subset=[date_col, score_col]).copy()
    if tmp.empty:
        return None

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

    scores = pd.to_numeric(tmp[score_col], errors="coerce")
    tmp["score"] = scores.clip(lower=0, upper=10)
    tmp = tmp.dropna(subset=["score"]).copy()
    if tmp.empty:
        return None

    def _grp(v: float) -> str:
        if v <= 6:
            return "Detractores (0-6)"
        if v <= 8:
            return "Pasivos (7-8)"
        return "Promotores (9-10)"

    tmp["grp"] = tmp["score"].map(_grp)
    agg = tmp.groupby(["day", "grp"], as_index=False).agg(n=("score", "size"))
    pivot = agg.pivot(index="grp", columns="day", values="n").fillna(0.0)

    order = ["Detractores (0-6)", "Pasivos (7-8)", "Promotores (9-10)"]
    for g in order:
        if g not in pivot.index:
            pivot.loc[g] = 0.0
    pivot = pivot.loc[order]

    th = chart_theme(theme)
    detr_c, pas_c, pro_c = _status_colors(theme)

    import plotly.graph_objects as go

    days_list = [pd.Timestamp(d) for d in pivot.columns.to_list()]
    zmax = float(pivot.to_numpy().max()) if pivot.to_numpy().size else 1.0

    fig = go.Figure()
    for grp, color in [
        ("Detractores (0-6)", detr_c),
        ("Pasivos (7-8)", pas_c),
        ("Promotores (9-10)", pro_c),
    ]:
        z = [pivot.loc[grp].to_list()]
        fig.add_trace(
            go.Heatmap(
                x=days_list,
                y=[grp],
                z=z,
                zmin=0,
                zmax=zmax,
                colorscale=[
                    [0.0, _shade(color, toward=th.paper_bg, t=0.85)],
                    [0.35, _shade(color, toward=th.paper_bg, t=0.55)],
                    [1.0, color],
                ],
                showscale=False,
                hovertemplate=("Día=%{x|%Y-%m-%d}<br>" + grp + "=%{z}<extra></extra>"),
            )
        )

    fig.update_layout(xaxis_title="Día", yaxis_title="", showlegend=False)
    _layout_common(fig, th, height=220)
    _apply_day_ticks(fig, days_list, max_ticks=28)
    return apply_plotly_template(fig, theme)


def chart_daily_kpis(df: pd.DataFrame, theme: Theme, *, days: int = 60):
    """Daily time-series for detractor rate and classic NPS.

    Complements the ladder heatmap: the heatmap shows distribution, this chart shows
    % detractors and classic NPS (promoters - detractors) per day.
    """
    if "Fecha" not in df.columns or "NPS" not in df.columns:
        return None

    tmp = df.dropna(subset=["Fecha", "NPS"]).copy()
    if tmp.empty:
        return None

    tmp["day"] = tmp["Fecha"].dt.floor("D")
    end = tmp["day"].max()
    start = end - pd.Timedelta(days=int(days) - 1)
    tmp = tmp.loc[tmp["day"] >= start].copy()
    if tmp.empty:
        return None

    scores = pd.to_numeric(tmp["NPS"], errors="coerce")
    tmp["score"] = scores.clip(lower=0, upper=10)
    tmp = tmp.dropna(subset=["score"]).copy()
    if tmp.empty:
        return None

    tmp["is_prom"] = tmp["score"] >= 9
    tmp["is_det"] = tmp["score"] <= 6

    agg = (
        tmp.groupby("day", as_index=False)
        .agg(
            n=("score", "size"),
            prom=("is_prom", "mean"),
            det=("is_det", "mean"),
        )
        .sort_values("day")
    )
    agg["classic_nps"] = (agg["prom"] - agg["det"]) * 100.0
    agg["det_pct"] = agg["det"] * 100.0

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

    fig.update_layout(showlegend=True)
    fig.update_xaxes(title_text="Día")
    fig.update_yaxes(title_text="NPS clásico (pp)", secondary_y=False)
    fig.update_yaxes(title_text="% detractores", secondary_y=True)

    _layout_common(fig, th, height=300)
    _apply_day_ticks(fig, [pd.Timestamp(d) for d in agg["day"].tolist()], max_ticks=21)
    return apply_plotly_template(fig, theme)


def chart_daily_mix_business(df: pd.DataFrame, theme: Theme, *, days: int = 60):
    """Business-friendly daily view: promoters/passives/detractors mix.

    This is intentionally easier to interpret than the heatmap ladder:
    - Detractors (0-6) penalize NPS
    - Passives (7-8) are neutral
    - Promoters (9-10) are favorable

    Output: 100% stacked bars by day.
    """

    if "Fecha" not in df.columns or "NPS" not in df.columns:
        return None

    tmp = df.dropna(subset=["Fecha", "NPS"]).copy()
    if tmp.empty:
        return None

    tmp["day"] = tmp["Fecha"].dt.floor("D")
    end = tmp["day"].max()
    start = end - pd.Timedelta(days=int(days) - 1)
    tmp = tmp.loc[tmp["day"] >= start].copy()
    if tmp.empty:
        return None

    scores = pd.to_numeric(tmp["NPS"], errors="coerce")
    tmp["score"] = scores.clip(lower=0, upper=10)
    tmp = tmp.dropna(subset=["score"]).copy()
    if tmp.empty:
        return None

    tmp["promoters"] = tmp["score"] >= 9
    tmp["passives"] = (tmp["score"] >= 7) & (tmp["score"] <= 8)
    tmp["detractors"] = tmp["score"] <= 6

    agg = (
        tmp.groupby("day", as_index=False)
        .agg(
            n=("score", "size"),
            promoters=("promoters", "mean"),
            passives=("passives", "mean"),
            detractors=("detractors", "mean"),
        )
        .sort_values("day")
    )
    # Convert to percentages.
    for col in ("promoters", "passives", "detractors"):
        agg[col] = agg[col] * 100.0

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

    fig.update_layout(
        barmode="stack",
        yaxis_title="Mix diario (% de respuestas)",
        xaxis_title="Día",
        legend_title_text="Cómo leerlo",
    )
    _layout_common(fig, th, height=320)
    _apply_day_ticks(fig, [pd.Timestamp(d) for d in agg["day"].tolist()], max_ticks=21)
    fig.update_yaxes(range=[0, 100])
    return apply_plotly_template(fig, theme)


def chart_daily_volume(df: pd.DataFrame, theme: Theme, *, days: int = 60):
    """Daily response volume (n). Useful to avoid over-reading low-N days."""
    if "Fecha" not in df.columns or "NPS" not in df.columns:
        return None
    tmp = df.dropna(subset=["Fecha", "NPS"]).copy()
    if tmp.empty:
        return None
    tmp["day"] = tmp["Fecha"].dt.floor("D")
    end = tmp["day"].max()
    start = end - pd.Timedelta(days=int(days) - 1)
    tmp = tmp.loc[tmp["day"] >= start].copy()
    if tmp.empty:
        return None
    agg = tmp.groupby("day", as_index=False).agg(n=("NPS", "size")).sort_values("day")

    th = chart_theme(theme)
    import plotly.express as px  # lazy import

    fig = px.bar(agg, x="day", y="n", hover_data={"n": True})
    fig.update_traces(marker_color=th.grid)
    fig.update_layout(xaxis_title="Día", yaxis_title="Respuestas (n)", showlegend=False)
    _layout_common(fig, th, height=220)
    _apply_day_ticks(fig, [pd.Timestamp(d) for d in agg["day"].tolist()], max_ticks=21)
    return apply_plotly_template(fig, theme)


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
    fig.update_traces(marker_color=_diverging_colors(theme, d["gap_vs_overall"]))
    fig.update_layout(
        xaxis_title="Diferencia vs NPS global (puntos)",
        yaxis_title="",
        showlegend=False,
    )
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
    import plotly.express as px  # lazy import for faster cold-start

    d = topics_df.sort_values("n", ascending=False).head(top_k).copy()

    def _topic_label(row: pd.Series) -> str:
        terms = list(row["top_terms"])[:3]
        cid = int(row["cluster_id"])
        return f"#{cid}: {', '.join(terms)}"

    d["label"] = d.apply(_topic_label, axis=1)
    fig = px.bar(d, x="n", y="label", orientation="h")
    fig.update_traces(marker_color=th.grid)
    fig.update_layout(xaxis_title="Volumen (n comentarios)", yaxis_title="", showlegend=False)
    _layout_common(fig, th, height=360)
    return apply_plotly_template(fig, theme)


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
    fig.update_traces(marker_color=_diverging_colors(theme, d["delta_nps"]))
    fig.update_layout(xaxis_title="Delta NPS (actual - base)", yaxis_title="", showlegend=False)
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
    fig = px.imshow(
        pivot,
        aspect="auto",
        labels=dict(x=col_dim, y=row_dim, color="NPS"),
        color_continuous_scale=_colorscale_rgy(theme),
        zmin=0,
        zmax=10,
    )
    fig.update_layout(coloraxis_colorbar=dict(title="NPS"))
    _layout_common(fig, th, height=420)
    return apply_plotly_template(fig, theme)
