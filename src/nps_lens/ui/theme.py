from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

from nps_lens.design.tokens import DesignTokens, palette


@dataclass(frozen=True)
class Theme:
    mode: str  # "light" | "dark"
    bg: str
    surface: str
    surface_2: str
    text: str
    muted: str
    border: str
    accent: str
    on_accent: str
    danger: str
    warning: str
    success: str


def get_theme(mode: str) -> Theme:
    toks = DesignTokens.default()
    p = palette(toks, mode)

    # Map tokens to app-level semantics (no hardcoded colors outside tokens).
    bg = p["color.primary.bg.alternative.default"]
    surface = p.get("color.app.surface.default", bg)
    surface_2 = p.get("color.app.surface.raised", surface)

    text = p["color.primary.text.primary"]
    muted = p["color.primary.text.disabled"]
    border = p["color.primary.bg.bar"]
    accent = p["color.primary.accent.value-01.default"]
    on_accent = p.get("color.app.text.on-accent", p.get("color.primary.text.main-inverse.default", text))
    danger = p["color.primary.bg.alert"]
    warning = p["color.primary.bg.warning"]
    success = p["color.primary.bg.success"]
    return Theme(
        mode=mode,
        bg=bg,
        surface=surface,
        surface_2=surface_2,
        text=text,
        muted=muted,
        border=border,
        accent=accent,
        on_accent=on_accent,
        danger=danger,
        warning=warning,
        success=success,
    )


def apply_theme(t: Theme) -> None:
    """Central CSS, driven by tokens.

    Streamlit is not a full design-system host, but we can get very close by:
    - defining CSS variables
    - styling the main containers, headings, cards, and controls
    - keeping all styling in one place
    """

    st.markdown(
        f"""
<style>
:root {{
  --nps-bg: {t.bg};
  --nps-surface: {t.surface};
  --nps-surface-2: {t.surface_2};
  --nps-text: {t.text};
  --nps-muted: {t.muted};
  --nps-border: {t.border};
  --nps-accent: {t.accent};
  --nps-on-accent: {t.on_accent};
  --nps-shadow-color: color-mix(in srgb, #000 10%, transparent);
  --nps-border-softer: color-mix(in srgb, var(--nps-text) 18%, transparent);
  --nps-border-soft: color-mix(in srgb, var(--nps-text) 22%, transparent);
  --nps-border-strong: color-mix(in srgb, var(--nps-text) 32%, transparent);
  --nps-border-stronger: color-mix(in srgb, var(--nps-text) 42%, transparent);
  --nps-accent-soft: color-mix(in srgb, var(--nps-accent) 18%, transparent);
  --nps-accent-strong: color-mix(in srgb, var(--nps-accent) 45%, transparent);
  --nps-danger: {t.danger};
  --nps-warning: {t.warning};
  --nps-success: {t.success};
  --nps-radius: 18px;
  --nps-shadow: 0 10px 30px var(--nps-shadow-color);
}}

html, body, [data-testid="stAppViewContainer"] {{
  background: var(--nps-bg);
  color: var(--nps-text);
}}

/* Typography */
h1, h2, h3, h4, h5, h6 {{
  letter-spacing: -0.02em;
  color: var(--nps-text);
}}

/* Markdown containers sometimes get default colors; enforce tokens */
div[data-testid="stMarkdownContainer"],
div[data-testid="stMarkdownContainer"] * {{
  color: var(--nps-text);
}}

/* Tabs (BaseWeb) */
button[data-baseweb="tab"] {{
  color: var(--nps-muted) !important;
  background: transparent !important;
}}
button[data-baseweb="tab"][aria-selected="true"] {{
  color: var(--nps-text) !important;
  border-bottom: 2px solid var(--nps-accent) !important;
}}

/* Hide Streamlit default header chrome */
header[data-testid="stHeader"] {{
  background: transparent;
}}

/* Sidebar */
[data-testid="stSidebar"] {{
  background: var(--nps-surface);
  border-right: 1px solid var(--nps-border-soft);
}}

/* Cards */
.nps-card {{
  background: var(--nps-surface);
  border: 1px solid var(--nps-border-softer);
  border-radius: var(--nps-radius);
  padding: 16px 18px;
  box-shadow: var(--nps-shadow);
}}

.nps-card--flat {{
  box-shadow: none;
}}

.nps-muted {{
  color: var(--nps-muted);
}}

.nps-kpi {{
  font-size: 34px;
  font-weight: 800;
  line-height: 1.05;
}}

.nps-pill {{
  display: inline-block;
  padding: 6px 10px;
  border-radius: 999px;
  background: var(--nps-accent-soft);
  border: 1px solid var(--nps-accent-strong);
  color: var(--nps-text);
  font-size: 12px;
  font-weight: 600;
}}

/* Buttons */
div.stButton > button {{
  border-radius: 12px;
  border: 1px solid var(--nps-border-strong);
}}

div.stButton > button[kind="primary"] {{
  background: var(--nps-accent);
  color: var(--nps-on-accent);
  border: none;
}}

/* Secondary buttons (incl. download) */
div.stButton > button[kind="secondary"],
button[kind="secondary"] {{
  background: var(--nps-surface-2) !important;
  color: var(--nps-text) !important;
  border: 1px solid var(--nps-border-strong) !important;
}}

/* Dataframes */
[data-testid="stDataFrame"] {{
  border-radius: var(--nps-radius);
  overflow: hidden;
  border: 1px solid var(--nps-border-soft);
}}

/* Streamlit DataFrame uses AG-Grid under the hood */
div[data-testid="stDataFrame"] .ag-root-wrapper,
div[data-testid="stDataFrame"] .ag-root-wrapper-body,
div[data-testid="stDataFrame"] .ag-center-cols-viewport,
div[data-testid="stDataFrame"] .ag-body-viewport {{
  background: var(--nps-surface) !important;
}}
div[data-testid="stDataFrame"] .ag-header,
div[data-testid="stDataFrame"] .ag-header-row,
div[data-testid="stDataFrame"] .ag-header-cell {{
  background: var(--nps-surface-2) !important;
  color: var(--nps-text) !important;
  border-color: var(--nps-border-soft) !important;
}}
div[data-testid="stDataFrame"] .ag-row {{
  background: var(--nps-surface) !important;
  color: var(--nps-text) !important;
}}
div[data-testid="stDataFrame"] .ag-row:hover {{
  background: var(--nps-accent-soft) !important;
}}
div[data-testid="stDataFrame"] .ag-cell {{
  color: var(--nps-text) !important;
  border-color: var(--nps-border-softer) !important;
}}

/* Controls (BaseWeb) — keep contrast in dark mode */
div[data-baseweb="select"] > div {{
  background: var(--nps-surface-2) !important;
  color: var(--nps-text) !important;
  border-color: var(--nps-border) !important;
  box-shadow: none !important;
}}
div[data-baseweb="select"] span, div[data-baseweb="select"] input {{
  color: var(--nps-text) !important;
}}
div[data-baseweb="select"] input::placeholder {{
  color: var(--nps-muted) !important;
}}
div[data-baseweb="popover"] * {{
  color: var(--nps-text) !important;
}}
/* BaseWeb popover container (Streamlit renders menus in a portal) */
div[data-baseweb="popover"] {{
  background: var(--nps-surface-2) !important;
}}
/* Ensure all nested popover layers inherit the dark surface */
div[data-baseweb="popover"] div {{
  background: var(--nps-surface-2) !important;
}}
div[data-baseweb="popover"] > div {{
  background: var(--nps-surface-2) !important;
  border: 1px solid var(--nps-border-soft) !important;
  border-radius: 14px !important;
}}
div[data-baseweb="popover"] ul {{
  background: var(--nps-surface-2) !important;
}}
div[data-baseweb="popover"] [role="option"]:hover {{
  background: var(--nps-accent-soft) !important;
}}
ul[role="listbox"] {{
  background: var(--nps-surface-2) !important;
  border-radius: 14px;
  border: 1px solid var(--nps-border-soft) !important;
}}
div[data-baseweb="menu"], div[role="listbox"] {{
  background: var(--nps-surface-2) !important;
  border: 1px solid var(--nps-border-soft) !important;
}}
div[data-baseweb="menu"] *, div[role="listbox"] * {{
  color: var(--nps-text) !important;
}}
div[data-baseweb="menu"] [role="option"],
div[role="listbox"] [role="option"],
ul[role="listbox"] [role="option"] {{
  color: var(--nps-text) !important;
}}

ul[role="listbox"] li {{
  background: transparent !important;
}}
ul[role="listbox"] li:hover {{
  background: var(--nps-accent-soft) !important;
}}
ul[role="listbox"] li[aria-selected="true"] {{
  background: var(--nps-accent-soft) !important;
}}
div[data-testid="stRadio"] label, div[data-testid="stCheckbox"] label {{
  color: var(--nps-text) !important;
}}
div[data-testid="stMarkdownContainer"] a {{
  color: var(--nps-accent) !important;
}}

/* Sidebar container */
section[data-testid="stSidebar"] > div {{
  background: var(--nps-surface) !important;
}}

/* Inputs */
input, textarea {{
  color: var(--nps-text) !important;
  background: var(--nps-surface-2) !important;
  border: 1px solid var(--nps-border-strong) !important;
  border-radius: 12px !important;
}}

/* Some Streamlit widgets set inline styles; override aggressively */
textarea[style], input[style] {{
  background: var(--nps-surface-2) !important;
  color: var(--nps-text) !important;
}}

/* Text areas in Streamlit (executive report, prompts, etc.) */
div[data-testid="stTextArea"] textarea {{
  background: var(--nps-surface-2) !important;
  color: var(--nps-text) !important;
}}

div[data-testid="stTextArea"] > div > textarea {{
  background: var(--nps-surface-2) !important;
  color: var(--nps-text) !important;
}}

div[data-testid="stTextArea"] textarea::placeholder {{
  color: var(--nps-muted) !important;
}}


/* File uploader */
div[data-testid="stFileUploaderDropzone"] {{
  background: var(--nps-surface-2) !important;
  border: 1px dashed var(--nps-border-stronger) !important;
  border-radius: var(--nps-radius) !important;
}}
div[data-testid="stFileUploaderDropzone"] * {{
  color: var(--nps-text) !important;
}}
div[data-testid="stFileUploader"] small {{
  color: var(--nps-muted) !important;
}}

/* Plotly: force dark surfaces + readable text in dark mode */
div[data-testid="stPlotlyChart"] {{
  background: var(--nps-surface) !important;
  border-radius: var(--nps-radius);
  border: 1px solid var(--nps-border-soft);
}}
div[data-testid="stPlotlyChart"] .js-plotly-plot .plotly .main-svg .bg {{
  fill: var(--nps-bg) !important;
}}
div[data-testid="stPlotlyChart"] .js-plotly-plot .plotly text {{
  fill: var(--nps-text) !important;
}}



/* Tabs (BaseWeb) */
.stTabs [data-baseweb="tab"] {{
  color: var(--nps-text-muted) !important;
}}
.stTabs [data-baseweb="tab"][aria-selected="true"] {{
  color: var(--nps-text) !important;
}}
.stTabs [data-baseweb="tab-highlight"] {{
  background: var(--nps-accent) !important;
}}

/* Plotly container background (extra safety for embedded mode) */
.js-plotly-plot .plotly {{
  background: var(--nps-surface) !important;
}}

</style>
""",
        unsafe_allow_html=True,
    )