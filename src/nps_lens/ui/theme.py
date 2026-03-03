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
    danger: str
    warning: str
    success: str


def get_theme(mode: str) -> Theme:
    toks = DesignTokens.default()
    p = palette(toks, mode)

    # Map token subset to app-level semantics.
    bg = p["color.primary.bg.alternative.default"]
    surface = p["color.primary.bg.action.active"] if mode == "dark" else "#f7f8fa"
    surface_2 = p["color.primary.bg.action.default"]
    text = p["color.primary.text.primary"]
    muted = p["color.primary.text.disabled"]
    border = p["color.primary.bg.bar"]
    accent = p["color.primary.accent.value-01.default"]
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
  --nps-danger: {t.danger};
  --nps-warning: {t.warning};
  --nps-success: {t.success};
  --nps-radius: 18px;
  --nps-shadow: 0 10px 30px rgba(0,0,0,0.10);
}}

html, body, [data-testid="stAppViewContainer"] {{
  background: var(--nps-bg);
  color: var(--nps-text);
}}

/* Typography */
h1, h2, h3, h4 {{
  letter-spacing: -0.02em;
}}

/* Hide Streamlit default header chrome */
header[data-testid="stHeader"] {{
  background: transparent;
}}

/* Sidebar */
[data-testid="stSidebar"] {{
  background: var(--nps-surface);
  border-right: 1px solid rgba(127,127,127,0.25);
}}

/* Cards */
.nps-card {{
  background: var(--nps-surface);
  border: 1px solid rgba(127,127,127,0.22);
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
  background: rgba(133,200,255,0.18);
  border: 1px solid rgba(133,200,255,0.45);
  color: var(--nps-text);
  font-size: 12px;
  font-weight: 600;
}}

/* Buttons */
div.stButton > button {{
  border-radius: 12px;
  border: 1px solid rgba(127,127,127,0.35);
}}

div.stButton > button[kind="primary"] {{
  background: var(--nps-accent);
  color: #070e46;
  border: none;
}}

/* Dataframes */
[data-testid="stDataFrame"] {{
  border-radius: var(--nps-radius);
  overflow: hidden;
  border: 1px solid rgba(127,127,127,0.25);
}}

/* Controls (BaseWeb) — keep contrast in dark mode */
div[data-baseweb="select"] > div {{
  background: var(--nps-surface-2) !important;
  color: var(--nps-text) !important;
  border-color: var(--nps-border) !important;
}}
div[data-baseweb="select"] span, div[data-baseweb="select"] input {{
  color: var(--nps-text) !important;
}}
div[data-baseweb="popover"] * {{
  color: var(--nps-text) !important;
}}
ul[role="listbox"] {{
  background: var(--nps-surface) !important;
}}
ul[role="listbox"] li {{
  background: transparent !important;
}}
ul[role="listbox"] li[aria-selected="true"] {{
  background: rgba(255,255,255,0.06) !important;
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
}}

</style>
""",
        unsafe_allow_html=True,
    )
