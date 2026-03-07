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
    on_accent = p.get(
        "color.app.text.on-accent", p.get("color.primary.text.main-inverse.default", text)
    )
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

.nps-hero {{
  background:
    radial-gradient(circle at top right, color-mix(in srgb, var(--nps-accent) 24%, transparent), transparent 42%),
    linear-gradient(160deg, color-mix(in srgb, var(--nps-accent) 10%, var(--nps-surface)) 0%, var(--nps-surface) 68%);
  border: 1px solid var(--nps-border-soft);
  border-radius: 26px;
  padding: 22px;
  margin: 10px 0 18px 0;
  box-shadow: var(--nps-shadow);
}}

.nps-hero-kicker {{
  font-size: 11px;
  font-weight: 800;
  letter-spacing: .14em;
  text-transform: uppercase;
  color: var(--nps-muted);
}}

.nps-hero h3 {{
  margin: 8px 0 10px 0;
  font-size: 30px;
  line-height: 1.0;
}}

.nps-hero p {{
  margin: 0;
  max-width: 900px;
  color: var(--nps-text);
  line-height: 1.5;
}}

.nps-hero-metrics {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 10px;
  margin-top: 18px;
}}

.nps-hero-metric {{
  padding: 14px 16px;
  border-radius: 16px;
  background: color-mix(in srgb, var(--nps-surface-2) 88%, var(--nps-accent) 12%);
  border: 1px solid var(--nps-border-softer);
}}

.nps-hero-metric span {{
  display: block;
  font-size: 11px;
  letter-spacing: .08em;
  text-transform: uppercase;
  color: var(--nps-muted);
  margin-bottom: 6px;
}}

.nps-hero-metric strong {{
  font-size: 24px;
  line-height: 1;
}}

.nps-impact-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 14px;
  margin: 10px 0 18px 0;
}}

.nps-impact-card {{
  background:
    linear-gradient(150deg, color-mix(in srgb, var(--nps-accent) 12%, var(--nps-surface)) 0%, var(--nps-surface) 58%),
    var(--nps-surface);
  border: 1px solid var(--nps-border-soft);
  border-radius: 22px;
  padding: 18px;
  box-shadow: var(--nps-shadow);
}}

.nps-impact-card h4 {{
  margin: 10px 0 12px 0;
  font-size: 20px;
  line-height: 1.1;
}}

.nps-impact-head {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}}

.nps-impact-rank {{
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 38px;
  height: 38px;
  border-radius: 999px;
  background: var(--nps-accent);
  color: var(--nps-on-accent);
  font-weight: 800;
}}

.nps-impact-kicker {{
  font-size: 11px;
  font-weight: 700;
  letter-spacing: .12em;
  text-transform: uppercase;
  color: var(--nps-muted);
}}

.nps-impact-flow {{
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  margin: 0 0 14px 0;
}}

.nps-impact-step {{
  padding: 6px 10px;
  border-radius: 999px;
  background: var(--nps-accent-soft);
  border: 1px solid var(--nps-border-strong);
  font-size: 12px;
  font-weight: 600;
}}

.nps-impact-arrow {{
  color: var(--nps-muted);
  font-weight: 800;
}}

.nps-impact-metrics {{
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
  margin-bottom: 12px;
}}

.nps-impact-metrics span {{
  display: block;
  padding: 10px 12px;
  border-radius: 14px;
  background: color-mix(in srgb, var(--nps-surface-2) 82%, var(--nps-accent) 18%);
  border: 1px solid var(--nps-border-softer);
  font-size: 12px;
}}

.nps-impact-card p {{
  margin: 0;
  color: var(--nps-text);
  line-height: 1.45;
}}

.nps-spotlight {{
  background:
    radial-gradient(circle at top right, color-mix(in srgb, var(--nps-accent) 22%, transparent), transparent 40%),
    linear-gradient(160deg, color-mix(in srgb, var(--nps-accent) 10%, var(--nps-surface)) 0%, var(--nps-surface) 62%);
  border: 1px solid var(--nps-border-soft);
  border-radius: 28px;
  padding: 24px;
  box-shadow: var(--nps-shadow);
  margin-bottom: 12px;
}}

.nps-spotlight-head {{
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 18px;
}}

.nps-spotlight-kicker {{
  font-size: 11px;
  font-weight: 800;
  letter-spacing: .14em;
  text-transform: uppercase;
  color: var(--nps-muted);
}}

.nps-spotlight h3 {{
  margin: 8px 0 10px 0;
  font-size: 32px;
  line-height: 1.0;
}}

.nps-spotlight p {{
  margin: 0;
  line-height: 1.6;
  max-width: 920px;
}}

.nps-spotlight-rank {{
  min-width: 56px;
  height: 56px;
  border-radius: 999px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  background: var(--nps-accent);
  color: var(--nps-on-accent);
  font-weight: 800;
  font-size: 22px;
}}

.nps-spotlight-flow {{
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  margin: 18px 0 16px 0;
}}

.nps-spotlight-metrics {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 10px;
  margin-bottom: 18px;
}}

.nps-spotlight-metric {{
  padding: 12px 14px;
  border-radius: 16px;
  background: color-mix(in srgb, var(--nps-surface-2) 86%, var(--nps-accent) 14%);
  border: 1px solid var(--nps-border-softer);
}}

.nps-spotlight-metric span {{
  display: block;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: .08em;
  color: var(--nps-muted);
  margin-bottom: 6px;
}}

.nps-spotlight-metric strong {{
  font-size: 23px;
  line-height: 1;
}}

.nps-spotlight-evidence {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 16px;
}}

.nps-impact-evidence {{
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid var(--nps-border-softer);
}}

.nps-impact-label {{
  font-size: 11px;
  font-weight: 800;
  letter-spacing: .10em;
  text-transform: uppercase;
  color: var(--nps-muted);
  margin-bottom: 6px;
}}

.nps-impact-evidence ul {{
  margin: 0;
  padding-left: 18px;
}}

.nps-impact-evidence li {{
  margin: 0 0 6px 0;
  line-height: 1.35;
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
  background: var(--nps-surface) !important;
  --gdg-bg-cell: var(--nps-surface);
  --gdg-bg-header: var(--nps-surface-2);
  --gdg-bg-header-has-focus: var(--nps-surface-2);
  --gdg-border-color: var(--nps-border-soft);
  --gdg-text-dark: var(--nps-text);
  --gdg-text-medium: var(--nps-muted);
  --gdg-text-light: var(--nps-muted);
  --gdg-accent-color: var(--nps-accent);
  --gdg-accent-fg: var(--nps-on-accent);
  --gdg-bg-bubble: color-mix(in srgb, var(--nps-accent) 20%, var(--nps-surface-2));
  --gdg-font-family: inherit;
}}

/* Streamlit DataFrame uses AG-Grid under the hood */
div[data-testid="stDataFrame"] .ag-root-wrapper,
div[data-testid="stDataFrame"] .ag-root-wrapper-body,
div[data-testid="stDataFrame"] .ag-center-cols-viewport,
div[data-testid="stDataFrame"] .ag-body-viewport,
div[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"],
div[data-testid="stDataFrame"] > div,
div[data-testid="stDataFrame"] canvas,
div[data-testid="stTable"] > div,
div[data-testid="stTable"] table {{
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
div[data-testid="stDataFrame"] [role="grid"],
div[data-testid="stDataFrame"] [role="row"],
div[data-testid="stDataFrame"] [role="columnheader"],
div[data-testid="stDataFrame"] [role="gridcell"],
div[data-testid="stDataFrame"] .gdg-wmyidgi,
div[data-testid="stDataFrame"] .gdg-s1dgczr6,
div[data-testid="stDataFrame"] .gdg-seveqep,
div[data-testid="stDataFrame"] .gdg-d19meir1 {{
  background: var(--nps-surface) !important;
  color: var(--nps-text) !important;
}}
div[data-testid="stTable"] table th,
div[data-testid="stTable"] table td {{
  background: var(--nps-surface) !important;
  color: var(--nps-text) !important;
  border-color: var(--nps-border-softer) !important;
}}
div[data-testid="stTable"] table thead th {{
  background: var(--nps-surface-2) !important;
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
  color: var(--nps-muted) !important;
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
