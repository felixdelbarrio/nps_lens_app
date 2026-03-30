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
    chart_paper: str
    chart_plot: str
    chart_grid: str
    chart_zero_line: str
    table_bg: str
    table_bg_alt: str
    table_bg_hover: str
    table_header_bg: str
    table_header_text: str
    table_border: str
    control_bg: str
    control_bg_hover: str
    control_border: str
    control_text: str
    control_placeholder: str
    control_icon: str
    control_menu_bg: str
    control_menu_item_hover: str
    control_menu_item_selected: str
    accent: str
    on_accent: str
    brand: str
    on_brand: str
    danger: str
    danger_soft: str
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
    chart_paper = p.get("color.app.chart.paper", surface)
    chart_plot = p.get("color.app.chart.plot", surface_2)
    chart_grid = p.get("color.app.chart.grid", border)
    chart_zero_line = p.get("color.app.chart.zero-line", chart_grid)
    table_bg = p.get("color.app.table.bg", surface)
    table_bg_alt = p.get("color.app.table.bg.alt", surface_2)
    table_bg_hover = p.get("color.app.table.bg.hover", table_bg_alt)
    table_header_bg = p.get("color.app.table.header.bg", surface_2)
    table_header_text = p.get("color.app.table.header.text", muted)
    table_border = p.get("color.app.table.border", border)
    accent = p["color.primary.accent.value-01.default"]
    control_bg = p.get("color.app.control.bg", surface_2)
    control_bg_hover = p.get("color.app.control.bg.hover", control_bg)
    control_border = p.get("color.app.control.border", border)
    control_text = p.get("color.app.control.text", text)
    control_placeholder = p.get("color.app.control.placeholder", muted)
    control_icon = p.get(
        "color.app.control.icon", p.get("color.primary.text.action.default", accent)
    )
    control_menu_bg = p.get("color.app.control.menu.bg", surface_2)
    control_menu_item_hover = p.get("color.app.control.menu.item.hover", control_bg_hover)
    control_menu_item_selected = p.get(
        "color.app.control.menu.item.selected", control_menu_item_hover
    )
    on_accent = p.get(
        "color.app.text.on-accent", p.get("color.primary.text.main-inverse.default", text)
    )
    brand = p["color.primary.bg.action.default"]
    on_brand = p.get("color.primary.text.main-inverse.default", text)
    danger = p["color.primary.bg.alert"]
    danger_soft = p["color.primary.accent.value-07.default"]
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
        chart_paper=chart_paper,
        chart_plot=chart_plot,
        chart_grid=chart_grid,
        chart_zero_line=chart_zero_line,
        table_bg=table_bg,
        table_bg_alt=table_bg_alt,
        table_bg_hover=table_bg_hover,
        table_header_bg=table_header_bg,
        table_header_text=table_header_text,
        table_border=table_border,
        control_bg=control_bg,
        control_bg_hover=control_bg_hover,
        control_border=control_border,
        control_text=control_text,
        control_placeholder=control_placeholder,
        control_icon=control_icon,
        control_menu_bg=control_menu_bg,
        control_menu_item_hover=control_menu_item_hover,
        control_menu_item_selected=control_menu_item_selected,
        accent=accent,
        on_accent=on_accent,
        brand=brand,
        on_brand=on_brand,
        danger=danger,
        danger_soft=danger_soft,
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
  --nps-chart-paper: {t.chart_paper};
  --nps-chart-plot: {t.chart_plot};
  --nps-chart-grid: {t.chart_grid};
  --nps-chart-zero: {t.chart_zero_line};
  --nps-table-bg: {t.table_bg};
  --nps-table-bg-alt: {t.table_bg_alt};
  --nps-table-bg-hover: {t.table_bg_hover};
  --nps-table-header-bg: {t.table_header_bg};
  --nps-table-header-text: {t.table_header_text};
  --nps-table-border: {t.table_border};
  --nps-control-bg: {t.control_bg};
  --nps-control-bg-hover: {t.control_bg_hover};
  --nps-control-border: {t.control_border};
  --nps-control-text: {t.control_text};
  --nps-control-placeholder: {t.control_placeholder};
  --nps-control-icon: {t.control_icon};
  --nps-control-menu-bg: {t.control_menu_bg};
  --nps-control-menu-item-hover: {t.control_menu_item_hover};
  --nps-control-menu-item-selected: {t.control_menu_item_selected};
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
  --nps-danger-soft: {t.danger_soft};
  --nps-warning: {t.warning};
  --nps-success: {t.success};
  --nps-radius: 18px;
  --nps-shadow: 0 10px 30px var(--nps-shadow-color);
  --nps-font-display: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", serif;
  --nps-font-ui: "Benton Sans", "Avenir Next", "Segoe UI", sans-serif;
}}

html, body, [data-testid="stAppViewContainer"] {{
  background: var(--nps-bg);
  color: var(--nps-text);
  font-family: var(--nps-font-ui);
}}

.block-container {{
  padding-top: 0.65rem !important;
  padding-bottom: 2rem !important;
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

/* Hide Streamlit default chrome */
header[data-testid="stHeader"] {{
  background: transparent;
  height: 0;
}}

[data-testid="stToolbar"],
[data-testid="stDecoration"],
#MainMenu,
[data-testid="stStatusWidget"],
.stAppDeployButton,
button[title="Deploy"],
button[aria-label="Deploy"] {{
  display: none !important;
  visibility: hidden !important;
}}

/* Sidebar */
[data-testid="stSidebar"] {{
  background: var(--nps-surface);
  border-right: 1px solid var(--nps-border-soft);
}}

section[data-testid="stSidebar"] > div {{
  background: var(--nps-surface) !important;
  padding-top: 0.5rem !important;
}}

/* Sidebar collapse/expand control: keep always visible and readable */
[data-testid="stSidebarCollapseButton"],
[data-testid="stSidebarCollapsedControl"],
[data-testid="collapsedControl"] {{
  opacity: 1 !important;
  visibility: visible !important;
  z-index: 1000 !important;
}}

[data-testid="stSidebarCollapseButton"] button,
[data-testid="stSidebarCollapsedControl"] button,
[data-testid="collapsedControl"] button,
button[aria-label="Open sidebar"],
button[aria-label="Close sidebar"] {{
  display: inline-flex !important;
  align-items: center !important;
  justify-content: center !important;
  width: 32px !important;
  height: 32px !important;
  min-width: 32px !important;
  padding: 0 !important;
  border-radius: 8px !important;
  border: 1px solid var(--nps-control-border) !important;
  background: var(--nps-control-bg) !important;
  color: var(--nps-control-icon) !important;
  box-shadow: none !important;
  opacity: 1 !important;
  position: relative !important;
}}

[data-testid="stSidebarCollapseButton"] button:hover,
[data-testid="stSidebarCollapsedControl"] button:hover,
[data-testid="collapsedControl"] button:hover,
button[aria-label="Open sidebar"]:hover,
button[aria-label="Close sidebar"]:hover,
[data-testid="stSidebarCollapseButton"] button:focus-visible,
[data-testid="stSidebarCollapsedControl"] button:focus-visible,
[data-testid="collapsedControl"] button:focus-visible,
button[aria-label="Open sidebar"]:focus-visible,
button[aria-label="Close sidebar"]:focus-visible {{
  background: var(--nps-control-bg-hover) !important;
  color: var(--nps-control-text) !important;
  border-color: var(--nps-control-border) !important;
}}

[data-testid="stSidebarCollapseButton"] button svg,
[data-testid="stSidebarCollapsedControl"] button svg,
[data-testid="collapsedControl"] button svg,
button[aria-label="Open sidebar"] svg,
button[aria-label="Close sidebar"] svg {{
  display: none !important;
  width: 0 !important;
  height: 0 !important;
  opacity: 0 !important;
}}

/* Force deterministic ASCII arrows for sidebar toggle in every Streamlit state. */
[data-testid="stSidebarCollapseButton"] button::before,
button[aria-label="Close sidebar"]::before {{
  content: "<";
  display: block;
  color: var(--nps-control-icon) !important;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 18px;
  font-weight: 700;
  line-height: 1;
}}

[data-testid="stSidebarCollapsedControl"] button::before,
[data-testid="collapsedControl"] button::before,
button[aria-label="Open sidebar"]::before {{
  content: ">";
  display: block;
  color: var(--nps-control-icon) !important;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 18px;
  font-weight: 700;
  line-height: 1;
}}

[data-testid="stSidebarCollapseButton"] button svg path,
[data-testid="stSidebarCollapsedControl"] button svg path,
[data-testid="collapsedControl"] button svg path,
button[aria-label="Open sidebar"] svg path,
button[aria-label="Close sidebar"] svg path {{
  fill: currentColor !important;
  stroke: currentColor !important;
  opacity: 1 !important;
}}

/* Cards */
.nps-card {{
  background: var(--nps-surface);
  border: 1px solid var(--nps-border-softer);
  border-radius: var(--nps-radius);
  padding: 16px 18px;
  box-shadow: var(--nps-shadow);
}}

.nps-card__kicker {{
  font-size: 12px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: .08em;
}}

.nps-card__spacer {{
  height: 10px;
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

.nps-section {{
  margin: 10px 0 12px 0;
}}

.nps-section__title {{
  font-size: 22px;
  font-weight: 800;
}}

.nps-section__subtitle {{
  margin-top: 4px;
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

.nps-pill--detractor {{
  background: color-mix(in srgb, var(--nps-danger-soft) 18%, var(--nps-surface));
  border-color: color-mix(in srgb, var(--nps-danger-soft) 54%, var(--nps-surface));
}}

.nps-pill--passive {{
  background: color-mix(in srgb, var(--nps-warning) 22%, var(--nps-surface));
  border-color: color-mix(in srgb, var(--nps-warning) 62%, var(--nps-surface));
}}

.nps-pill--promoter {{
  background: color-mix(in srgb, var(--nps-success) 18%, var(--nps-surface));
  border-color: color-mix(in srgb, var(--nps-success) 52%, var(--nps-surface));
}}

.nps-pill-row {{
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin: 18px 0 26px 0;
}}

.nps-pill-row--compact {{
  margin: 10px 0 20px 0;
}}

.nps-copy-widget {{
  display: flex;
  gap: 12px;
  align-items: center;
}}

.nps-copy-widget__btn {{
  width: 100%;
  padding: 10px 14px;
  border-radius: 12px;
  border: 1px solid transparent;
  cursor: pointer;
  font-weight: 650;
  background: var(--nps-accent);
  color: var(--nps-on-accent);
}}

.nps-copy-widget__btn:hover,
.nps-copy-widget__btn:focus-visible {{
  background: color-mix(in srgb, var(--nps-accent) 84%, black 16%);
  outline: none;
}}

.nps-copy-widget__msg {{
  font-size: 12px;
  color: var(--nps-muted);
}}

/* Context row: keep context pills and Reporte action aligned on the same baseline. */
div[data-testid="stHorizontalBlock"]:has(.nps-pill-row--compact) {{
  align-items: center;
}}

div[data-testid="stHorizontalBlock"]:has(.nps-pill-row--compact) > div:last-child {{
  display: flex;
  justify-content: flex-end;
}}

div[data-testid="stHorizontalBlock"]:has(.nps-pill-row--compact) > div:last-child div.stButton {{
  width: 100%;
  margin-top: 10px;
}}

.nps-app-hero {{
  background:
    radial-gradient(circle at top right, color-mix(in srgb, #85c8ff 24%, transparent), transparent 30%),
    linear-gradient(145deg, #001391 0%, #0b2ab8 100%);
  border: 1px solid color-mix(in srgb, {t.brand} 72%, white 12%);
  border-radius: 28px;
  padding: 22px 28px 24px 28px;
  margin: 2px 0 0 0;
  box-shadow: none;
}}

.nps-app-hero__title {{
  margin: 0;
  color: #ffffff !important;
  font-family: var(--nps-font-display);
  font-size: clamp(32px, 3.4vw, 48px);
  line-height: .96;
  letter-spacing: -0.03em;
  font-weight: 700;
}}

.nps-app-hero h1,
.nps-app-hero .nps-app-hero__title,
div[data-testid="stMarkdownContainer"] .nps-app-hero h1,
div[data-testid="stMarkdownContainer"] .nps-app-hero .nps-app-hero__title {{
  color: #ffffff !important;
}}

.nps-app-hero__subtitle {{
  margin-top: 10px;
  color: rgba(255, 255, 255, 0.94) !important;
  font-size: 16px;
  line-height: 1.35;
  font-weight: 600;
  max-width: 920px;
}}

.nps-app-hero .nps-app-hero__subtitle,
div[data-testid="stMarkdownContainer"] .nps-app-hero .nps-app-hero__subtitle {{
  color: rgba(255, 255, 255, 0.94) !important;
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

.nps-hero-metric > span {{
  display: block;
  font-size: 11px;
  letter-spacing: .08em;
  text-transform: uppercase;
  color: var(--nps-muted);
  margin-bottom: 6px;
}}

.nps-hero-metric-help {{
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  margin-left: 6px;
  border-radius: 999px;
  border: 1px solid var(--nps-border-soft);
  background: color-mix(in srgb, var(--nps-surface) 88%, var(--nps-accent) 12%);
  color: var(--nps-muted);
  font-size: 12px;
  font-weight: 700;
  cursor: help;
  position: relative;
  flex: 0 0 auto;
}}

.nps-hero-metric-help-tooltip {{
  position: absolute;
  right: 0;
  bottom: calc(100% + 10px);
  width: min(320px, 60vw);
  padding: 10px 12px;
  border-radius: 14px;
  background: var(--nps-surface-2);
  border: 1px solid var(--nps-border-soft);
  box-shadow: var(--nps-shadow);
  color: var(--nps-text);
  font-size: 12px;
  line-height: 1.45;
  text-transform: none;
  letter-spacing: normal;
  white-space: normal;
  opacity: 0;
  pointer-events: none;
  transform: translateY(4px);
  transition: opacity 120ms ease, transform 120ms ease;
  z-index: 20;
}}

.nps-hero-metric-help:hover .nps-hero-metric-help-tooltip,
.nps-hero-metric-help:focus .nps-hero-metric-help-tooltip,
.nps-hero-metric-help:focus-visible .nps-hero-metric-help-tooltip {{
  opacity: 1;
  transform: translateY(0);
}}

.nps-hero-metric strong {{
  font-size: 24px;
  line-height: 1.05;
  flex: 1 1 auto;
  display: block;
}}

.nps-hero-metric-value {{
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  width: 100%;
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

.nps-evidence-toolbar-note {{
  margin-top: 6px;
  padding: 10px 14px;
  border-radius: 14px;
  border: 1px solid var(--nps-border-softer);
  background:
    linear-gradient(145deg, color-mix(in srgb, var(--nps-accent) 8%, var(--nps-surface-2)) 0%, var(--nps-surface-2) 100%);
  color: var(--nps-muted);
  font-size: 13px;
}}

.nps-evidence-grid {{
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 14px;
  margin-top: 8px;
}}

.nps-evidence-card {{
  position: relative;
  min-height: 150px;
  padding: 18px 18px 16px 18px;
  border-radius: 20px;
  border: 1px solid var(--nps-border-softer);
  background:
    radial-gradient(circle at top right, color-mix(in srgb, var(--nps-accent) 14%, transparent), transparent 38%),
    linear-gradient(180deg, color-mix(in srgb, var(--nps-accent) 4%, var(--nps-surface)) 0%, var(--nps-surface) 100%);
  box-shadow: 0 18px 45px rgba(7, 35, 86, 0.08);
}}

.nps-evidence-card-index {{
  display: inline-flex;
  align-items: center;
  justify-content: center;
  margin-bottom: 12px;
  padding: 6px 10px;
  border-radius: 999px;
  background: var(--nps-accent-soft);
  border: 1px solid var(--nps-border-strong);
  color: var(--nps-text);
  font-size: 11px;
  font-weight: 800;
  letter-spacing: .08em;
  text-transform: uppercase;
}}

.nps-evidence-card-index a {{
  color: inherit;
  text-decoration: none;
}}

.nps-evidence-card-index a:hover {{
  text-decoration: underline;
}}

.nps-evidence-card p {{
  margin: 0;
  line-height: 1.55;
  color: var(--nps-text);
}}

.nps-evidence-table-wrap {{
  width: 100%;
  overflow-x: auto;
  margin-top: 8px;
}}

.nps-evidence-table {{
  width: 100%;
  table-layout: auto;
  border-collapse: separate;
  border-spacing: 0;
  background: var(--nps-surface);
  border: 1px solid var(--nps-border-softer);
  border-radius: 18px;
  overflow: hidden;
}}

.nps-evidence-table thead th {{
  padding: 14px 16px;
  background: color-mix(in srgb, var(--nps-accent) 10%, var(--nps-surface-2));
  color: var(--nps-muted);
  font-size: 11px;
  font-weight: 800;
  letter-spacing: .08em;
  text-transform: uppercase;
  text-align: center;
  vertical-align: middle;
  border-bottom: 1px solid var(--nps-border-softer);
}}

.nps-evidence-table tbody td {{
  padding: 16px;
  color: var(--nps-text);
  line-height: 1.55;
  text-align: center;
  vertical-align: middle;
  border-bottom: 1px solid var(--nps-border-softer);
  word-break: break-word;
  white-space: normal;
}}

.nps-evidence-table tbody td.nps-band--detractor {{
  background: color-mix(in srgb, var(--nps-danger-soft) 14%, var(--nps-surface));
}}

.nps-evidence-table tbody td.nps-band--passive {{
  background: color-mix(in srgb, var(--nps-warning) 16%, var(--nps-surface));
}}

.nps-evidence-table tbody td.nps-band--promoter {{
  background: color-mix(in srgb, var(--nps-success) 14%, var(--nps-surface));
}}

.nps-evidence-table tbody tr:last-child td {{
  border-bottom: none;
}}

.nps-evidence-table a {{
  color: var(--nps-accent);
  font-weight: 700;
  text-decoration: none;
}}

.nps-evidence-table a:hover {{
  text-decoration: underline;
}}

.nps-data-table-wrap {{
  width: 100%;
  overflow-x: auto;
  overflow-y: auto;
  margin-top: 8px;
  border: 1px solid var(--nps-table-border-local, var(--nps-table-border)) !important;
  border-radius: 14px;
  background: var(--nps-table-bg-local, var(--nps-table-bg)) !important;
}}

.nps-data-table {{
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  background: var(--nps-table-bg-local, var(--nps-table-bg)) !important;
  color: var(--nps-table-text-local, var(--nps-text)) !important;
  font-size: 13px;
}}

.nps-data-table thead th {{
  position: sticky;
  top: 0;
  z-index: 1;
  background: var(--nps-table-header-bg-local, var(--nps-table-header-bg)) !important;
  color: var(--nps-table-header-text-local, var(--nps-table-header-text)) !important;
  text-align: left;
  padding: 10px 12px;
  border-bottom: 1px solid var(--nps-table-border-local, var(--nps-table-border)) !important;
  border-right: 1px solid var(--nps-table-border-local, var(--nps-table-border)) !important;
  font-weight: 700;
  white-space: nowrap;
}}

.nps-data-table thead th:last-child {{
  border-right: none;
}}

.nps-data-table tbody td {{
  background: var(--nps-table-bg-local, var(--nps-table-bg)) !important;
  color: var(--nps-table-text-local, var(--nps-text)) !important;
  text-align: left;
  padding: 8px 12px;
  border-bottom: 1px solid var(--nps-table-border-local, var(--nps-table-border)) !important;
  border-right: 1px solid var(--nps-table-border-local, var(--nps-table-border)) !important;
  white-space: nowrap;
}}

.nps-data-table tbody td:last-child {{
  border-right: none;
}}

.nps-data-table tbody tr:nth-child(even) td {{
  background: var(--nps-table-bg-alt-local, var(--nps-table-bg-alt)) !important;
}}

.nps-data-table tbody tr:hover td {{
  background: var(--nps-table-bg-hover-local, var(--nps-table-bg-hover)) !important;
}}

.nps-data-table th.nps-data-table__index,
.nps-data-table td.nps-data-table__index {{
  width: 56px;
  min-width: 56px;
  text-align: right;
  color: var(--nps-table-muted-local, var(--nps-muted)) !important;
  font-variant-numeric: tabular-nums;
}}

.nps-data-table--wrap tbody td {{
  white-space: normal !important;
  word-break: break-word;
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
  border: 1px solid var(--nps-table-border);
  background: var(--nps-table-bg) !important;
  box-shadow: var(--nps-shadow);
  --gdg-bg-cell: var(--nps-table-bg);
  --gdg-bg-cell-medium: var(--nps-table-bg-alt);
  --gdg-bg-header: var(--nps-table-header-bg);
  --gdg-bg-header-hovered: var(--nps-table-header-bg);
  --gdg-bg-header-has-focus: var(--nps-table-header-bg);
  --gdg-bg-search-result: color-mix(in srgb, var(--nps-accent) 14%, var(--nps-table-bg));
  --gdg-bg-search-result-hover: color-mix(in srgb, var(--nps-accent) 20%, var(--nps-table-bg));
  --gdg-border-color: var(--nps-table-border);
  --gdg-horizontal-border-color: var(--nps-table-border);
  --gdg-text-dark: var(--nps-text);
  --gdg-text-medium: var(--nps-muted);
  --gdg-text-light: var(--nps-muted);
  --gdg-text-header: var(--nps-table-header-text);
  --gdg-text-group-header: var(--nps-table-header-text);
  --gdg-accent-color: var(--nps-accent);
  --gdg-accent-fg: var(--nps-on-accent);
  --gdg-bg-bubble: color-mix(in srgb, var(--nps-accent) 20%, var(--nps-table-bg-alt));
  --gdg-font-family: inherit;
}}

/* Streamlit DataFrame uses AG-Grid under the hood */
div[data-testid="stDataFrame"] .ag-root-wrapper,
div[data-testid="stDataFrame"] .ag-root-wrapper-body,
div[data-testid="stDataFrame"] .ag-center-cols-viewport,
div[data-testid="stDataFrame"] .ag-body-viewport,
div[data-testid="stDataFrame"] .glideDataEditor,
div[data-testid="stDataFrame"] .glide-data-grid,
div[data-testid="stDataFrame"] .glide-data-grid * ,
div[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"],
div[data-testid="stDataFrame"] > div,
div[data-testid="stDataFrame"] canvas,
div[data-testid="stTable"] > div,
div[data-testid="stTable"] table {{
  background: var(--nps-table-bg) !important;
}}
div[data-testid="stDataFrame"] .ag-header,
div[data-testid="stDataFrame"] .ag-header-row,
div[data-testid="stDataFrame"] .ag-header-cell,
div[data-testid="stDataFrame"] [role="columnheader"] {{
  background: var(--nps-table-header-bg) !important;
  color: var(--nps-table-header-text) !important;
  border-color: var(--nps-table-border) !important;
}}
div[data-testid="stDataFrame"] .ag-row {{
  background: var(--nps-table-bg) !important;
  color: var(--nps-text) !important;
}}
div[data-testid="stDataFrame"] .ag-row:hover {{
  background: var(--nps-table-bg-hover) !important;
}}
div[data-testid="stDataFrame"] .ag-cell {{
  color: var(--nps-text) !important;
  border-color: var(--nps-table-border) !important;
}}
div[data-testid="stDataFrame"] [data-testid="StyledDataFrameCell"],
div[data-testid="stDataFrame"] [data-testid="StyledDataFrameCell"] * {{
  color: var(--nps-text) !important;
  background: var(--nps-table-bg) !important;
}}
div[data-testid="stDataFrame"] [role="grid"],
div[data-testid="stDataFrame"] [role="row"],
div[data-testid="stDataFrame"] [role="columnheader"],
div[data-testid="stDataFrame"] [role="gridcell"],
div[data-testid="stDataFrame"] .gdg-wmyidgi,
div[data-testid="stDataFrame"] .gdg-s1dgczr6,
div[data-testid="stDataFrame"] .gdg-seveqep,
div[data-testid="stDataFrame"] .gdg-d19meir1 {{
  background: var(--nps-table-bg) !important;
  color: var(--nps-text) !important;
}}
div[data-testid="stDataFrame"] [role="gridcell"] {{
  border-color: var(--nps-table-border) !important;
}}
div[data-testid="stTable"] table th,
div[data-testid="stTable"] table td {{
  background: var(--nps-table-bg) !important;
  color: var(--nps-text) !important;
  border-color: var(--nps-table-border) !important;
}}
div[data-testid="stTable"] table thead th {{
  background: var(--nps-table-header-bg) !important;
  color: var(--nps-table-header-text) !important;
}}

/* Controls (BaseWeb) — keep contrast in dark mode */
div[data-baseweb="select"] > div {{
  background: var(--nps-control-bg) !important;
  color: var(--nps-control-text) !important;
  border: 1px solid var(--nps-control-border) !important;
  border-radius: 10px !important;
  box-shadow: none !important;
}}
div[data-baseweb="select"] > div:hover {{
  background: var(--nps-control-bg-hover) !important;
}}
div[data-baseweb="select"] span,
div[data-baseweb="select"] input,
div[data-baseweb="select"] div[role="combobox"],
div[data-testid="stSelectbox"] span,
div[data-testid="stMultiSelect"] span {{
  color: var(--nps-control-text) !important;
}}
div[data-baseweb="select"] input::placeholder {{
  color: var(--nps-control-placeholder) !important;
}}
div[data-baseweb="select"] svg {{
  color: var(--nps-control-icon) !important;
  opacity: 1 !important;
}}
div[data-baseweb="select"] svg path,
div[data-baseweb="select"] svg line,
div[data-baseweb="select"] svg polyline,
div[data-baseweb="select"] svg polygon,
div[data-baseweb="select"] svg circle,
div[data-baseweb="select"] svg ellipse,
div[data-testid="stSelectbox"] svg,
div[data-testid="stSelectbox"] svg path,
div[data-testid="stSelectbox"] svg line,
div[data-testid="stSelectbox"] svg polyline,
div[data-testid="stSelectbox"] svg polygon,
div[data-testid="stSelectbox"] svg circle,
div[data-testid="stSelectbox"] svg ellipse,
div[data-testid="stMultiSelect"] svg,
div[data-testid="stMultiSelect"] svg path,
div[data-testid="stMultiSelect"] svg line,
div[data-testid="stMultiSelect"] svg polyline,
div[data-testid="stMultiSelect"] svg polygon,
div[data-testid="stMultiSelect"] svg circle,
div[data-testid="stMultiSelect"] svg ellipse {{
  fill: currentColor !important;
  stroke: currentColor !important;
  opacity: 1 !important;
}}
div[data-baseweb="select"] svg rect,
div[data-testid="stSelectbox"] svg rect,
div[data-testid="stMultiSelect"] svg rect {{
  stroke: currentColor !important;
  fill: transparent !important;
}}
div[data-baseweb="select"] [aria-hidden="true"] {{
  opacity: 1 !important;
}}
div[data-baseweb="popover"],
div[data-baseweb="popover"] > div,
div[data-baseweb="popover"] > div > div,
div[data-baseweb="menu"],
div[data-baseweb="menu"] > div,
div[data-baseweb="menu"] ul,
div[role="listbox"],
ul[role="listbox"] {{
  background: var(--nps-control-menu-bg) !important;
  border: 1px solid var(--nps-control-border) !important;
  border-radius: 12px !important;
}}
div[data-baseweb="popover"] *,
div[data-baseweb="menu"] *,
div[role="listbox"] *,
ul[role="listbox"] * {{
  color: var(--nps-control-text) !important;
}}
div[data-baseweb="popover"] [role="option"],
div[data-baseweb="menu"] [role="option"],
ul[role="listbox"] [role="option"],
li[role="option"] {{
  background: var(--nps-control-menu-bg) !important;
  color: var(--nps-control-text) !important;
  opacity: 1 !important;
}}
div[data-baseweb="popover"] [role="option"] *,
div[data-baseweb="menu"] [role="option"] *,
ul[role="listbox"] [role="option"] *,
li[role="option"] * {{
  color: var(--nps-control-text) !important;
  opacity: 1 !important;
}}
div[data-baseweb="popover"] [role="option"]:hover,
div[data-baseweb="menu"] [role="option"]:hover,
ul[role="listbox"] [role="option"]:hover,
li[role="option"]:hover {{
  background: var(--nps-control-menu-item-hover) !important;
}}
div[data-baseweb="popover"] [role="option"][aria-selected="true"],
div[data-baseweb="menu"] [role="option"][aria-selected="true"],
ul[role="listbox"] [role="option"][aria-selected="true"],
li[role="option"][aria-selected="true"] {{
  background: var(--nps-control-menu-item-selected) !important;
}}
div[data-baseweb="popover"] [aria-disabled="true"],
div[data-baseweb="menu"] [aria-disabled="true"],
ul[role="listbox"] [aria-disabled="true"] {{
  color: var(--nps-control-placeholder) !important;
}}
div[data-baseweb="popover"] input,
div[data-baseweb="popover"] textarea {{
  background: var(--nps-control-bg) !important;
  color: var(--nps-control-text) !important;
  border: 1px solid var(--nps-control-border) !important;
}}

/* Help/tooltip system: keep it tokenized and readable in both modes */
[data-testid="stTooltipIcon"],
[data-testid="stTooltipHoverTarget"] {{
  color: var(--nps-control-icon) !important;
}}
[data-testid="stTooltipIcon"] svg,
[data-testid="stTooltipHoverTarget"] svg {{
  color: inherit !important;
}}
[data-testid="stTooltipIcon"] svg path,
[data-testid="stTooltipHoverTarget"] svg path {{
  fill: currentColor !important;
  stroke: currentColor !important;
}}
[role="tooltip"],
div[data-baseweb="tooltip"],
div[data-baseweb="popover"][role="tooltip"],
div[data-testid="stTooltipContent"] {{
  background: var(--nps-control-menu-bg) !important;
  color: var(--nps-control-text) !important;
  border: 1px solid var(--nps-control-border) !important;
  border-radius: 12px !important;
  box-shadow: var(--nps-shadow);
}}
[role="tooltip"] *,
div[data-baseweb="tooltip"] *,
div[data-baseweb="popover"][role="tooltip"] *,
div[data-testid="stTooltipContent"] * {{
  color: var(--nps-control-text) !important;
}}

/* Dialogs: keep report modal aligned with app theme and use native close icon only. */
div[data-testid="stDialog"] > div[role="dialog"],
div[data-testid="stDialog"] [data-baseweb="modal"] {{
  background: var(--nps-surface) !important;
  color: var(--nps-text) !important;
  border: 1px solid var(--nps-border-soft) !important;
  border-radius: 20px !important;
  box-shadow: var(--nps-shadow) !important;
}}

div[data-testid="stDialog"] [data-testid="stDialogContent"] {{
  background: transparent !important;
}}

div[data-testid="stDialog"] [data-testid="stMarkdownContainer"],
div[data-testid="stDialog"] [data-testid="stMarkdownContainer"] *,
div[data-testid="stDialog"] h1,
div[data-testid="stDialog"] h2,
div[data-testid="stDialog"] h3,
div[data-testid="stDialog"] p,
div[data-testid="stDialog"] label,
div[data-testid="stDialog"] span {{
  color: var(--nps-text) !important;
}}

div[data-testid="stDialog"] button[aria-label="Close"] {{
  color: var(--nps-text) !important;
  background: color-mix(in srgb, var(--nps-surface-2) 92%, transparent) !important;
  border: 1px solid var(--nps-border-soft) !important;
  border-radius: 12px !important;
}}

div[data-testid="stDialog"] button[aria-label="Close"]:hover {{
  background: var(--nps-control-bg-hover) !important;
}}

/* Plotly containers */
[data-testid="stPlotlyChart"] {{
  border-radius: 22px;
  border: 1px solid var(--nps-border-soft);
  background: linear-gradient(
      180deg,
      color-mix(in srgb, var(--nps-accent) 4%, var(--nps-chart-paper)) 0%,
      var(--nps-chart-paper) 100%
    ) !important;
  padding: 14px 14px 6px 14px;
  box-shadow: var(--nps-shadow);
  overflow: hidden;
}}

[data-testid="stPlotlyChart"] > div {{
  background: transparent !important;
}}

[data-testid="stPlotlyChart"] .js-plotly-plot,
[data-testid="stPlotlyChart"] .plot-container,
[data-testid="stPlotlyChart"] .svg-container {{
  background: transparent !important;
}}

[data-testid="stPlotlyChart"] .modebar-container {{
  position: static !important;
  top: auto !important;
  right: auto !important;
  left: auto !important;
  width: 100% !important;
  display: flex !important;
  justify-content: flex-end !important;
  align-items: center !important;
  margin: 0 0 10px 0 !important;
  padding: 0 !important;
  pointer-events: none;
}}

[data-testid="stPlotlyChart"] .modebar {{
  position: static !important;
  top: auto !important;
  right: auto !important;
  left: auto !important;
  z-index: 1 !important;
  background: color-mix(in srgb, var(--nps-control-bg) 92%, transparent) !important;
  border: 1px solid var(--nps-control-border) !important;
  border-radius: 12px !important;
  padding: 3px 5px !important;
  opacity: 0.96 !important;
  transform: none !important;
  transition: background-color 120ms ease;
  margin-left: auto !important;
  pointer-events: auto !important;
}}

[data-testid="stPlotlyChart"]:hover .modebar,
[data-testid="stPlotlyChart"] .modebar:hover,
[data-testid="stPlotlyChart"] .modebar:focus-within {{
  opacity: 1 !important;
}}

[data-testid="stPlotlyChart"] .modebar-group {{
  padding: 0 1px;
}}

[data-testid="stPlotlyChart"] .modebar-btn {{
  width: 26px !important;
  height: 24px !important;
  border-radius: 8px !important;
}}

[data-testid="stPlotlyChart"] .modebar-btn:hover {{
  background: var(--nps-control-menu-item-hover) !important;
}}

[data-testid="stPlotlyChart"] .modebar-btn.active {{
  background: var(--nps-control-menu-item-selected) !important;
}}

[data-testid="stPlotlyChart"] .modebar-btn svg {{
  fill: var(--nps-control-icon) !important;
  stroke: var(--nps-control-icon) !important;
}}
[data-testid="stPlotlyChart"] .modebar-btn svg path,
[data-testid="stPlotlyChart"] .modebar-btn svg * {{
  fill: var(--nps-control-icon) !important;
  stroke: var(--nps-control-icon) !important;
}}

[data-testid="stPlotlyChart"] .modebar-btn:hover svg {{
  fill: var(--nps-control-text) !important;
  stroke: var(--nps-control-text) !important;
}}
[data-testid="stPlotlyChart"] .modebar-btn:hover svg path,
[data-testid="stPlotlyChart"] .modebar-btn:hover svg * {{
  fill: var(--nps-control-text) !important;
  stroke: var(--nps-control-text) !important;
}}
div[data-testid="stRadio"] label, div[data-testid="stCheckbox"] label {{
  color: var(--nps-text) !important;
}}
div[data-testid="stMarkdownContainer"] a {{
  color: var(--nps-accent) !important;
}}

/* Inputs */
input, textarea {{
  color: var(--nps-text) !important;
  background: var(--nps-control-bg) !important;
  border: 1px solid var(--nps-control-border) !important;
  border-radius: 12px !important;
}}

/* Some Streamlit widgets set inline styles; override aggressively */
textarea[style], input[style] {{
  background: var(--nps-control-bg) !important;
  color: var(--nps-text) !important;
}}

/* Text areas in Streamlit (executive report, prompts, etc.) */
div[data-testid="stTextArea"] textarea {{
  background: var(--nps-control-bg) !important;
  color: var(--nps-text) !important;
}}

div[data-testid="stTextArea"] > div > textarea {{
  background: var(--nps-control-bg) !important;
  color: var(--nps-text) !important;
}}

div[data-testid="stTextArea"] textarea::placeholder {{
  color: var(--nps-control-placeholder) !important;
}}


/* File uploader */
div[data-testid="stFileUploaderDropzone"] {{
  background: var(--nps-control-bg) !important;
  border: 1px dashed var(--nps-border-stronger) !important;
  border-radius: var(--nps-radius) !important;
}}
div[data-testid="stFileUploader"] [data-testid="stFileUploaderDropzone"],
div[data-testid="stFileUploader"] [data-testid="stFileUploaderDropzone"] > div,
div[data-testid="stFileUploader"] [data-testid="stFileUploaderDropzone"] > div > div {{
  background: var(--nps-control-bg) !important;
  border-color: var(--nps-border-stronger) !important;
}}
div[data-testid="stFileUploaderDropzone"] *,
div[data-testid="stFileUploader"] [data-testid="stFileUploaderDropzone"] * {{
  color: var(--nps-text) !important;
}}
div[data-testid="stFileUploader"] small {{
  color: var(--nps-muted) !important;
}}
div[data-testid="stFileUploader"] button,
div[data-testid="stFileUploader"] button[kind="secondary"] {{
  background: var(--nps-control-bg-hover) !important;
  color: var(--nps-control-text) !important;
  border: 1px solid var(--nps-control-border) !important;
}}

/* Plotly: force dark surfaces + readable text in dark mode */
div[data-testid="stPlotlyChart"] {{
  background: var(--nps-chart-paper) !important;
  border-radius: var(--nps-radius);
  border: 1px solid var(--nps-border-soft);
}}
div[data-testid="stPlotlyChart"] .js-plotly-plot .plotly .main-svg .bg {{
  fill: var(--nps-chart-plot) !important;
}}
div[data-testid="stPlotlyChart"] .js-plotly-plot .plotly text {{
  fill: var(--nps-text) !important;
}}
div[data-testid="stPlotlyChart"] .js-plotly-plot .plotly .gridlayer path {{
  stroke: var(--nps-chart-grid) !important;
}}
div[data-testid="stPlotlyChart"] .js-plotly-plot .plotly .zerolinelayer path {{
  stroke: var(--nps-chart-zero) !important;
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
  background: var(--nps-chart-paper) !important;
}}

</style>
""",
        unsafe_allow_html=True,
    )
