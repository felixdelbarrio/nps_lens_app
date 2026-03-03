from __future__ import annotations

from pathlib import Path

from nps_lens.design.tokens import DesignTokens, primary_color


def streamlit_css(repo_root: Path) -> str:
    """Minimal CSS aligned to BBVA Experience tokens.

    Nota: Streamlit tiene limitaciones de theming. Esta capa evita inventar estilos;
    solo ajusta acentos y espaciado de forma conservadora.
    """
    t = DesignTokens.load(repo_root)
    accent = primary_color(t)
    navy = t.core.get("bbva_navy_900", "#070E46")
    grey = t.core.get("bbva_grey_200", "#F7F8F8")

    white = t.core.get("bbva_white_100", "#FFFFFF")
    grey_300 = t.core.get("bbva_grey_300", "#E2E6EA")

    # Streamlit is restrictive; we keep this as a light, token-driven layer.
    return f"""
<style>
:root {{
  --nps-accent: {accent};
  --nps-navy: {navy};
  --nps-bg: {grey};
  --nps-white: {white};
  --nps-border: {grey_300};
}}

/* App background */
div[data-testid="stAppViewContainer"] {{
  background: var(--nps-bg);
}}

/* Typography */
h1, h2, h3, h4 {{
  color: var(--nps-navy);
}}

/* Containers / cards */
.nps-card {{
  background: var(--nps-white);
  border: 1px solid var(--nps-border);
  border-radius: 14px;
  padding: 14px 16px;
}}
.nps-muted {{
  color: rgba(7, 14, 70, 0.75);
}}
.nps-kpi {{
  font-size: 28px;
  font-weight: 700;
  line-height: 1.1;
}}
.nps-kpi-label {{
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.02em;
  text-transform: uppercase;
  color: rgba(7, 14, 70, 0.7);
}}

/* Badges */
.nps-badge {{
  display: inline-block;
  padding: 2px 10px;
  border-radius: 999px;
  border: 1px solid var(--nps-border);
  font-size: 12px;
  font-weight: 600;
  color: var(--nps-navy);
  background: var(--nps-white);
}}
.nps-badge--accent {{
  border-color: var(--nps-accent);
}}

/* Buttons */
button[kind="primary"] {{
  border-color: var(--nps-accent) !important;
}}

/* Make dataframes readable in wide layout */
div[data-testid="stDataFrame"] {{
  background: var(--nps-white);
  border: 1px solid var(--nps-border);
  border-radius: 14px;
  padding: 6px;
}}
</style>
"""
