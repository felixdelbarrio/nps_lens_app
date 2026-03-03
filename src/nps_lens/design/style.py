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

    return f"""
<style>
:root {{
  --nps-accent: {accent};
  --nps-navy: {navy};
  --nps-bg: {grey};
}}
div[data-testid="stAppViewContainer"] {{
  background: var(--nps-bg);
}}
h1, h2, h3 {{
  color: var(--nps-navy);
}}
button[kind="primary"] {{
  border-color: var(--nps-accent) !important;
}}
</style>
"""
