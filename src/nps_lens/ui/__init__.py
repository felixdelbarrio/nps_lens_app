"""Streamlit UI layer.

This package intentionally avoids re-exporting symbols.

Rationale:
- Keeps imports fast and deterministic (no heavy, optional deps at package import time).
- Prevents accidental coupling and circular imports.

Import UI helpers from their concrete modules, e.g.:
    from nps_lens.ui.theme import Theme
"""
