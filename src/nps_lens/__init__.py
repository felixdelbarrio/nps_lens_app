"""NPS Lens — multi-fuente VoC analytics (NPS + texto + incidencias).

This package is designed to run on Python 3.9.x in corporate environments.
"""

__all__ = ["__version__", "PIPELINE_VERSION"]

# Package version (distribution)
__version__ = "0.1.0"

# Pipeline version (cache invalidation / reproducibility)
# Bump when feature engineering, joins, scoring, or pack contracts change.
PIPELINE_VERSION = "2026.03.05-extreme"
