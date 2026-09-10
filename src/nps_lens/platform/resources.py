from __future__ import annotations

import sys
from pathlib import Path


def resource_root() -> Path:
    """Return the repository root or PyInstaller's bundled Resources directory."""
    if getattr(sys, "frozen", False):
        bundled_root = getattr(sys, "_MEIPASS", None)
        if bundled_root:
            return Path(str(bundled_root)).resolve()
    return Path(__file__).resolve().parents[3]
