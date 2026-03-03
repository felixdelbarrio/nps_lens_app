from __future__ import annotations

from pathlib import Path


def icon_path(repo_root: Path, icon_name: str) -> Path:
    """Returns path to an SVG/PNG icon from the provided BBVA Experience icon set.

    The icon set is shipped in assets/icons (extracted from all_icons.zip).
    """
    base = repo_root / "assets" / "icons"
    # try common patterns
    candidates = [
        base / f"{icon_name}.svg",
        base / f"{icon_name}.png",
    ]
    for c in candidates:
        if c.exists():
            return c
    return candidates[0]
