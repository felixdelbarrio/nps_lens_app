from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict


@dataclass(frozen=True)
class DesignTokens:
    core: Dict[str, str]
    tokens: Dict[str, str]

    @staticmethod
    def load(repo_root: Path) -> "DesignTokens":
        path = repo_root / "design" / "tokens.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        return DesignTokens(core=dict(data.get("core", {})), tokens=dict(data.get("tokens", {})))


def primary_color(tokens: DesignTokens) -> str:
    # Conservative: prefer BBVA Blue 500 if present.
    return tokens.core.get("bbva_blue_500", "#85C8FF")
