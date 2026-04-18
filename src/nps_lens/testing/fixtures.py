from __future__ import annotations

import unicodedata
from pathlib import Path

FIXTURES_DIR = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "excel"


def fixture_excel(name: str) -> Path:
    expected = unicodedata.normalize("NFD", name)
    for candidate in FIXTURES_DIR.iterdir():
        if unicodedata.normalize("NFD", candidate.name) == expected:
            return candidate
    raise FileNotFoundError(f"Fixture not found: {name}")
