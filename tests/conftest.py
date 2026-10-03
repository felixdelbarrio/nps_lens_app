import sys
from pathlib import Path

import pytest

# Ensure `src/` is importable when running tests without an editable install.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(autouse=True)
def configured_company(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", '["Test Bank"]')
