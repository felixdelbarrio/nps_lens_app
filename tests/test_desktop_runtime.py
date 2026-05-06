from __future__ import annotations

import sys
from pathlib import Path

import pytest
import uvicorn

from nps_lens import desktop
from nps_lens.logging import setup_logging


def test_setup_logging_handles_windowed_executable_without_stdio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "stdout", None)
    monkeypatch.setattr(sys, "stderr", None)

    setup_logging("INFO")


def test_embedded_server_disables_uvicorn_default_logging_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    frontend_dist = tmp_path / "frontend" / "dist"
    frontend_dist.mkdir(parents=True)
    (frontend_dist / "index.html").write_text("<!doctype html>", encoding="utf-8")
    monkeypatch.setattr(desktop, "_frontend_dist_path", lambda: frontend_dist)

    captured: dict[str, object] = {}

    def fake_run(*args: object, **kwargs: object) -> None:
        captured["args"] = args
        captured.update(kwargs)

    monkeypatch.setattr(uvicorn, "run", fake_run)

    desktop._run_api_server(8765)

    assert captured["log_config"] is None
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8765
