from __future__ import annotations

from typing import Any

from typer.testing import CliRunner

from nps_lens.cli import app


runner = CliRunner()


def test_serve_accepts_host_and_port(monkeypatch: Any) -> None:
    captured: dict[str, object] = {}

    def fake_run(*args: object, **kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr("nps_lens.cli.load_runtime_dotenv", lambda: None)
    monkeypatch.setattr("nps_lens.cli.setup_logging", lambda _level: None)
    monkeypatch.setattr("nps_lens.cli.uvicorn.run", fake_run)

    result = runner.invoke(app, ["serve", "--host", "127.0.0.1", "--port", "4100"])

    assert result.exit_code == 0, result.output
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 4100
