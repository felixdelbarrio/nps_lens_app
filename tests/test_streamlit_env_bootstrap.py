from __future__ import annotations

import importlib.util
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def _load_streamlit_app_module():
    path = Path(__file__).resolve().parents[1] / "app" / "streamlit_app.py"
    spec = importlib.util.spec_from_file_location("test_streamlit_app_env", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_resolve_runtime_dotenv_paths_bootstraps_user_env_for_frozen(monkeypatch, tmp_path) -> None:
    streamlit_app = _load_streamlit_app_module()

    bundle_root = tmp_path / "bundle"
    app_dir = bundle_root / "app"
    app_dir.mkdir(parents=True)
    here = app_dir / "streamlit_app.py"
    here.touch()

    bundled_example = bundle_root / ".env.example"
    bundled_example.write_text(
        "NPS_LENS_SERVICE_ORIGIN_BUUG=BBVA México\n"
        'NPS_LENS_SERVICE_ORIGIN_N1={"BBVA México": ["Senda"]}\n',
        encoding="utf-8",
    )

    fake_home = tmp_path / "home"
    fake_home.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.delenv("NPS_LENS_ENV_FILE", raising=False)
    monkeypatch.setattr(streamlit_app, "find_dotenv", lambda usecwd=True: "")
    monkeypatch.setattr(streamlit_app.sys, "frozen", True, raising=False)

    dotenv_path, prefs_path = streamlit_app._resolve_runtime_dotenv_paths(here=here)

    expected = fake_home / ".nps-lens" / ".env"
    assert dotenv_path == expected
    assert prefs_path == expected
    assert expected.exists()
    assert "NPS_LENS_SERVICE_ORIGIN_BUUG=BBVA México" in expected.read_text(encoding="utf-8")


def test_resolve_runtime_dotenv_paths_bootstraps_repo_env_for_non_frozen(
    monkeypatch, tmp_path
) -> None:
    streamlit_app = _load_streamlit_app_module()

    repo_root = tmp_path / "repo"
    app_dir = repo_root / "app"
    app_dir.mkdir(parents=True)
    here = app_dir / "streamlit_app.py"
    here.touch()

    bundled_example = repo_root / ".env.example"
    bundled_example.write_text(
        "NPS_LENS_SERVICE_ORIGIN_BUUG=BBVA México\n"
        'NPS_LENS_SERVICE_ORIGIN_N1={"BBVA México": ["Senda"]}\n',
        encoding="utf-8",
    )

    monkeypatch.delenv("NPS_LENS_ENV_FILE", raising=False)
    monkeypatch.setattr(streamlit_app, "find_dotenv", lambda usecwd=True: "")
    monkeypatch.setattr(streamlit_app.sys, "frozen", False, raising=False)

    dotenv_path, prefs_path = streamlit_app._resolve_runtime_dotenv_paths(here=here)

    expected = repo_root / ".env"
    assert dotenv_path == expected
    assert prefs_path == expected
    assert expected.exists()
    assert "NPS_LENS_SERVICE_ORIGIN_BUUG=BBVA México" in expected.read_text(encoding="utf-8")
