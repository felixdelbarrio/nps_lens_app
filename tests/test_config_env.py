from pathlib import Path

import pytest

import nps_lens.config as config_module
from nps_lens.config import Settings, persist_ui_prefs, ui_pref


def test_settings_reads_context_values_from_env(monkeypatch):
    # service_origin_buug values
    monkeypatch.setenv(
        "NPS_LENS_SERVICE_ORIGIN_BUUG",
        "BBVA México, BBVA España",
    )
    # mapping origin -> n1
    monkeypatch.setenv(
        "NPS_LENS_SERVICE_ORIGIN_N1",
        '{"BBVA México": ["Senda", "Helix"], "BBVA España": ["Senda"]}',
    )
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N2", "SN2A, SN2B")
    monkeypatch.setenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN", "BBVA México")
    monkeypatch.setenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1", "Senda")
    monkeypatch.setenv("NPS_LENS_UI_MIN_N_CROSS_COMPARISONS", "40")

    s = Settings.from_env()

    assert s.service_origin_values == ["BBVA México", "BBVA España"]
    assert s.service_origin_n1_map["BBVA México"] == ["Senda", "Helix"]
    assert s.service_origin_n2_values == ["SN2A", "SN2B"]
    assert s.default_min_n_cross_comparisons == 40


def test_settings_accepts_compact_n1_format(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "BBVA México")
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", "BBVA México:Senda|Helix")
    monkeypatch.delenv("NPS_LENS_SERVICE_ORIGIN_N2", raising=False)

    s = Settings.from_env()
    assert s.service_origin_n1_map["BBVA México"] == ["Senda", "Helix"]


def test_settings_fails_fast_when_required_env_missing(monkeypatch):
    # Missing NPS_LENS_SERVICE_ORIGIN_N1
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "BBVA México")
    monkeypatch.delenv("NPS_LENS_SERVICE_ORIGIN_N1", raising=False)
    monkeypatch.delenv("SERVICE_ORIGIN_N1", raising=False)

    with pytest.raises(RuntimeError) as e:
        Settings.from_env()
    assert "NPS_LENS_SERVICE_ORIGIN_N1" in str(e.value)


def test_settings_fails_fast_when_n1_map_incomplete(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "BBVA México, BBVA España")
    # Map only defines México
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", '{"BBVA México": ["Senda"]}')

    with pytest.raises(RuntimeError) as e:
        Settings.from_env()
    msg = str(e.value)
    assert "missing entries" in msg
    assert "BBVA España" in msg


def test_settings_normalizes_defaults_and_numeric_bounds(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", '["MX", "ES"]')
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", '{"MX": "Senda,Helix", "ES": ["Web"]}')
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N2", '["N2A", "N2B"]')
    monkeypatch.setenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN", "AR")
    monkeypatch.setenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1", "Otro")
    monkeypatch.setenv("NPS_LENS_UI_THEME_MODE", "SEPIA")
    monkeypatch.setenv("NPS_LENS_UI_TOUCHPOINT_SOURCE", "")
    monkeypatch.setenv("NPS_LENS_UI_MIN_SIMILARITY", "2.0")
    monkeypatch.setenv("NPS_LENS_UI_MAX_DAYS_APART", "-5")
    monkeypatch.setenv("NPS_LENS_UI_MIN_N_OPPORTUNITIES", "10")
    monkeypatch.setenv("NPS_LENS_UI_MIN_N_CROSS_COMPARISONS", "500")

    s = Settings.from_env()

    assert s.default_service_origin == "MX"
    assert s.default_service_origin_n1 == "Senda"
    assert s.default_theme_mode == "light"
    assert s.default_touchpoint_source
    assert s.default_min_similarity == 1.0
    assert s.default_max_days_apart == 0
    assert s.default_min_n_opportunities == 50
    assert s.default_min_n_cross_comparisons == 200


def test_ui_pref_and_persist_ui_prefs_roundtrip(tmp_path: Path, monkeypatch):
    dotenv_path = tmp_path / ".env"
    persist_ui_prefs(
        dotenv_path,
        {
            "service_origin": "BBVA México",
            "theme_mode": "dark",
            "unknown": "ignored",
        },
    )

    assert "NPS_LENS_UI_SERVICE_ORIGIN" in dotenv_path.read_text(encoding="utf-8")
    assert ui_pref("service_origin") == "BBVA México"
    assert ui_pref("theme_mode") == "dark"
    assert ui_pref("missing", default="fallback") == "fallback"

    monkeypatch.delenv("NPS_LENS_UI_SERVICE_ORIGIN", raising=False)
    persist_ui_prefs(None, {"service_origin": "No-op"})


def test_settings_parsers_ignore_invalid_and_blank_mapping_entries(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "MX")
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", '{"": ["skip"], "MX": "Senda,Helix"}')
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N2", '{"unexpected": "object"}')

    s = Settings.from_env()
    assert s.service_origin_n1_map == {"MX": ["Senda", "Helix"]}
    assert s.service_origin_n2_values == ['{"unexpected": "object"}']


def test_settings_uses_user_writable_dirs_for_relative_paths_in_frozen_mode(
    monkeypatch, tmp_path: Path
):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "MX")
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", '{"MX": ["Senda"]}')
    monkeypatch.setenv("NPS_LENS_DATA_DIR", "./data")
    monkeypatch.setenv("NPS_LENS_KNOWLEDGE_DIR", "./knowledge")
    fake_home = tmp_path / "home"
    fake_home.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.delenv("NPS_LENS_APP_HOME", raising=False)
    monkeypatch.setattr(config_module.sys, "frozen", True, raising=False)

    s = Settings.from_env()
    assert s.data_dir == fake_home / ".nps-lens" / "data"
    assert s.knowledge_dir == fake_home / ".nps-lens" / "knowledge"
