from pathlib import Path

from dotenv import dotenv_values

import nps_lens.config as config_module
import nps_lens.settings as settings_module
from nps_lens.config import (
    DEFAULT_UI_HELIX_BASE_URL,
    DEFAULT_UI_REPORT_DIMENSION_ANALYSIS,
    Settings,
    load_runtime_dotenv,
    normalize_report_dimension_analysis,
    persist_ui_prefs,
    ui_pref,
)

DOTENV_TEST_KEYS = {
    "NPS_LENS_DATA_DIR",
    "NPS_LENS_KNOWLEDGE_DIR",
    "NPS_LENS_DEFAULT_SERVICE_ORIGIN",
    "NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1",
    "NPS_LENS_SERVICE_ORIGIN",
    "NPS_LENS_SERVICE_ORIGIN_BUUG",
    "NPS_LENS_SERVICE_ORIGIN_N1",
    "NPS_LENS_SERVICE_ORIGIN_N2",
    "NPS_LENS_SERVICE_ORIGIN_N2_MAP",
    "NPS_LENS_LOG_LEVEL",
    "NPS_LENS_UI_SERVICE_ORIGIN",
    "NPS_LENS_UI_SERVICE_ORIGIN_N1",
    "NPS_LENS_UI_SERVICE_ORIGIN_N2",
    "NPS_LENS_UI_POP_YEAR",
    "NPS_LENS_UI_POP_MONTH",
    "NPS_LENS_UI_NPS_GROUP",
    "NPS_LENS_UI_THEME_MODE",
    "NPS_LENS_UI_DOWNLOADS_PATH",
    "NPS_LENS_UI_HELIX_BASE_URL",
    "NPS_LENS_UI_TOUCHPOINT_SOURCE",
    "NPS_LENS_UI_MIN_SIMILARITY",
    "NPS_LENS_UI_MAX_DAYS_APART",
    "NPS_LENS_UI_MIN_N_NPS_GAPS",
    "NPS_LENS_UI_MIN_N_CROSS_COMPARISONS",
    "NPS_LENS_UI_SCORE_CHANNEL",
    "NPS_LENS_UI_REPORT_DIMENSION_ANALYSIS",
    "NPS_LENS_TAXONOMY_DISCOVERY_METHOD",
    "NPS_LENS_TAXONOMY_DESIGNER_URL",
    "NPS_LENS_TAXONOMY_CLASSIFIER_URL",
    "NPS_LENS_TAXONOMY_BATCH_SIZE",
    "NPS_LENS_PORT",
    "NPS_LENS_PPT_TEMPLATE",
    "NPS_LENS_OPENAI_API_KEY",
    "NPS_LENS_OPENAI_MODEL",
    "NPS_LENS_OPENAI_TEMPERATURE",
    "NPS_LENS_OPENAI_TIMEOUT_S",
    "NPS_LENS_PROFILE",
}


def clear_dotenv_test_keys(monkeypatch) -> None:
    for env_key in DOTENV_TEST_KEYS:
        monkeypatch.delenv(env_key, raising=False)


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

    assert s.allowed_service_origins == ["BBVA México", "BBVA España"]
    assert s.allowed_service_origin_n1["BBVA México"] == ["Senda", "Helix"]
    assert s.service_origin_n2_values == ["SN2A", "SN2B"]
    assert s.default_min_n_cross_comparisons == 40
    assert s.default_downloads_path.endswith("Downloads")
    assert s.default_helix_base_url == DEFAULT_UI_HELIX_BASE_URL
    assert s.default_report_dimension_analysis == DEFAULT_UI_REPORT_DIMENSION_ANALYSIS


def test_settings_accepts_compact_n1_format(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "BBVA México")
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", "BBVA México:Senda|Helix")
    monkeypatch.delenv("NPS_LENS_SERVICE_ORIGIN_N2", raising=False)

    s = Settings.from_env()
    assert s.allowed_service_origin_n1["BBVA México"] == ["Senda", "Helix"]


def test_settings_uses_safe_defaults_when_context_env_missing(monkeypatch):
    clear_dotenv_test_keys(monkeypatch)

    s = Settings.from_env()

    assert "BBVA México" in s.allowed_service_origins
    assert s.allowed_service_origin_n1["BBVA México"] == ["ENTERPRISE WEB", "MOBILE ENTERPRISE"]
    assert s.default_service_origin == "BBVA México"
    assert s.default_service_origin_n1 == "ENTERPRISE WEB"


def test_settings_repairs_missing_n1_map_from_defaults(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "BBVA México")
    monkeypatch.delenv("NPS_LENS_SERVICE_ORIGIN_N1", raising=False)
    monkeypatch.delenv("SERVICE_ORIGIN_N1", raising=False)

    s = Settings.from_env()

    assert s.allowed_service_origin_n1["BBVA México"] == ["ENTERPRISE WEB", "MOBILE ENTERPRISE"]


def test_settings_repairs_incomplete_n1_map_from_defaults(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "BBVA México, BBVA España")
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", '{"BBVA México": ["Senda"]}')

    s = Settings.from_env()

    assert s.allowed_service_origin_n1["BBVA México"] == ["Senda"]
    assert s.allowed_service_origin_n1["BBVA España"] == [
        "ENTERPRISE MOBILE CHANNEL",
        "ENTERPRISES CHANNEL",
    ]


def test_settings_normalizes_defaults_and_numeric_bounds(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", '["MX", "ES"]')
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", '{"MX": "Senda,Helix", "ES": ["Web"]}')
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N2", '["N2A", "N2B"]')
    monkeypatch.setenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN", "AR")
    monkeypatch.setenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1", "Otro")
    monkeypatch.setenv("NPS_LENS_UI_THEME_MODE", "SEPIA")
    monkeypatch.setenv("NPS_LENS_UI_TOUCHPOINT_SOURCE", "")
    monkeypatch.setenv("NPS_LENS_UI_REPORT_DIMENSION_ANALYSIS", "dimension")
    monkeypatch.setenv("NPS_LENS_UI_MIN_SIMILARITY", "2.0")
    monkeypatch.setenv("NPS_LENS_UI_MAX_DAYS_APART", "-5")
    monkeypatch.setenv("NPS_LENS_UI_MIN_N_NPS_GAPS", "10")
    monkeypatch.setenv("NPS_LENS_UI_MIN_N_CROSS_COMPARISONS", "500")

    s = Settings.from_env()

    assert s.default_service_origin == "MX"
    assert s.default_service_origin_n1 == "Senda"
    assert s.default_theme_mode == "light"
    assert s.default_touchpoint_source
    assert s.default_report_dimension_analysis == "palanca"
    assert s.default_min_similarity == 1.0
    assert s.default_max_days_apart == 0
    assert s.default_min_n_nps_gaps == 50
    assert s.default_min_n_cross_comparisons == 200


def test_ui_pref_and_persist_ui_prefs_roundtrip(tmp_path: Path, monkeypatch):
    dotenv_path = tmp_path / ".env"
    persist_ui_prefs(
        dotenv_path,
        {
            "service_origin": "BBVA México",
            "theme_mode": "dark",
            "downloads_path": "~/Downloads/nps-lens",
            "helix_base_url": "https://example.com/helix",
            "report_dimension_analysis": "subpalanca",
            "unknown": "ignored",
        },
    )

    assert "NPS_LENS_UI_SERVICE_ORIGIN" in dotenv_path.read_text(encoding="utf-8")
    assert "NPS_LENS_UI_DOWNLOADS_PATH" in dotenv_path.read_text(encoding="utf-8")
    assert ui_pref("service_origin") == "BBVA México"
    assert ui_pref("theme_mode") == "dark"
    assert ui_pref("downloads_path").endswith("Downloads/nps-lens")
    assert ui_pref("helix_base_url") == "https://example.com/helix/"
    assert ui_pref("report_dimension_analysis") == "subpalanca"
    assert ui_pref("missing", default="fallback") == "fallback"
    assert normalize_report_dimension_analysis("other") == "palanca"

    monkeypatch.delenv("NPS_LENS_UI_SERVICE_ORIGIN", raising=False)
    persist_ui_prefs(None, {"service_origin": "No-op"})


def test_taxonomy_discovery_settings_are_local_and_validated(tmp_path: Path, monkeypatch) -> None:
    clear_dotenv_test_keys(monkeypatch)
    dotenv_path = tmp_path / ".env"
    persist_ui_prefs(
        dotenv_path,
        {
            "taxonomy_discovery_method": "chatgpt_browser",
            "taxonomy_designer_url": "https://chatgpt.com/g/designer",
            "taxonomy_classifier_url": "https://chatgpt.com/g/classifier",
        },
    )
    settings = Settings.from_env()
    assert settings.taxonomy_discovery_method == "chatgpt_browser"
    assert settings.taxonomy_designer_url == "https://chatgpt.com/g/designer"
    assert settings.taxonomy_classifier_url == "https://chatgpt.com/g/classifier"
    assert settings.ui_defaults()["taxonomy_discovery_method"] == "chatgpt_browser"
    for key in (
        "NPS_LENS_TAXONOMY_DISCOVERY_METHOD",
        "NPS_LENS_TAXONOMY_DESIGNER_URL",
        "NPS_LENS_TAXONOMY_CLASSIFIER_URL",
    ):
        monkeypatch.delenv(key, raising=False)


def test_settings_parsers_ignore_invalid_and_blank_mapping_entries(monkeypatch):
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_BUUG", "MX")
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N1", '{"": ["skip"], "MX": "Senda,Helix"}')
    monkeypatch.setenv("NPS_LENS_SERVICE_ORIGIN_N2", '{"unexpected": "object"}')

    s = Settings.from_env()
    assert s.allowed_service_origin_n1 == {"MX": ["Senda", "Helix"]}
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


def test_load_runtime_dotenv_bootstraps_missing_file_from_example(
    monkeypatch, tmp_path: Path
) -> None:
    clear_dotenv_test_keys(monkeypatch)
    dotenv_path = tmp_path / ".env"
    monkeypatch.setenv("NPS_LENS_DOTENV_PATH", str(dotenv_path))

    loaded_path = load_runtime_dotenv()

    assert loaded_path == dotenv_path
    assert dotenv_path.exists()
    values = dotenv_values(dotenv_path)
    assert values["NPS_LENS_SERVICE_ORIGIN_N1"]
    assert "NPS_LENS_UI_SERVICE_ORIGIN" in values
    assert "NPS_LENS_SERVICE_ORIGIN_N1" in settings_module.os.environ


def test_load_runtime_dotenv_repairs_existing_empty_context_keys(
    monkeypatch, tmp_path: Path
) -> None:
    clear_dotenv_test_keys(monkeypatch)
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "NPS_LENS_DATA_DIR=./custom-data\nNPS_LENS_SERVICE_ORIGIN_N1=\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("NPS_LENS_DOTENV_PATH", str(dotenv_path))

    load_runtime_dotenv()

    values = dotenv_values(dotenv_path)
    assert values["NPS_LENS_DATA_DIR"] == "./custom-data"
    assert values["NPS_LENS_SERVICE_ORIGIN_N1"]


def test_missing_dotenv_example_does_not_block_startup(monkeypatch, tmp_path: Path) -> None:
    clear_dotenv_test_keys(monkeypatch)
    dotenv_path = tmp_path / ".env"
    monkeypatch.setenv("NPS_LENS_DOTENV_PATH", str(dotenv_path))
    monkeypatch.setattr(settings_module, "resolve_dotenv_example_path", lambda: None)

    loaded_path = settings_module.load_runtime_dotenv()
    settings = Settings.from_env()

    assert loaded_path == dotenv_path
    assert dotenv_path.exists()
    assert settings.default_service_origin == "BBVA México"
    assert settings.allowed_service_origin_n1["BBVA México"]


def test_frozen_runtime_bootstraps_dotenv_in_user_app_home(monkeypatch, tmp_path: Path) -> None:
    clear_dotenv_test_keys(monkeypatch)
    fake_home = tmp_path / "home"
    resource_root = tmp_path / "bundle"
    resource_root.mkdir(parents=True)
    (resource_root / ".env.example").write_text(
        "\n".join(
            [
                "NPS_LENS_SERVICE_ORIGIN_BUUG=BBVA México",
                'NPS_LENS_SERVICE_ORIGIN_N1={"BBVA México": ["ENTERPRISE WEB"]}',
                "NPS_LENS_DEFAULT_SERVICE_ORIGIN=BBVA México",
                "NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1=ENTERPRISE WEB",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.delenv("NPS_LENS_APP_HOME", raising=False)
    monkeypatch.delenv("NPS_LENS_DOTENV_PATH", raising=False)
    monkeypatch.setattr(settings_module.sys, "frozen", True, raising=False)
    monkeypatch.setattr(settings_module.sys, "_MEIPASS", str(resource_root), raising=False)

    loaded_path = settings_module.load_runtime_dotenv()
    settings = Settings.from_env()

    assert loaded_path == fake_home / ".nps-lens" / ".env"
    assert loaded_path.exists()
    assert settings.dotenv_path == loaded_path
    assert settings.allowed_service_origin_n1 == {"BBVA México": ["ENTERPRISE WEB"]}
