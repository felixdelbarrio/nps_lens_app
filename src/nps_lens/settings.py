from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional
from urllib.parse import urlsplit

from dotenv import dotenv_values, load_dotenv, set_key

from nps_lens.platform.resources import resource_root

DEFAULT_UI_THEME_MODE = "light"
DEFAULT_UI_TOUCHPOINT_SOURCE = "executive_journeys"
DEFAULT_UI_REPORT_DIMENSION_ANALYSIS = "palanca"
DEFAULT_UI_HELIX_BASE_URL = "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/"
DEFAULT_UI_MIN_SIMILARITY = 0.15
DEFAULT_UI_MAX_DAYS_APART = 90
DEFAULT_UI_MIN_N_NPS_GAPS = 200
DEFAULT_UI_MIN_N_CROSS_COMPARISONS = 30
DEFAULT_UI_NPS_GROUP = "Detractores"
DEFAULT_UI_SCORE_CHANNEL = "Web"
DEFAULT_UI_POP_VALUE = "Todos"
DEFAULT_TAXONOMY_DISCOVERY_METHOD = "local"
DEFAULT_TAXONOMY_DESIGNER_URL = "https://chatgpt.com/g/g-p-6aaabb05fd0881a49965c28ef21333a9"
DEFAULT_TAXONOMY_CLASSIFIER_URL = "https://chatgpt.com/g/g-p-6aaab79eb94481a498b0b2bb6ba2cb2a"
DEFAULT_SERVICE_ORIGINS = [
    "BBVA México",
    "BBVA España",
    "BBVA Colombia",
    "BBVA Perú",
    "BBVA Argentina",
]
DEFAULT_SERVICE_ORIGIN = DEFAULT_SERVICE_ORIGINS[0]
DEFAULT_SERVICE_ORIGIN_N1 = "ENTERPRISE WEB"
DEFAULT_SERVICE_ORIGIN_N1_MAP = {
    "BBVA México": ["ENTERPRISE WEB", "MOBILE ENTERPRISE"],
    "BBVA España": ["ENTERPRISE MOBILE CHANNEL", "ENTERPRISES CHANNEL"],
    "BBVA Colombia": ["ENTERPRISE MOBILE (GEMA)", "ENTERPRISE WEB CHANNEL"],
    "BBVA Perú": ["ENTERPRISE BANKING CANALES WEB & MOBILE"],
    "BBVA Argentina": ["AR44 PLATAFORMA SENDA ARG", "AR46 GEMA ARG"],
}
BOOTSTRAP_CONTEXT_ENV_KEYS = {
    "NPS_LENS_SERVICE_ORIGIN_BUUG",
    "NPS_LENS_SERVICE_ORIGIN_N1",
    "NPS_LENS_DEFAULT_SERVICE_ORIGIN",
    "NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1",
}
SERVICE_ORIGIN_N2_MAP_ENV_KEY = "NPS_LENS_SERVICE_ORIGIN_N2_MAP"
UI_PREF_ENV_KEYS = {
    "service_origin": "NPS_LENS_UI_SERVICE_ORIGIN",
    "service_origin_n1": "NPS_LENS_UI_SERVICE_ORIGIN_N1",
    "service_origin_n2": "NPS_LENS_UI_SERVICE_ORIGIN_N2",
    "pop_year": "NPS_LENS_UI_POP_YEAR",
    "pop_month": "NPS_LENS_UI_POP_MONTH",
    "nps_group_choice": "NPS_LENS_UI_NPS_GROUP",
    "score_channel": "NPS_LENS_UI_SCORE_CHANNEL",
    "theme_mode": "NPS_LENS_UI_THEME_MODE",
    "downloads_path": "NPS_LENS_UI_DOWNLOADS_PATH",
    "helix_base_url": "NPS_LENS_UI_HELIX_BASE_URL",
    "report_dimension_analysis": "NPS_LENS_UI_REPORT_DIMENSION_ANALYSIS",
    "touchpoint_source": "NPS_LENS_UI_TOUCHPOINT_SOURCE",
    "min_similarity": "NPS_LENS_UI_MIN_SIMILARITY",
    "max_days_apart": "NPS_LENS_UI_MAX_DAYS_APART",
    "min_n_nps_gaps": "NPS_LENS_UI_MIN_N_NPS_GAPS",
    "min_n_cross_comparisons": "NPS_LENS_UI_MIN_N_CROSS_COMPARISONS",
    "taxonomy_discovery_method": "NPS_LENS_TAXONOMY_DISCOVERY_METHOD",
    "taxonomy_designer_url": "NPS_LENS_TAXONOMY_DESIGNER_URL",
    "taxonomy_classifier_url": "NPS_LENS_TAXONOMY_CLASSIFIER_URL",
}


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in values:
        normalized = str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)
    return output


def _parse_json_list(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return _dedupe([str(item).strip() for item in parsed if str(item).strip()])


def _parse_origin_map(value: str) -> dict[str, list[str]]:
    raw = str(value or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, dict):
        output: dict[str, list[str]] = {}
        for key, items in parsed.items():
            normalized_key = str(key).strip()
            if not normalized_key:
                continue
            if isinstance(items, list):
                output[normalized_key] = _dedupe(
                    [str(item).strip() for item in items if str(item).strip()]
                )
            else:
                output[normalized_key] = _dedupe(_split_csv(str(items)))
        return output
    output = {}
    for chunk in [item.strip() for item in raw.split(";") if item.strip()]:
        if ":" not in chunk:
            continue
        key, payload = chunk.split(":", 1)
        normalized_key = key.strip()
        if not normalized_key:
            continue
        output[normalized_key] = _dedupe(
            [item.strip() for item in payload.split("|") if item.strip()]
        )
    return output


def _parse_origin_n2_map(value: str) -> dict[str, dict[str, list[str]]]:
    raw = str(value or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    output: dict[str, dict[str, list[str]]] = {}
    for origin_key, origin_value in parsed.items():
        origin = str(origin_key).strip()
        if not origin or not isinstance(origin_value, dict):
            continue
        n1_map: dict[str, list[str]] = {}
        for n1_key, n2_values in origin_value.items():
            n1 = str(n1_key).strip()
            if not n1:
                continue
            normalized_values = (
                _dedupe([str(item).strip() for item in n2_values if str(item).strip()])
                if isinstance(n2_values, list)
                else _dedupe(_split_csv(str(n2_values)))
            )
            n1_map[n1] = normalized_values
        output[origin] = n1_map
    return output


def _resolve_runtime_dir(env_key: str, default_rel: str) -> Path:
    raw = str(os.getenv(env_key, default_rel)).strip() or default_rel
    candidate = Path(raw).expanduser()
    if candidate.is_absolute():
        return candidate
    if getattr(sys, "frozen", False):
        return _runtime_app_home() / candidate
    return candidate


def _runtime_app_home() -> Path:
    app_home_raw = str(os.getenv("NPS_LENS_APP_HOME", "")).strip()
    return Path(app_home_raw).expanduser() if app_home_raw else (Path.home() / ".nps-lens")


def resolve_dotenv_path() -> Optional[Path]:
    explicit = str(os.getenv("NPS_LENS_DOTENV_PATH", "")).strip()
    if explicit:
        candidate = Path(explicit).expanduser()
        return candidate if candidate.exists() else candidate
    if getattr(sys, "frozen", False):
        return _runtime_app_home() / ".env"

    repo_root = resource_root()
    candidates = [
        Path.cwd() / ".env",
        repo_root / ".env",
        repo_root / "app" / ".env",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return repo_root / ".env"


def resolve_dotenv_example_path() -> Optional[Path]:
    candidates = [
        resource_root() / ".env.example",
        Path.cwd() / ".env.example",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _env_template_values(dotenv_example_path: Optional[Path]) -> dict[str, str]:
    if dotenv_example_path is None or not dotenv_example_path.exists():
        return {}
    values = dotenv_values(dotenv_example_path)
    return {
        str(key): str(value)
        for key, value in values.items()
        if key is not None and value is not None
    }


def _should_bootstrap_value(env_key: str, current_value: Optional[str]) -> bool:
    if current_value is None:
        return True
    return env_key in BOOTSTRAP_CONTEXT_ENV_KEYS and not str(current_value).strip()


def ensure_runtime_dotenv(dotenv_path: Optional[Path]) -> Optional[Path]:
    if dotenv_path is None:
        return None
    dotenv_example_path = resolve_dotenv_example_path()
    template_values = _env_template_values(dotenv_example_path)
    try:
        dotenv_path.parent.mkdir(parents=True, exist_ok=True)
        if not dotenv_path.exists():
            if dotenv_example_path and dotenv_example_path.exists():
                dotenv_path.write_text(
                    dotenv_example_path.read_text(encoding="utf-8"), encoding="utf-8"
                )
            else:
                dotenv_path.touch()
            return dotenv_path
        if not template_values:
            return dotenv_path

        current_values = dotenv_values(dotenv_path)
        for env_key, template_value in template_values.items():
            current_value = current_values.get(env_key)
            if _should_bootstrap_value(env_key, current_value):
                set_key(str(dotenv_path), env_key, template_value, quote_mode="auto")
    except OSError:
        return dotenv_path
    return dotenv_path


def load_runtime_dotenv(*, override: bool = False) -> Optional[Path]:
    dotenv_path = ensure_runtime_dotenv(resolve_dotenv_path())
    if dotenv_path and dotenv_path.exists():
        load_dotenv(dotenv_path, override=override)
    dotenv_example_path = resolve_dotenv_example_path()
    if dotenv_example_path and dotenv_example_path.exists() and dotenv_example_path != dotenv_path:
        load_dotenv(dotenv_example_path, override=False)
    return dotenv_path


def _to_float(value: str, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value: str, default: int) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def ui_pref(name: str, default: str = "") -> str:
    env_key = UI_PREF_ENV_KEYS.get(name)
    if not env_key:
        return default
    value = os.getenv(env_key)
    return str(value).strip() if value is not None else default


def default_downloads_path() -> str:
    return str((Path.home() / "Downloads").expanduser().resolve())


def normalize_downloads_path(value: object, *, create: bool = False) -> str:
    """Normalize a user-selected download path without touching the filesystem.

    Values reaching this function can originate in an HTTP request.  Keeping this
    function purely lexical prevents request-controlled data from reaching filesystem
    operations during preference validation (the CodeQL ``py/path-injection`` flow).
    Actual directory creation is performed later by the download persistence layer.

    ``create`` is retained for API compatibility; directory creation is intentionally
    not performed here.
    """

    safe_root = Path.home().expanduser()
    raw = str(value or "").strip() or str(safe_root / "Downloads")
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = safe_root / candidate

    # Lexically collapse '.' and '..' without resolving symlinks or accessing disk.
    candidate_str = os.path.abspath(os.path.normpath(str(candidate)))
    safe_root_str = os.path.abspath(os.path.normpath(str(safe_root)))
    try:
        common = os.path.commonpath([safe_root_str, candidate_str])
    except ValueError as exc:
        raise ValueError(
            "La ruta de descargas debe estar dentro del directorio personal del usuario."
        ) from exc
    if os.path.normcase(common) != os.path.normcase(safe_root_str):
        raise ValueError(
            "La ruta de descargas debe estar dentro del directorio personal del usuario."
        )

    return candidate_str


def normalize_helix_base_url(value: object) -> str:
    raw = str(value or "").strip() or DEFAULT_UI_HELIX_BASE_URL
    if not raw.lower().startswith(("https://", "http://")):
        raise ValueError("La ruta base de Helix debe comenzar por http:// o https://.")
    return raw.rstrip("/") + "/"


def safe_normalize_downloads_path(value: object, fallback: object) -> str:
    """Normalize a UI downloads preference while preserving a trusted configured default.

    ``fallback`` comes from application configuration and may legitimately live outside
    the interactive user's home directory (for example pytest's ``tmp_path`` or a
    deployment-mounted directory). If the requested value is exactly that configured
    fallback, accept it after lexical normalization.

    Any different value is treated as user-controlled input and must satisfy
    ``normalize_downloads_path()``, which confines it to the user's home directory.
    """

    safe_root = Path.home().expanduser()

    def _lexical(value_to_normalize: object) -> str:
        raw = str(value_to_normalize or "").strip() or str(safe_root / "Downloads")
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = safe_root / candidate
        return os.path.abspath(os.path.normpath(str(candidate)))

    fallback_path = _lexical(fallback)
    requested_path = _lexical(value)

    if os.path.normcase(requested_path) == os.path.normcase(fallback_path):
        return fallback_path

    try:
        return normalize_downloads_path(value)
    except (OSError, ValueError):
        return fallback_path


def safe_normalize_helix_base_url(value: object, fallback: object) -> str:
    try:
        return normalize_helix_base_url(value)
    except ValueError:
        return normalize_helix_base_url(fallback)


def normalize_report_dimension_analysis(value: object) -> str:
    raw = str(value or "").strip().lower()
    return raw if raw in {"palanca", "subpalanca"} else DEFAULT_UI_REPORT_DIMENSION_ANALYSIS


def normalize_taxonomy_discovery_method(value: object) -> str:
    raw = str(value or "").strip().lower()
    if raw not in {"local", "chatgpt_browser"}:
        raise ValueError("Método de descubrimiento desconocido.")
    return raw


def normalize_chatgpt_project_url(value: object) -> str:
    raw = str(value or "").strip()
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("La URL de proyecto de ChatGPT no es válida.") from exc

    if (
        parsed.scheme.casefold() != "https"
        or (parsed.hostname or "").casefold() != "chatgpt.com"
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or not parsed.path.startswith("/g/")
    ):
        raise ValueError("La URL debe ser un proyecto HTTPS de https://chatgpt.com/g/.")
    return raw


def persist_ui_prefs(dotenv_path: Optional[Path], values: Mapping[str, object]) -> None:
    if dotenv_path is None:
        return
    dotenv_path.parent.mkdir(parents=True, exist_ok=True)
    if not dotenv_path.exists():
        dotenv_path.touch()
    for name, raw_value in values.items():
        env_key = UI_PREF_ENV_KEYS.get(str(name))
        if not env_key:
            continue
        if str(name) == "downloads_path":
            # Callers validate this preference before persistence.  Do not route a
            # generic Mapping value through filesystem/path operations here: doing so
            # makes unrelated request fields (for example taxonomy URLs) appear to
            # CodeQL as possible path inputs.
            value = str(raw_value).strip()
            if not value:
                raise ValueError("La ruta de descargas no puede estar vacía.")
        elif str(name) == "helix_base_url":
            value = normalize_helix_base_url(raw_value)
        elif str(name) == "report_dimension_analysis":
            value = normalize_report_dimension_analysis(raw_value)
        elif str(name) == "taxonomy_discovery_method":
            value = normalize_taxonomy_discovery_method(raw_value)
        elif str(name) in {"taxonomy_designer_url", "taxonomy_classifier_url"}:
            value = normalize_chatgpt_project_url(raw_value)
        else:
            value = str(raw_value)
        os.environ[env_key] = value
        set_key(str(dotenv_path), env_key, value, quote_mode="auto")


def persist_service_origin_hierarchy(
    dotenv_path: Optional[Path],
    *,
    service_origins: list[str],
    service_origin_n1_map: dict[str, list[str]],
    service_origin_n2_map: dict[str, dict[str, list[str]]],
    default_service_origin: str,
    default_service_origin_n1: str,
) -> None:
    if dotenv_path is None:
        return
    dotenv_path.parent.mkdir(parents=True, exist_ok=True)
    if not dotenv_path.exists():
        dotenv_path.touch()
    payloads = {
        "NPS_LENS_SERVICE_ORIGIN_BUUG": ", ".join(service_origins),
        "NPS_LENS_SERVICE_ORIGIN_N1": json.dumps(service_origin_n1_map, ensure_ascii=False),
        SERVICE_ORIGIN_N2_MAP_ENV_KEY: json.dumps(service_origin_n2_map, ensure_ascii=False),
        "NPS_LENS_DEFAULT_SERVICE_ORIGIN": default_service_origin,
        "NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1": default_service_origin_n1,
    }
    for env_key, raw_value in payloads.items():
        value = str(raw_value)
        os.environ[env_key] = value
        set_key(str(dotenv_path), env_key, value, quote_mode="auto")


def _fallback_n1_values(service_origin: str) -> list[str]:
    configured = DEFAULT_SERVICE_ORIGIN_N1_MAP.get(str(service_origin).strip())
    return list(configured) if configured else [DEFAULT_SERVICE_ORIGIN_N1]


def _complete_origin_n1_map(
    service_origins: list[str], origin_n1_map: dict[str, list[str]]
) -> dict[str, list[str]]:
    completed: dict[str, list[str]] = {}
    for origin in service_origins:
        values = origin_n1_map.get(origin) or _fallback_n1_values(origin)
        completed[origin] = _dedupe(values) or _fallback_n1_values(origin)
    return completed


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    database_path: Path
    frontend_dist_dir: Path
    frontend_public_dir: Path
    api_host: str
    api_port: int
    default_service_origin: str
    default_service_origin_n1: str
    allowed_service_origins: list[str]
    allowed_service_origin_n1: dict[str, list[str]]
    log_level: str
    auth_mode: str = "local"
    allowed_email_domain: str = "bbva.com"
    admin_emails: tuple[str, ...] = ()
    dotenv_path: Optional[Path] = None
    knowledge_dir: Path = Path("./knowledge")
    equivalences_path: Path = Path("./data/config/equivalences.json")
    column_aliases_path: Path = Path("./data/config/nps_column_aliases.json")
    service_origin_n2_values: list[str] = field(default_factory=list)
    service_origin_n2_map: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    default_theme_mode: str = DEFAULT_UI_THEME_MODE
    default_touchpoint_source: str = DEFAULT_UI_TOUCHPOINT_SOURCE
    default_report_dimension_analysis: str = DEFAULT_UI_REPORT_DIMENSION_ANALYSIS
    default_downloads_path: str = field(default_factory=default_downloads_path)
    default_helix_base_url: str = DEFAULT_UI_HELIX_BASE_URL
    default_min_similarity: float = DEFAULT_UI_MIN_SIMILARITY
    default_max_days_apart: int = DEFAULT_UI_MAX_DAYS_APART
    default_min_n_nps_gaps: int = DEFAULT_UI_MIN_N_NPS_GAPS
    default_min_n_cross_comparisons: int = DEFAULT_UI_MIN_N_CROSS_COMPARISONS
    taxonomy_discovery_method: str = DEFAULT_TAXONOMY_DISCOVERY_METHOD
    taxonomy_designer_url: str = DEFAULT_TAXONOMY_DESIGNER_URL
    taxonomy_classifier_url: str = DEFAULT_TAXONOMY_CLASSIFIER_URL
    taxonomy_batch_size: int = 500

    @staticmethod
    def from_env() -> "Settings":
        data_dir = _resolve_runtime_dir("NPS_LENS_DATA_DIR", "./data")
        database_path = Path(
            os.getenv("NPS_LENS_DATABASE_PATH", str(data_dir / "nps_lens.sqlite3"))
        ).expanduser()
        frontend_dist_dir = Path(
            os.getenv("NPS_LENS_FRONTEND_DIST_DIR", "./frontend/dist")
        ).expanduser()
        frontend_public_dir = Path(
            os.getenv("NPS_LENS_FRONTEND_PUBLIC_DIR", "./frontend/public")
        ).expanduser()
        knowledge_dir = _resolve_runtime_dir("NPS_LENS_KNOWLEDGE_DIR", "./knowledge")
        equivalences_path = Path(
            os.getenv(
                "NPS_LENS_EQUIVALENCES_PATH",
                str(data_dir / "config" / "equivalences.json"),
            )
        ).expanduser()
        column_aliases_path = Path(
            os.getenv(
                "NPS_LENS_COLUMN_ALIASES_PATH",
                str(data_dir / "config" / "nps_column_aliases.json"),
            )
        ).expanduser()
        origins_raw = os.getenv(
            "NPS_LENS_SERVICE_ORIGIN_BUUG",
            os.getenv("NPS_LENS_SERVICE_ORIGIN", ", ".join(DEFAULT_SERVICE_ORIGINS)),
        )
        allowed_service_origins = (
            _parse_json_list(origins_raw)
            or _dedupe(_split_csv(origins_raw))
            or DEFAULT_SERVICE_ORIGINS
        )
        origin_n1_raw = os.getenv("NPS_LENS_SERVICE_ORIGIN_N1", "")
        origin_n1_map = _complete_origin_n1_map(
            allowed_service_origins,
            _parse_origin_map(origin_n1_raw),
        )

        service_origin_n2_values = _parse_json_list(
            os.getenv("NPS_LENS_SERVICE_ORIGIN_N2", "")
        ) or _dedupe(_split_csv(os.getenv("NPS_LENS_SERVICE_ORIGIN_N2", "")))
        service_origin_n2_map = _parse_origin_n2_map(os.getenv(SERVICE_ORIGIN_N2_MAP_ENV_KEY, ""))
        default_service_origin = (
            os.getenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN", DEFAULT_SERVICE_ORIGIN).strip()
            or allowed_service_origins[0]
        )
        if default_service_origin not in allowed_service_origins:
            default_service_origin = allowed_service_origins[0]
        default_n1_candidates = origin_n1_map.get(default_service_origin) or _fallback_n1_values(
            default_service_origin
        )
        default_service_origin_n1 = (
            os.getenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1", DEFAULT_SERVICE_ORIGIN_N1).strip()
            or default_n1_candidates[0]
        )
        if default_service_origin_n1 not in default_n1_candidates:
            default_service_origin_n1 = default_n1_candidates[0]
        default_theme_mode = (
            os.getenv("NPS_LENS_UI_THEME_MODE", DEFAULT_UI_THEME_MODE).strip().lower()
            or DEFAULT_UI_THEME_MODE
        )
        if default_theme_mode not in {"light", "dark"}:
            default_theme_mode = DEFAULT_UI_THEME_MODE
        default_touchpoint_source = (
            os.getenv("NPS_LENS_UI_TOUCHPOINT_SOURCE", DEFAULT_UI_TOUCHPOINT_SOURCE).strip()
            or DEFAULT_UI_TOUCHPOINT_SOURCE
        )
        default_report_dimension_analysis = normalize_report_dimension_analysis(
            os.getenv(
                "NPS_LENS_UI_REPORT_DIMENSION_ANALYSIS",
                DEFAULT_UI_REPORT_DIMENSION_ANALYSIS,
            )
        )
        default_downloads_dir = safe_normalize_downloads_path(
            os.getenv("NPS_LENS_UI_DOWNLOADS_PATH", default_downloads_path()),
            default_downloads_path(),
        )
        default_helix_base_url = safe_normalize_helix_base_url(
            os.getenv("NPS_LENS_UI_HELIX_BASE_URL", DEFAULT_UI_HELIX_BASE_URL),
            DEFAULT_UI_HELIX_BASE_URL,
        )
        default_min_similarity = min(
            max(
                _to_float(
                    os.getenv("NPS_LENS_UI_MIN_SIMILARITY", str(DEFAULT_UI_MIN_SIMILARITY)),
                    DEFAULT_UI_MIN_SIMILARITY,
                ),
                0.0,
            ),
            1.0,
        )
        default_max_days_apart = max(
            0,
            _to_int(
                os.getenv("NPS_LENS_UI_MAX_DAYS_APART", str(DEFAULT_UI_MAX_DAYS_APART)),
                DEFAULT_UI_MAX_DAYS_APART,
            ),
        )
        default_min_n_nps_gaps = max(
            50,
            _to_int(
                os.getenv(
                    "NPS_LENS_UI_MIN_N_NPS_GAPS",
                    str(DEFAULT_UI_MIN_N_NPS_GAPS),
                ),
                DEFAULT_UI_MIN_N_NPS_GAPS,
            ),
        )
        default_min_n_cross_comparisons = min(
            max(
                _to_int(
                    os.getenv(
                        "NPS_LENS_UI_MIN_N_CROSS_COMPARISONS",
                        str(DEFAULT_UI_MIN_N_CROSS_COMPARISONS),
                    ),
                    DEFAULT_UI_MIN_N_CROSS_COMPARISONS,
                ),
                10,
            ),
            200,
        )
        try:
            taxonomy_discovery_method = normalize_taxonomy_discovery_method(
                os.getenv("NPS_LENS_TAXONOMY_DISCOVERY_METHOD", DEFAULT_TAXONOMY_DISCOVERY_METHOD)
            )
        except ValueError:
            taxonomy_discovery_method = DEFAULT_TAXONOMY_DISCOVERY_METHOD
        try:
            taxonomy_designer_url = normalize_chatgpt_project_url(
                os.getenv("NPS_LENS_TAXONOMY_DESIGNER_URL", DEFAULT_TAXONOMY_DESIGNER_URL)
            )
        except ValueError:
            taxonomy_designer_url = DEFAULT_TAXONOMY_DESIGNER_URL
        try:
            taxonomy_classifier_url = normalize_chatgpt_project_url(
                os.getenv("NPS_LENS_TAXONOMY_CLASSIFIER_URL", DEFAULT_TAXONOMY_CLASSIFIER_URL)
            )
        except ValueError:
            taxonomy_classifier_url = DEFAULT_TAXONOMY_CLASSIFIER_URL
        taxonomy_batch_size = min(
            max(_to_int(os.getenv("NPS_LENS_TAXONOMY_BATCH_SIZE", "500"), 500), 50), 2000
        )
        return Settings(
            data_dir=data_dir,
            database_path=database_path,
            frontend_dist_dir=frontend_dist_dir,
            frontend_public_dir=frontend_public_dir,
            api_host=os.getenv("NPS_LENS_API_HOST", "127.0.0.1").strip() or "127.0.0.1",
            api_port=_to_int(os.getenv("NPS_LENS_API_PORT", "8000"), 8000),
            default_service_origin=default_service_origin,
            default_service_origin_n1=default_service_origin_n1,
            allowed_service_origins=allowed_service_origins,
            allowed_service_origin_n1=origin_n1_map,
            log_level=os.getenv("NPS_LENS_LOG_LEVEL", "INFO").strip().upper() or "INFO",
            auth_mode=os.getenv("NPS_LENS_AUTH_MODE", "local").strip().lower() or "local",
            allowed_email_domain=(
                os.getenv("NPS_LENS_ALLOWED_EMAIL_DOMAIN", "bbva.com").strip().lower().lstrip("@")
                or "bbva.com"
            ),
            admin_emails=tuple(
                value.casefold()
                for value in _dedupe(_split_csv(os.getenv("NPS_LENS_ADMIN_EMAILS", "")))
            ),
            dotenv_path=resolve_dotenv_path(),
            knowledge_dir=knowledge_dir,
            equivalences_path=equivalences_path,
            column_aliases_path=column_aliases_path,
            service_origin_n2_values=service_origin_n2_values,
            service_origin_n2_map=service_origin_n2_map,
            default_theme_mode=default_theme_mode,
            default_touchpoint_source=default_touchpoint_source,
            default_report_dimension_analysis=default_report_dimension_analysis,
            default_downloads_path=default_downloads_dir,
            default_helix_base_url=default_helix_base_url,
            default_min_similarity=default_min_similarity,
            default_max_days_apart=default_max_days_apart,
            default_min_n_nps_gaps=default_min_n_nps_gaps,
            default_min_n_cross_comparisons=default_min_n_cross_comparisons,
            taxonomy_discovery_method=taxonomy_discovery_method,
            taxonomy_designer_url=taxonomy_designer_url,
            taxonomy_classifier_url=taxonomy_classifier_url,
            taxonomy_batch_size=taxonomy_batch_size,
        )

    def service_origin_n2_options(self, service_origin: str, service_origin_n1: str) -> list[str]:
        origin = str(service_origin or "").strip()
        n1 = str(service_origin_n1 or "").strip()
        mapped = self.service_origin_n2_map.get(origin, {}).get(n1, [])
        if mapped:
            return mapped
        return self.service_origin_n2_values

    def ui_defaults(self) -> dict[str, object]:
        default_service_origin = ui_pref("service_origin", self.default_service_origin)
        if default_service_origin not in self.allowed_service_origins:
            default_service_origin = self.default_service_origin
        available_n1 = self.allowed_service_origin_n1.get(default_service_origin) or [
            self.default_service_origin_n1
        ]
        default_service_origin_n1 = ui_pref("service_origin_n1", self.default_service_origin_n1)
        if default_service_origin_n1 not in available_n1:
            default_service_origin_n1 = available_n1[0]
        theme_mode = (
            ui_pref("theme_mode", self.default_theme_mode).lower() or self.default_theme_mode
        )
        if theme_mode not in {"light", "dark"}:
            theme_mode = self.default_theme_mode
        touchpoint_source = (
            ui_pref("touchpoint_source", self.default_touchpoint_source)
            or self.default_touchpoint_source
        )
        report_dimension_analysis = normalize_report_dimension_analysis(
            ui_pref("report_dimension_analysis", self.default_report_dimension_analysis)
        )
        downloads_path = safe_normalize_downloads_path(
            ui_pref("downloads_path", self.default_downloads_path),
            self.default_downloads_path,
        )
        helix_base_url = safe_normalize_helix_base_url(
            ui_pref("helix_base_url", self.default_helix_base_url),
            self.default_helix_base_url,
        )
        min_similarity = min(
            max(
                _to_float(
                    ui_pref("min_similarity", f"{self.default_min_similarity:.2f}"),
                    self.default_min_similarity,
                ),
                0.0,
            ),
            1.0,
        )
        max_days_apart = max(
            0,
            _to_int(
                ui_pref("max_days_apart", str(self.default_max_days_apart)),
                self.default_max_days_apart,
            ),
        )
        min_n_nps_gaps = max(
            50,
            _to_int(
                ui_pref("min_n_nps_gaps", str(self.default_min_n_nps_gaps)),
                self.default_min_n_nps_gaps,
            ),
        )
        min_n_cross_comparisons = min(
            max(
                _to_int(
                    ui_pref(
                        "min_n_cross_comparisons",
                        str(self.default_min_n_cross_comparisons),
                    ),
                    self.default_min_n_cross_comparisons,
                ),
                10,
            ),
            200,
        )
        return {
            "service_origin": default_service_origin,
            "service_origin_n1": default_service_origin_n1,
            "service_origin_n2": ui_pref("service_origin_n2", ""),
            "pop_year": ui_pref("pop_year", DEFAULT_UI_POP_VALUE) or DEFAULT_UI_POP_VALUE,
            "pop_month": ui_pref("pop_month", DEFAULT_UI_POP_VALUE) or DEFAULT_UI_POP_VALUE,
            "nps_group_choice": ui_pref("nps_group_choice", DEFAULT_UI_NPS_GROUP)
            or DEFAULT_UI_NPS_GROUP,
            "score_channel": ui_pref("score_channel", DEFAULT_UI_SCORE_CHANNEL)
            or DEFAULT_UI_SCORE_CHANNEL,
            "theme_mode": theme_mode,
            "downloads_path": downloads_path,
            "helix_base_url": helix_base_url,
            "report_dimension_analysis": report_dimension_analysis,
            "touchpoint_source": touchpoint_source,
            "min_similarity": min_similarity,
            "max_days_apart": max_days_apart,
            "min_n_nps_gaps": min_n_nps_gaps,
            "min_n_cross_comparisons": min_n_cross_comparisons,
            "taxonomy_discovery_method": self.taxonomy_discovery_method,
            "taxonomy_designer_url": self.taxonomy_designer_url,
            "taxonomy_classifier_url": self.taxonomy_classifier_url,
        }
