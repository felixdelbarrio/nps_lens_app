from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional

from dotenv import load_dotenv, set_key

DEFAULT_UI_THEME_MODE = "light"
DEFAULT_UI_TOUCHPOINT_SOURCE = "executive_journeys"
DEFAULT_UI_HELIX_BASE_URL = "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/"
DEFAULT_UI_MIN_SIMILARITY = 0.25
DEFAULT_UI_MAX_DAYS_APART = 10
DEFAULT_UI_MIN_N_OPPORTUNITIES = 200
DEFAULT_UI_MIN_N_CROSS_COMPARISONS = 30
DEFAULT_UI_NPS_GROUP = "Todos"
DEFAULT_UI_POP_VALUE = "Todos"

SERVICE_ORIGIN_N2_MAP_ENV_KEY = "NPS_LENS_SERVICE_ORIGIN_N2_MAP"

UI_PREF_ENV_KEYS = {
    "service_origin": "NPS_LENS_UI_SERVICE_ORIGIN",
    "service_origin_n1": "NPS_LENS_UI_SERVICE_ORIGIN_N1",
    "service_origin_n2": "NPS_LENS_UI_SERVICE_ORIGIN_N2",
    "pop_year": "NPS_LENS_UI_POP_YEAR",
    "pop_month": "NPS_LENS_UI_POP_MONTH",
    "nps_group_choice": "NPS_LENS_UI_NPS_GROUP",
    "theme_mode": "NPS_LENS_UI_THEME_MODE",
    "downloads_path": "NPS_LENS_UI_DOWNLOADS_PATH",
    "helix_base_url": "NPS_LENS_UI_HELIX_BASE_URL",
    "touchpoint_source": "NPS_LENS_UI_TOUCHPOINT_SOURCE",
    "min_similarity": "NPS_LENS_UI_MIN_SIMILARITY",
    "max_days_apart": "NPS_LENS_UI_MAX_DAYS_APART",
    "min_n_opportunities": "NPS_LENS_UI_MIN_N_OPPORTUNITIES",
    "min_n_cross_comparisons": "NPS_LENS_UI_MIN_N_CROSS_COMPARISONS",
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
        app_home_raw = str(os.getenv("NPS_LENS_APP_HOME", "")).strip()
        app_home = Path(app_home_raw).expanduser() if app_home_raw else (Path.home() / ".nps-lens")
        return app_home / candidate
    return candidate


def resolve_dotenv_path() -> Optional[Path]:
    explicit = str(os.getenv("NPS_LENS_DOTENV_PATH", "")).strip()
    if explicit:
        candidate = Path(explicit).expanduser()
        return candidate if candidate.exists() else candidate

    repo_root = Path(__file__).resolve().parents[2]
    candidates = [
        Path.cwd() / ".env",
        repo_root / ".env",
        repo_root / "app" / ".env",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return repo_root / ".env"


def load_runtime_dotenv(*, override: bool = False) -> Optional[Path]:
    dotenv_path = resolve_dotenv_path()
    if dotenv_path and dotenv_path.exists():
        load_dotenv(dotenv_path, override=override)
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
    raw = str(value or "").strip()
    candidate = Path(raw).expanduser() if raw else Path(default_downloads_path())
    if not candidate.is_absolute():
        candidate = (Path.home() / candidate).expanduser()
    candidate = candidate.resolve()
    if candidate.exists() and not candidate.is_dir():
        raise ValueError("La ruta de descargas debe apuntar a un directorio.")
    if create:
        candidate.mkdir(parents=True, exist_ok=True)
    return str(candidate)


def normalize_helix_base_url(value: object) -> str:
    raw = str(value or "").strip() or DEFAULT_UI_HELIX_BASE_URL
    if not raw.lower().startswith(("https://", "http://")):
        raise ValueError("La ruta base de Helix debe comenzar por http:// o https://.")
    return raw.rstrip("/") + "/"


def safe_normalize_downloads_path(value: object, fallback: object) -> str:
    try:
        return normalize_downloads_path(value)
    except (OSError, ValueError):
        return normalize_downloads_path(fallback)


def safe_normalize_helix_base_url(value: object, fallback: object) -> str:
    try:
        return normalize_helix_base_url(value)
    except ValueError:
        return normalize_helix_base_url(fallback)


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
            value = normalize_downloads_path(raw_value)
        elif str(name) == "helix_base_url":
            value = normalize_helix_base_url(raw_value)
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
    dotenv_path: Optional[Path] = None
    knowledge_dir: Path = Path("./knowledge")
    service_origin_n2_values: list[str] = field(default_factory=list)
    service_origin_n2_map: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    default_theme_mode: str = DEFAULT_UI_THEME_MODE
    default_touchpoint_source: str = DEFAULT_UI_TOUCHPOINT_SOURCE
    default_downloads_path: str = field(default_factory=default_downloads_path)
    default_helix_base_url: str = DEFAULT_UI_HELIX_BASE_URL
    default_min_similarity: float = DEFAULT_UI_MIN_SIMILARITY
    default_max_days_apart: int = DEFAULT_UI_MAX_DAYS_APART
    default_min_n_opportunities: int = DEFAULT_UI_MIN_N_OPPORTUNITIES
    default_min_n_cross_comparisons: int = DEFAULT_UI_MIN_N_CROSS_COMPARISONS

    @property
    def service_origin_values(self) -> list[str]:
        """Backward-compatible alias used by older callers and tests."""
        return self.allowed_service_origins

    @property
    def service_origin_n1_map(self) -> dict[str, list[str]]:
        """Backward-compatible alias used by older callers and tests."""
        return self.allowed_service_origin_n1

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

        origins_raw = os.getenv(
            "NPS_LENS_SERVICE_ORIGIN_BUUG",
            os.getenv("NPS_LENS_SERVICE_ORIGIN", "BBVA México"),
        )
        allowed_service_origins = (
            _parse_json_list(origins_raw) or _dedupe(_split_csv(origins_raw)) or ["BBVA México"]
        )

        origin_n1_raw = os.getenv("NPS_LENS_SERVICE_ORIGIN_N1", "")
        origin_n1_map = _parse_origin_map(origin_n1_raw)
        missing_env: list[str] = []
        if not str(origins_raw).strip() or not allowed_service_origins:
            missing_env.append("NPS_LENS_SERVICE_ORIGIN_BUUG")
        if not origin_n1_raw.strip() or not origin_n1_map:
            missing_env.append("NPS_LENS_SERVICE_ORIGIN_N1")
        incomplete = [origin for origin in allowed_service_origins if not origin_n1_map.get(origin)]
        if missing_env or incomplete:
            lines = ["Invalid .env configuration for context dimensions:"]
            if missing_env:
                lines.append("- Missing/empty required env var(s):")
                lines.extend([f"  - {name}" for name in missing_env])
            if incomplete:
                lines.append(
                    "- service_origin_n1 map is missing entries (or empty lists) for: "
                    + ", ".join(incomplete)
                )
            raise RuntimeError("\n".join(lines))

        service_origin_n2_values = _parse_json_list(
            os.getenv("NPS_LENS_SERVICE_ORIGIN_N2", "")
        ) or _dedupe(_split_csv(os.getenv("NPS_LENS_SERVICE_ORIGIN_N2", "")))
        service_origin_n2_map = _parse_origin_n2_map(os.getenv(SERVICE_ORIGIN_N2_MAP_ENV_KEY, ""))

        default_service_origin = (
            os.getenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN", allowed_service_origins[0]).strip()
            or allowed_service_origins[0]
        )
        if default_service_origin not in allowed_service_origins:
            default_service_origin = allowed_service_origins[0]

        default_n1_candidates = origin_n1_map.get(default_service_origin) or ["ENTERPRISE WEB"]
        default_service_origin_n1 = (
            os.getenv("NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1", default_n1_candidates[0]).strip()
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
        default_min_n_opportunities = max(
            50,
            _to_int(
                os.getenv(
                    "NPS_LENS_UI_MIN_N_OPPORTUNITIES",
                    str(DEFAULT_UI_MIN_N_OPPORTUNITIES),
                ),
                DEFAULT_UI_MIN_N_OPPORTUNITIES,
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
            dotenv_path=resolve_dotenv_path(),
            knowledge_dir=knowledge_dir,
            service_origin_n2_values=service_origin_n2_values,
            service_origin_n2_map=service_origin_n2_map,
            default_theme_mode=default_theme_mode,
            default_touchpoint_source=default_touchpoint_source,
            default_downloads_path=default_downloads_dir,
            default_helix_base_url=default_helix_base_url,
            default_min_similarity=default_min_similarity,
            default_max_days_apart=default_max_days_apart,
            default_min_n_opportunities=default_min_n_opportunities,
            default_min_n_cross_comparisons=default_min_n_cross_comparisons,
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
        min_n_opportunities = max(
            50,
            _to_int(
                ui_pref("min_n_opportunities", str(self.default_min_n_opportunities)),
                self.default_min_n_opportunities,
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
            "theme_mode": theme_mode,
            "downloads_path": downloads_path,
            "helix_base_url": helix_base_url,
            "touchpoint_source": touchpoint_source,
            "min_similarity": min_similarity,
            "max_days_apart": max_days_apart,
            "min_n_opportunities": min_n_opportunities,
            "min_n_cross_comparisons": min_n_cross_comparisons,
        }
