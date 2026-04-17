from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_origin_map(value: str) -> dict[str, list[str]]:
    raw = value.strip()
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
                output[normalized_key] = [str(item).strip() for item in items if str(item).strip()]
            else:
                output[normalized_key] = _split_csv(str(items))
        return output

    output = {}
    for chunk in [item.strip() for item in raw.split(";") if item.strip()]:
        if ":" not in chunk:
            continue
        key, payload = chunk.split(":", 1)
        normalized_key = key.strip()
        if not normalized_key:
            continue
        output[normalized_key] = [item.strip() for item in payload.split("|") if item.strip()]
    return output


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

    @staticmethod
    def from_env() -> "Settings":
        data_dir = Path(os.getenv("NPS_LENS_DATA_DIR", "./data")).expanduser()
        frontend_dist_dir = Path(
            os.getenv("NPS_LENS_FRONTEND_DIST_DIR", "./frontend/dist")
        ).expanduser()
        frontend_public_dir = Path(
            os.getenv("NPS_LENS_FRONTEND_PUBLIC_DIR", "./frontend/public")
        ).expanduser()
        database_path = Path(
            os.getenv("NPS_LENS_DATABASE_PATH", str(data_dir / "nps_lens.sqlite3"))
        ).expanduser()

        origins_raw = os.getenv(
            "NPS_LENS_SERVICE_ORIGIN_BUUG",
            os.getenv("NPS_LENS_SERVICE_ORIGIN", "BBVA México"),
        )
        allowed_service_origins = _split_csv(origins_raw) or ["BBVA México"]

        origin_n1_map = _parse_origin_map(
            os.getenv(
                "NPS_LENS_SERVICE_ORIGIN_N1",
                '{"BBVA México":["Senda"]}',
            )
        )
        if not origin_n1_map:
            origin_n1_map = {"BBVA México": ["Senda"]}

        default_service_origin = os.getenv(
            "NPS_LENS_DEFAULT_SERVICE_ORIGIN",
            allowed_service_origins[0],
        ).strip() or allowed_service_origins[0]
        default_origin_n1_candidates = origin_n1_map.get(default_service_origin) or ["Senda"]
        default_service_origin_n1 = os.getenv(
            "NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1",
            default_origin_n1_candidates[0],
        ).strip() or default_origin_n1_candidates[0]

        return Settings(
            data_dir=data_dir,
            database_path=database_path,
            frontend_dist_dir=frontend_dist_dir,
            frontend_public_dir=frontend_public_dir,
            api_host=os.getenv("NPS_LENS_API_HOST", "127.0.0.1").strip() or "127.0.0.1",
            api_port=int(os.getenv("NPS_LENS_API_PORT", "8000")),
            default_service_origin=default_service_origin,
            default_service_origin_n1=default_service_origin_n1,
            allowed_service_origins=allowed_service_origins,
            allowed_service_origin_n1=origin_n1_map,
            log_level=os.getenv("NPS_LENS_LOG_LEVEL", "INFO").strip().upper() or "INFO",
        )
