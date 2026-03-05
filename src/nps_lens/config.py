from __future__ import annotations

import os
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    knowledge_dir: Path

    # Context dimensions (UI + dataset partitioning)
    default_service_origin: str
    default_service_origin_n1: str

    # Allowed values are sourced ONLY from .env (or process env). Advanced users can
    # extend them manually by editing the .env file.
    service_origin_values: List[str]
    # Mapping service_origin -> list of allowed n1 values
    service_origin_n1_map: Dict[str, List[str]]
    # Allowed n2 tokens (optional). If empty, UI can fall back to free text.
    service_origin_n2_values: List[str]

    log_level: str

    @staticmethod
    def from_env() -> "Settings":
        data_dir = Path(os.getenv("NPS_LENS_DATA_DIR", "./data"))
        knowledge_dir = Path(os.getenv("NPS_LENS_KNOWLEDGE_DIR", "./knowledge"))

        def _get_env(*names: str) -> str:
            for n in names:
                v = os.getenv(n)
                if v is not None and str(v).strip() != "":
                    return str(v)
            return ""

        def _split_csv(v: str) -> List[str]:
            return [p.strip() for p in (v or "").split(",") if p.strip()]

        def _parse_json_list(v: str) -> List[str]:
            try:
                obj = json.loads(v)
            except Exception:
                return []
            if isinstance(obj, list):
                return [str(x).strip() for x in obj if str(x).strip()]
            return []

        def _parse_n1_map(v: str) -> Dict[str, List[str]]:
            """Parse service_origin_n1 mapping.

            Supports:
              1) JSON dict: {"BBVA México": ["Senda", "..."], "BBVA España": ["..."]}
              2) Compact string: "BBVA México:Senda|Helix;BBVA España:Senda"
            """
            v = (v or "").strip()
            if not v:
                return {}
            # JSON first
            try:
                obj = json.loads(v)
                if isinstance(obj, dict):
                    out: Dict[str, List[str]] = {}
                    for k, vals in obj.items():
                        key = str(k).strip()
                        if not key:
                            continue
                        if isinstance(vals, list):
                            # JSON list is already structured; never re-parse its string representation.
                            out[key] = [str(x).strip() for x in vals if str(x).strip()]
                        else:
                            out[key] = _split_csv(str(vals))
                    return out
            except Exception:
                pass

            # Fallback compact format
            out2: Dict[str, List[str]] = {}
            for chunk in [c.strip() for c in v.split(";") if c.strip()]:
                if ":" not in chunk:
                    continue
                k, rest = chunk.split(":", 1)
                key = k.strip()
                if not key:
                    continue
                vals = [p.strip() for p in rest.split("|") if p.strip()] if "|" in rest else _split_csv(rest)
                out2[key] = vals
            return out2

        # ------------- Context options sourced from .env -------------
        # The user explicitly wants these to be controlled via .env:
        # - service_origin_buug (legacy upstream name) => service_origin_values
        # - service_origin_n1 mapping
        # - service_origin_n2 token list (optional)

        so_raw = _get_env(
            "NPS_LENS_SERVICE_ORIGIN_BUUG",
            "SERVICE_ORIGIN_BUUG",
            "NPS_LENS_SERVICE_ORIGIN",
            "SERVICE_ORIGIN",
        )
        service_origin_values = _parse_json_list(so_raw) or _split_csv(so_raw)

        n1_raw = _get_env(
            "NPS_LENS_SERVICE_ORIGIN_N1",
            "SERVICE_ORIGIN_N1",
        )
        service_origin_n1_map = _parse_n1_map(n1_raw)

        n2_raw = _get_env(
            "NPS_LENS_SERVICE_ORIGIN_N2",
            "SERVICE_ORIGIN_N2",
        )
        service_origin_n2_values = _parse_json_list(n2_raw) or _split_csv(n2_raw)

        # ------------- Fail-fast validation (explicit contract) -------------
        # The app must obtain these dimensions from .env (advanced users may
        # extend them manually). We do NOT infer them from datasets or Excel.
        missing_env: List[str] = []
        if not (so_raw or "").strip() or not service_origin_values:
            missing_env.append(
                "NPS_LENS_SERVICE_ORIGIN_BUUG (or SERVICE_ORIGIN_BUUG / NPS_LENS_SERVICE_ORIGIN / SERVICE_ORIGIN)"
            )
        if not (n1_raw or "").strip() or not service_origin_n1_map:
            missing_env.append("NPS_LENS_SERVICE_ORIGIN_N1 (or SERVICE_ORIGIN_N1)")

        # Ensure mapping is complete for all origins
        incomplete_map: List[str] = []
        for so in service_origin_values:
            vals = service_origin_n1_map.get(so, [])
            if not vals:
                incomplete_map.append(so)

        if missing_env or incomplete_map:
            msg_parts: List[str] = [
                "Invalid .env configuration for context dimensions:",
            ]
            if missing_env:
                msg_parts.append("- Missing/empty required env var(s):")
                msg_parts.extend([f"  - {x}" for x in missing_env])
            if incomplete_map:
                msg_parts.append(
                    "- service_origin_n1 map is missing entries (or empty lists) for: "
                    + ", ".join(incomplete_map)
                )
                msg_parts.append(
                    "  Define NPS_LENS_SERVICE_ORIGIN_N1 as JSON dict mapping each service_origin to a non-empty list."
                )
            raise RuntimeError("\n".join(msg_parts))

        # Backward compatible env var fallbacks (older builds used GEO/CHANNEL naming)
        default_service_origin = os.getenv(
            "NPS_LENS_DEFAULT_SERVICE_ORIGIN",
            os.getenv("NPS_LENS_DEFAULT_GEO", "BBVA México"),
        )
        default_service_origin_n1 = os.getenv(
            "NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1",
            os.getenv("NPS_LENS_DEFAULT_CHANNEL", "Senda"),
        )

        # If defaults are not part of the allowed sets, make them consistent.
        if service_origin_values and default_service_origin not in service_origin_values:
            default_service_origin = service_origin_values[0]
        if service_origin_n1_map.get(default_service_origin):
            allowed_n1 = service_origin_n1_map[default_service_origin]
            if allowed_n1 and default_service_origin_n1 not in allowed_n1:
                default_service_origin_n1 = allowed_n1[0]

        return Settings(
            data_dir=data_dir,
            knowledge_dir=knowledge_dir,
            default_service_origin=default_service_origin,
            default_service_origin_n1=default_service_origin_n1,
            service_origin_values=service_origin_values,
            service_origin_n1_map=service_origin_n1_map,
            service_origin_n2_values=service_origin_n2_values,
            log_level=os.getenv("NPS_LENS_LOG_LEVEL", "INFO"),
        )
