from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    knowledge_dir: Path
    default_geo: str
    default_channel: str
    log_level: str

    @staticmethod
    def from_env() -> Settings:
        data_dir = Path(os.getenv("NPS_LENS_DATA_DIR", "./data"))
        knowledge_dir = Path(os.getenv("NPS_LENS_KNOWLEDGE_DIR", "./knowledge"))
        return Settings(
            data_dir=data_dir,
            knowledge_dir=knowledge_dir,
            default_geo=os.getenv("NPS_LENS_DEFAULT_GEO", "MX"),
            default_channel=os.getenv("NPS_LENS_DEFAULT_CHANNEL", "Senda"),
            log_level=os.getenv("NPS_LENS_LOG_LEVEL", "INFO"),
        )
