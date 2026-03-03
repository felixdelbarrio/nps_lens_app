from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field


class CanonicalBase(BaseModel):
    dataset_id: str = Field(..., description="Identificador versionado del dataset de origen.")
    geo: str = Field(..., min_length=2, description="Geografía/país, e.g. MX, ES, CO.")
    channel: str = Field(..., min_length=1, description="Canal, e.g. Senda, Gema.")
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        allow_mutation = False
        anystr_strip_whitespace = True


class TimeWindow(BaseModel):
    start: date
    end: date
