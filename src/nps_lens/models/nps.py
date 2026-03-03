from __future__ import annotations

from datetime import datetime, date
from typing import Optional

from pydantic import BaseModel, Field, validator

from nps_lens.models.common import CanonicalBase


class NpsGroup(str):
    PROMOTER = "PROMOTOR"
    PASSIVE = "PASIVO"
    DETRACTOR = "DETRACTOR"


class NPSRecord(CanonicalBase):
    responded_at: datetime
    response_id: str
    nps_score: int = Field(..., ge=0, le=10)
    nps_group: str
    comment: Optional[str] = None

    # Dimensions / taxonomy
    segment: Optional[str] = None
    decision_user: Optional[str] = None
    touchpoint: Optional[str] = None  # journey step if available
    lever: str = Field(..., description="Palanca")
    sublever: str = Field(..., description="Subpalanca")

    @validator("nps_group")
    def validate_group(cls, v: str) -> str:
        up = v.strip().upper()
        if up not in {NpsGroup.PROMOTER, NpsGroup.PASSIVE, NpsGroup.DETRACTOR}:
            return up
        return up

    @property
    def period(self) -> date:
        return self.responded_at.date()
