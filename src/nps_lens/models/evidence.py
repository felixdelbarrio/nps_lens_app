from __future__ import annotations

from pydantic import BaseModel, Field


class EvidenceLink(BaseModel):
    source_type: str
    source_id: str
    target_type: str
    target_id: str
    score: float = Field(..., ge=0.0, le=1.0)
    explanation: str
