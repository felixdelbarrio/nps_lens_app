from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class InsightPackV1(BaseModel):
    schema_version: str = Field("1.0", const=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    insight_id: str
    title: str
    context: dict[str, str]
    metrics: dict[str, float]
    quantitative_evidence: dict[str, object]
    qualitative_evidence: dict[str, object]
    hypotheses: list[dict[str, object]]
    suggested_questions: list[str]
    suggested_actions: list[str]
    technical_trace: dict[str, object]
