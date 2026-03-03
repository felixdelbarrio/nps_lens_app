from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class InsightPackV1(BaseModel):
    schema_version: str = Field("1.0", const=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    insight_id: str
    title: str
    context: Dict[str, str]
    metrics: Dict[str, float]
    quantitative_evidence: Dict[str, object]
    qualitative_evidence: Dict[str, object]
    hypotheses: List[Dict[str, object]]
    suggested_questions: List[str]
    suggested_actions: List[str]
    technical_trace: Dict[str, object]
