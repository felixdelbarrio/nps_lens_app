from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from typing_extensions import Literal

from pydantic import BaseModel, Field


class InsightPackV1(BaseModel):
    # Compatible with both Pydantic v1 and v2 (Field(const=True) was removed in v2)
    schema_version: Literal["1.0"] = "1.0"
    created_at: datetime = Field(default_factory=datetime.utcnow)

    insight_id: str
    title: str
    context: Dict[str, str]
    metrics: Dict[str, float]
    quantitative_evidence: Dict[str, Any]
    qualitative_evidence: Dict[str, Any]
    hypotheses: List[Dict[str, Any]]
    suggested_questions: List[str]
    suggested_actions: List[str]
    technical_trace: Dict[str, Any]
