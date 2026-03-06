from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError, field_validator
from typing_extensions import Literal


class ActionV1(BaseModel):
    action: str = ""
    owner: str = ""
    eta: str = ""


class RootCauseV1(BaseModel):
    cause: str
    why: str = ""
    evidence: List[str] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    actions: List[ActionV1] = Field(default_factory=list)


class InsightResponseV1(BaseModel):
    schema_version: Literal["1.0"] = "1.0"

    insight_id: str
    title: str
    executive_summary: str

    confidence: float = 0.0
    severity: int = 1

    journey_route: str = "unknown"
    segments_most_affected: List[str] = Field(default_factory=list)

    root_causes: List[RootCauseV1]

    assumptions: List[str] = Field(default_factory=list)
    risks: List[str] = Field(default_factory=list)
    next_questions: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)

    @field_validator("confidence")
    @classmethod
    def _conf_range(cls, v: float) -> float:
        try:
            v = float(v)
        except Exception:
            return 0.0
        return max(0.0, min(1.0, v))

    @field_validator("severity")
    @classmethod
    def _sev_range(cls, v: int) -> int:
        try:
            v = int(v)
        except Exception:
            return 1
        return max(1, min(5, v))


def validate_insight_response(
    obj: Dict[str, Any]
) -> tuple[bool, List[str], Optional[Dict[str, Any]]]:
    """Validate and normalize an LLM Insight JSON.

    Returns:
      ok, errors, normalized_dict
    """

    try:
        model = InsightResponseV1.model_validate(obj)
    except ValidationError as e:
        errs: List[str] = []
        for it in e.errors()[:12]:
            loc = ".".join(str(p) for p in it.get("loc", []))
            msg = str(it.get("msg", "invalid"))
            errs.append(f"{loc}: {msg}")
        return False, errs, None

    # Normalized dict (no PII handling here; prompt must avoid it)
    return True, [], model.model_dump(mode="json")
