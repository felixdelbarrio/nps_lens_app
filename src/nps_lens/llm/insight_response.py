from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from typing_extensions import Literal


class QuantEvidenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric: str = ""
    value: str = ""
    context: str = ""


class EvidenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    quant: List[QuantEvidenceV1] = Field(default_factory=list)
    qual: List[str] = Field(default_factory=list)

    @field_validator("qual")
    @classmethod
    def _cap_qual(cls, v: List[str]) -> List[str]:
        return [str(item).strip() for item in v if str(item).strip()][:5]


class TagsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    geo: str = "unknown"
    channel: str = "unknown"
    lever: str = "unknown"
    sublever: str = "unknown"
    period: str = "unknown"
    route_signature: str = "unknown"

    @field_validator("geo", "channel", "lever", "sublever", "period", "route_signature")
    @classmethod
    def _default_unknown(cls, v: str) -> str:
        txt = str(v or "").strip()
        return txt or "unknown"


class ActionV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: str = ""
    owner: str = ""
    eta: str = ""


class RootCauseV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cause: str
    why: str = ""
    evidence: EvidenceV1 = Field(default_factory=EvidenceV1)
    assumptions: List[str] = Field(default_factory=list)
    actions: List[ActionV1] = Field(default_factory=list)
    tests_or_checks: List[str] = Field(default_factory=list)

    @field_validator("assumptions")
    @classmethod
    def _clean_assumptions(cls, v: List[str]) -> List[str]:
        return [str(item).strip() for item in v if str(item).strip()]

    @field_validator("actions")
    @classmethod
    def _cap_actions(cls, v: List[ActionV1]) -> List[ActionV1]:
        return v[:3]

    @field_validator("tests_or_checks")
    @classmethod
    def _cap_tests(cls, v: List[str]) -> List[str]:
        return [str(item).strip() for item in v if str(item).strip()][:5]


class InsightResponseV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

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
    tags: TagsV1 = Field(default_factory=TagsV1)

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

    @field_validator("segments_most_affected")
    @classmethod
    def _clean_segments(cls, v: List[str]) -> List[str]:
        return [str(item).strip() for item in v if str(item).strip()]

    @field_validator("root_causes")
    @classmethod
    def _cap_root_causes(cls, v: List[RootCauseV1]) -> List[RootCauseV1]:
        return v[:3]

    @field_validator("assumptions", "risks", "next_questions")
    @classmethod
    def _clean_text_lists(cls, v: List[str]) -> List[str]:
        return [str(item).strip() for item in v if str(item).strip()]


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
