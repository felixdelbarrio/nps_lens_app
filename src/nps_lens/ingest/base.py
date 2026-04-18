from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Sequence

import pandas as pd


@dataclass(frozen=True)
class ValidationIssue:
    level: str  # ERROR|WARN
    message: str
    column: Optional[str] = None
    code: str = ""
    details: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        payload = {
            "level": self.level,
            "message": self.message,
            "column": self.column,
        }
        if self.code:
            payload["code"] = self.code
        if self.details:
            payload["details"] = self.details
        return payload


@dataclass(frozen=True)
class IngestResult:
    df: pd.DataFrame
    issues: list[ValidationIssue]
    dataset_id: str
    meta: dict[str, object] = field(default_factory=dict)


def require_columns(df: pd.DataFrame, required: Sequence[str]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    missing = [c for c in required if c not in df.columns]
    for c in missing:
        issues.append(
            ValidationIssue(
                level="ERROR",
                message="Missing required column",
                column=c,
                code="missing_required_column",
            )
        )
    return issues


def standardize_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    ren = {}
    for col in out.columns:
        key = col.strip()
        if key in mapping:
            ren[col] = mapping[key]
    return out.rename(columns=ren)
