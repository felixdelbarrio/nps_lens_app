"""Compact ZIP categories; business artifacts keep their original label pairs."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from nps_lens.services.semantic_validation import GroundedDecision

CLASSIFICATION_BATCH_ROWS = 1_000
CLASSIFICATION_BATCH_BYTES = 300_000
CLASSIFIER_SCHEMA = "nps-lens-comments/4"
HELIX_SCHEMA = "nps-lens-helix/4"


def encode(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(encode(value)).hexdigest()


def comment_classification_fingerprint(comment: str) -> str:
    """Hash only the source text that can change a comment's semantic category."""

    return digest(comment)


def incident_classification_fingerprint(incident: dict[str, Any]) -> str:
    """Hash only narrative evidence; operational routing fields remain mutable."""

    return digest(str(incident.get("description", "")))


def category_catalog(taxonomy: dict[str, Any]) -> dict[str, dict[str, str]]:
    pairs = sorted(
        {(branch["lever"], sub) for branch in taxonomy["taxonomy"] for sub in branch["sublevers"]}
    )
    return {
        f"c{index:03d}": {"lever": lever, "sublever": sub}
        for index, (lever, sub) in enumerate(pairs, 1)
    }


class CompactClassification(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    id: str = Field(min_length=1)
    primary: Optional[str]
    evidence: GroundedDecision
    secondary: list[str] = Field(default_factory=list, max_length=2)

    def expand(self, catalog: dict[str, dict[str, str]]) -> dict[str, Any]:
        ids = ([self.primary] if self.primary is not None else []) + self.secondary
        if (
            len(ids) != len(set(ids))
            or any(key not in catalog for key in ids)
            or (self.primary is None and self.secondary)
        ):
            raise ValueError(
                "ID de categoría desconocido en la taxonomía o temas secundarios inválidos."
            )
        return {
            "primary_classification": (
                catalog[self.primary] if self.primary is not None else {"lever": "", "sublever": ""}
            ),
            "secondary_classifications": [catalog[key] for key in self.secondary],
        }


class CompactComment(CompactClassification):
    primary: str


class CompactCommentResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    classifications: list[CompactComment]
