from __future__ import annotations

import unicodedata
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator
from typing_extensions import Annotated

from nps_lens.services.semantic_validation import GroundedDecision
from nps_lens.services.taxonomy_prompts import (
    FALLBACK_LEVER,
    FALLBACK_SUBLEVERS,
    MAX_LEVERS,
    MAX_SUBLEVERS,
)


class DiscoveryErrorCode(str, Enum):
    INVALID_TAXONOMY = "INVALID_TAXONOMY"


class TaxonomyDiscoveryError(RuntimeError):
    def __init__(self, code: DiscoveryErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class TaxonomyBranch(_StrictModel):
    lever: str = Field(min_length=1)
    sublevers: list[str] = Field(min_length=1, max_length=MAX_SUBLEVERS)

    @field_validator("lever")
    @classmethod
    def clean_lever(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("lever must not contain surrounding whitespace")
        return value

    @field_validator("sublevers")
    @classmethod
    def unique_sublevers(cls, values: list[str]) -> list[str]:
        if any(not value.strip() or value != value.strip() for value in values):
            raise ValueError("sublevers must be non-empty and trimmed")
        if len(values) != len(set(values)):
            raise ValueError("sublevers must be unique within a lever")
        return values


class TaxonomyResponse(_StrictModel):
    taxonomy: list[TaxonomyBranch] = Field(min_length=1, max_length=MAX_LEVERS)


class TaxonomyReview(GroundedDecision):
    """Corpus-level evidence for a designed taxonomy.

    Unlike one classification decision (which intentionally caps evidence at three
    quotes), a taxonomy review may need to ground several boundaries and
    counterexamples across the corpus. Keep it bounded by the maximum number of
    taxonomy leaves rather than reusing the per-decision limit.
    """

    quotes: list[str] = Field(max_length=MAX_LEVERS * MAX_SUBLEVERS)
    reason: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=12, max_length=5_000)
    ]


class TaxonomyDesignResponse(TaxonomyResponse):
    review: TaxonomyReview


class TaxonomyValidator:
    @staticmethod
    def _validate_taxonomy(taxonomy: TaxonomyResponse) -> dict[str, set[str]]:
        def normalized(value: str) -> str:
            return unicodedata.normalize("NFKC", value).casefold()

        levers = [normalized(branch.lever) for branch in taxonomy.taxonomy]
        sublevers = [normalized(sub) for branch in taxonomy.taxonomy for sub in branch.sublevers]
        if len(levers) != len(set(levers)) or len(sublevers) != len(set(sublevers)):
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INVALID_TAXONOMY, "La taxonomía contiene categorías duplicadas."
            )
        categories = {branch.lever: set(branch.sublevers) for branch in taxonomy.taxonomy}
        if categories.get(FALLBACK_LEVER) != set(FALLBACK_SUBLEVERS):
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INVALID_TAXONOMY,
                "La taxonomía no contiene las categorías obligatorias para texto sin encaje.",
            )
        return categories
