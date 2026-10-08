from __future__ import annotations

import unicodedata
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

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


class TaxonomyLeaf(_StrictModel):
    name: str = Field(min_length=1)
    criterion: str = Field(min_length=1, max_length=500)

    @field_validator("name", "criterion")
    @classmethod
    def trimmed(cls, value: str) -> str:
        if not value.strip() or value != value.strip():
            raise ValueError("name and criterion must be non-empty and trimmed")
        return value


class TaxonomyBranch(_StrictModel):
    lever: str = Field(min_length=1)
    sublevers: list[TaxonomyLeaf] = Field(min_length=1, max_length=MAX_SUBLEVERS)

    @field_validator("lever")
    @classmethod
    def clean_lever(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("lever must not contain surrounding whitespace")
        return value


class TaxonomyResponse(_StrictModel):
    taxonomy: list[TaxonomyBranch] = Field(min_length=1, max_length=MAX_LEVERS)


class TaxonomyReview(_StrictModel):
    """Bounded corpus review retained with the designed semantic catalog."""

    quotes: list[Annotated[str, StringConstraints(min_length=1, max_length=2000)]] = Field(
        max_length=MAX_LEVERS * MAX_SUBLEVERS
    )
    reason: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=12, max_length=5_000)
    ]


class SemanticCriteriaResponse(_StrictModel):
    criteria: dict[
        str, Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
    ]
    review: TaxonomyReview


class TaxonomyDesignResponse(TaxonomyResponse):
    review: TaxonomyReview


class TaxonomyValidator:
    @staticmethod
    def _validate_taxonomy(taxonomy: TaxonomyResponse) -> dict[str, set[str]]:
        def normalized(value: str) -> str:
            return unicodedata.normalize("NFKC", value).casefold()

        levers = [normalized(branch.lever) for branch in taxonomy.taxonomy]
        sublevers = [
            normalized(sub.name) for branch in taxonomy.taxonomy for sub in branch.sublevers
        ]
        if len(levers) != len(set(levers)) or len(sublevers) != len(set(sublevers)):
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INVALID_TAXONOMY, "La taxonomía contiene categorías duplicadas."
            )
        categories = {
            branch.lever: {sub.name for sub in branch.sublevers} for branch in taxonomy.taxonomy
        }
        if categories.get(FALLBACK_LEVER) != set(FALLBACK_SUBLEVERS):
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INVALID_TAXONOMY,
                "La taxonomía no contiene las categorías obligatorias para texto sin encaje.",
            )
        return categories
