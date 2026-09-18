from __future__ import annotations

import unicodedata
from enum import Enum
from typing import Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator

from nps_lens.services.taxonomy_prompts import (
    FALLBACK_LEVER,
    FALLBACK_SUBLEVERS,
    MAX_LEVERS,
    MAX_SUBLEVERS,
)


class DiscoveryErrorCode(str, Enum):
    INVALID_TAXONOMY = "INVALID_TAXONOMY"
    INVALID_CLASSIFICATION = "INVALID_CLASSIFICATION"


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


class PrimaryClassification(_StrictModel):
    lever: str = Field(min_length=1)
    sublever: str = Field(min_length=1)


class CommentClassification(_StrictModel):
    id: str = Field(min_length=1)
    primary_classification: PrimaryClassification


class ClassificationResponse(_StrictModel):
    classifications: list[CommentClassification]


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

    @staticmethod
    def _validate_batch(
        response: ClassificationResponse,
        expected: Sequence[str],
        categories: dict[str, set[str]],
    ) -> dict[str, PrimaryClassification]:
        ids = [item.id for item in response.classifications]
        if len(ids) != len(set(ids)) or set(ids) != set(expected):
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INVALID_CLASSIFICATION,
                "La clasificación contiene IDs desconocidos, duplicados o ausentes.",
            )
        result: dict[str, PrimaryClassification] = {}
        for item in response.classifications:
            primary = item.primary_classification
            if primary.lever not in categories or primary.sublever not in categories[primary.lever]:
                raise TaxonomyDiscoveryError(
                    DiscoveryErrorCode.INVALID_CLASSIFICATION,
                    "La clasificación utiliza una categoría fuera de la taxonomía.",
                )
            result[item.id] = primary
        return result
