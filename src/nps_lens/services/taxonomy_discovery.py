from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, ContextManager, Protocol, Sequence

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

DISCOVERY_ENGINE_VERSION = "chatgpt-browser-v1"


class DiscoveryErrorCode(str, Enum):
    AUTH_REQUIRED = "AUTH_REQUIRED"
    CHATGPT_UNAVAILABLE = "CHATGPT_UNAVAILABLE"
    PROJECT_NOT_ACCESSIBLE = "PROJECT_NOT_ACCESSIBLE"
    UI_CHANGED = "UI_CHANGED"
    TIMEOUT = "TIMEOUT"
    INVALID_JSON = "INVALID_JSON"
    INVALID_TAXONOMY = "INVALID_TAXONOMY"
    INVALID_CLASSIFICATION = "INVALID_CLASSIFICATION"


class TaxonomyDiscoveryError(RuntimeError):
    def __init__(self, code: DiscoveryErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


class BrowserAutomationRun(Protocol):
    def create_taxonomy(self, prompt: str, designer_url: str) -> str: ...

    def classify_comments(self, prompt: str, classifier_url: str) -> str: ...


class ChatGPTBrowser(Protocol):
    def session_status(self) -> str: ...

    def connect(self) -> str: ...

    def disconnect(self) -> None: ...

    def automation(self) -> ContextManager[BrowserAutomationRun]: ...


class TaxonomyDiscoveryProvider(Protocol):
    def session_status(self) -> str: ...

    def connect(self) -> str: ...

    def disconnect(self) -> None: ...

    def signature_config(self) -> dict[str, Any]: ...

    def discover(self, comments: Sequence[tuple[str, str]]) -> dict[str, Any]: ...


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class TaxonomyBranch(_StrictModel):
    lever: str = Field(min_length=1)
    sublevers: list[str] = Field(min_length=1)

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
    taxonomy: list[TaxonomyBranch] = Field(min_length=1)


class PrimaryClassification(_StrictModel):
    lever: str = Field(min_length=1)
    sublever: str = Field(min_length=1)


class CommentClassification(_StrictModel):
    id: str = Field(min_length=1)
    primary_classification: PrimaryClassification


class ClassificationResponse(_StrictModel):
    classifications: list[CommentClassification]


@dataclass(frozen=True)
class ChatGPTDiscoveryConfig:
    designer_url: str
    classifier_url: str
    batch_size: int = 500

    def __post_init__(self) -> None:
        if not self.designer_url.startswith("https://chatgpt.com/"):
            raise ValueError("La URL de Designer debe pertenecer a https://chatgpt.com/.")
        if not self.classifier_url.startswith("https://chatgpt.com/"):
            raise ValueError("La URL de Classifier debe pertenecer a https://chatgpt.com/.")
        if not 50 <= self.batch_size <= 2000:
            raise ValueError("El tamaño de lote debe estar entre 50 y 2000.")


def _strict_json(raw: str, code: DiscoveryErrorCode, model: type[_StrictModel]) -> _StrictModel:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise TaxonomyDiscoveryError(
            DiscoveryErrorCode.INVALID_JSON, "ChatGPT no devolvió un documento JSON válido."
        ) from exc
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        raise TaxonomyDiscoveryError(code, "La respuesta de ChatGPT incumple el contrato.") from exc


class ChatGPTTaxonomyDiscoveryProvider:
    def __init__(self, browser: ChatGPTBrowser, config: ChatGPTDiscoveryConfig) -> None:
        self.browser = browser
        self.config = config

    def session_status(self) -> str:
        return self.browser.session_status()

    def connect(self) -> str:
        return self.browser.connect()

    def disconnect(self) -> None:
        self.browser.disconnect()

    def signature_config(self) -> dict[str, Any]:
        return {
            "engine": DISCOVERY_ENGINE_VERSION,
            "method": "chatgpt_browser",
            "designer_url": self.config.designer_url,
            "classifier_url": self.config.classifier_url,
            "batch_size": self.config.batch_size,
        }

    @staticmethod
    def _designer_prompt(comments: Sequence[tuple[str, str]]) -> str:
        rows = [{"id": key, "Comment": comment} for key, comment in comments]
        return (
            "Crea una taxonomía jerárquica exhaustiva para estos comentarios. "
            "Devuelve exclusivamente JSON válido con este contrato exacto: "
            '{"taxonomy":[{"lever":"...","sublevers":["..."]}]}. '
            "No incluyas markdown, explicaciones ni campos adicionales. Comentarios: "
            + json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
        )

    @staticmethod
    def _classifier_prompt(
        taxonomy: TaxonomyResponse, comments: Sequence[tuple[str, str]]
    ) -> str:
        rows = [{"id": key, "Comment": comment} for key, comment in comments]
        return (
            "Clasifica todos los comentarios en una única categoría de la taxonomía dada. "
            "Devuelve exclusivamente JSON válido con este contrato exacto: "
            '{"classifications":[{"id":"...","primary_classification":'
            '{"lever":"...","sublever":"..."}}]}. '
            "No omitas IDs y no incluyas markdown, explicaciones ni campos adicionales. Taxonomía: "
            + taxonomy.model_dump_json()
            + ". Comentarios: "
            + json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
        )

    @staticmethod
    def _validate_taxonomy(taxonomy: TaxonomyResponse) -> dict[str, set[str]]:
        levers = [branch.lever for branch in taxonomy.taxonomy]
        if len(levers) != len(set(levers)):
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INVALID_TAXONOMY, "La taxonomía contiene Palancas duplicadas."
            )
        return {branch.lever: set(branch.sublevers) for branch in taxonomy.taxonomy}

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

    def discover(self, comments: Sequence[tuple[str, str]]) -> dict[str, Any]:
        if not comments:
            return {"lever": [], "sublever": [], "provenance": [], "nodes": []}
        ids = [key for key, _ in comments]
        if len(ids) != len(set(ids)):
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INVALID_CLASSIFICATION, "Los IDs de entrada no son únicos."
            )

        # One persistent context owns Designer and all Classifier batches.
        with self.browser.automation() as run:
            taxonomy = _strict_json(
                run.create_taxonomy(self._designer_prompt(comments), self.config.designer_url),
                DiscoveryErrorCode.INVALID_TAXONOMY,
                TaxonomyResponse,
            )
            assert isinstance(taxonomy, TaxonomyResponse)
            categories = self._validate_taxonomy(taxonomy)
            assignments: dict[str, PrimaryClassification] = {}
            for start in range(0, len(comments), self.config.batch_size):
                batch = comments[start : start + self.config.batch_size]
                parsed = _strict_json(
                    run.classify_comments(
                        self._classifier_prompt(taxonomy, batch), self.config.classifier_url
                    ),
                    DiscoveryErrorCode.INVALID_CLASSIFICATION,
                    ClassificationResponse,
                )
                assert isinstance(parsed, ClassificationResponse)
                assignments.update(
                    self._validate_batch(parsed, [key for key, _ in batch], categories)
                )

        return {
            "lever": [assignments[key].lever for key in ids],
            "sublever": [assignments[key].sublever for key in ids],
            "provenance": ["chatgpt"] * len(ids),
            "nodes": [],
        }
