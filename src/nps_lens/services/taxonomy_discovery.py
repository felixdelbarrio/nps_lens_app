from __future__ import annotations

import json
import logging
import unicodedata
from dataclasses import dataclass
from enum import Enum
from typing import Any, ContextManager, Iterator, Protocol, Sequence

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from nps_lens.services.taxonomy_prompts import (
    FALLBACK_LEVER,
    FALLBACK_SUBLEVERS,
    INSTRUCTIONS_VERSION,
    MAX_LEVERS,
    MAX_PROMPT_CHARS,
    MAX_RESPONSE_CHARS,
    MAX_SUBLEVERS,
    batch_prompt,
)

DISCOVERY_ENGINE_VERSION = "chatgpt-browser-v3"
logger = logging.getLogger(__name__)


class DiscoveryErrorCode(str, Enum):
    AUTH_REQUIRED = "AUTH_REQUIRED"
    INTERACTION_REQUIRED = "INTERACTION_REQUIRED"
    BROWSER_UNAVAILABLE = "BROWSER_UNAVAILABLE"
    CORPORATE_POLICY_BLOCKED = "CORPORATE_POLICY_BLOCKED"
    CHATGPT_UNAVAILABLE = "CHATGPT_UNAVAILABLE"
    PROJECT_NOT_ACCESSIBLE = "PROJECT_NOT_ACCESSIBLE"
    UI_CHANGED = "UI_CHANGED"
    TIMEOUT = "TIMEOUT"
    INVALID_JSON = "INVALID_JSON"
    INVALID_TAXONOMY = "INVALID_TAXONOMY"
    INVALID_CLASSIFICATION = "INVALID_CLASSIFICATION"
    INPUT_TOO_LARGE = "INPUT_TOO_LARGE"


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

    def verify_connection(self) -> str: ...

    def disconnect(self) -> None: ...

    def automation(self) -> ContextManager[BrowserAutomationRun]: ...


class TaxonomyDiscoveryProvider(Protocol):
    def session_status(self) -> str: ...

    def connect(self) -> str: ...

    def verify_connection(self) -> str: ...

    def disconnect(self) -> None: ...

    def signature_config(self) -> dict[str, Any]: ...

    def discover(self, comments: Sequence[tuple[str, str]]) -> dict[str, Any]: ...


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
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(value: str) -> None:
        raise ValueError("Non-JSON numeric constant")

    if len(raw) > MAX_RESPONSE_CHARS:
        raise TaxonomyDiscoveryError(code, "La respuesta excede el presupuesto del contrato batch.")
    try:
        payload = json.loads(raw, object_pairs_hook=unique_object, parse_constant=invalid_constant)
    except ValueError as exc:
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

    def verify_connection(self) -> str:
        return self.browser.verify_connection()

    def disconnect(self) -> None:
        self.browser.disconnect()

    def signature_config(self) -> dict[str, Any]:
        return {
            "engine": DISCOVERY_ENGINE_VERSION,
            "method": "chatgpt_browser",
            "designer_url": self.config.designer_url,
            "classifier_url": self.config.classifier_url,
            "batch_size": self.config.batch_size,
            "instructions_version": INSTRUCTIONS_VERSION,
        }

    @staticmethod
    def _designer_prompt(comments: Sequence[tuple[str, str]]) -> str:
        rows = [{"id": key, "Comment": comment} for key, comment in comments]
        return batch_prompt(
            "designer",
            {
                "task": "create_taxonomy",
                "config": {"max_levers": MAX_LEVERS, "max_sublevers_per_lever": MAX_SUBLEVERS},
                "comments": rows,
            },
        )

    @staticmethod
    def _classifier_prompt(taxonomy: TaxonomyResponse, comments: Sequence[tuple[str, str]]) -> str:
        rows = [{"id": key, "Comment": comment} for key, comment in comments]
        return batch_prompt(
            "classifier",
            {
                "task": "classify_comments",
                "taxonomy": taxonomy.model_dump()["taxonomy"],
                "comments": rows,
            },
        )

    @staticmethod
    def _consolidation_prompt(candidates: Sequence[TaxonomyResponse]) -> str:
        return batch_prompt(
            "designer",
            {
                "task": "consolidate_taxonomies",
                "config": {"max_levers": MAX_LEVERS, "max_sublevers_per_lever": MAX_SUBLEVERS},
                "taxonomy_candidates": [candidate.model_dump() for candidate in candidates],
            },
        )

    def _designer_batches(
        self, comments: Sequence[tuple[str, str]]
    ) -> Iterator[list[tuple[str, str]]]:
        base = len(self._designer_prompt([]))
        size = base
        batch: list[tuple[str, str]] = []
        for key, comment in comments:
            cost = len(
                json.dumps(
                    {"id": key, "Comment": comment}, ensure_ascii=False, separators=(",", ":")
                )
            )
            if base + cost > MAX_PROMPT_CHARS:
                raise TaxonomyDiscoveryError(
                    DiscoveryErrorCode.INPUT_TOO_LARGE,
                    "Un comentario individual excede el presupuesto por lote. "
                    "No se truncará; revisa esa fila antes de continuar.",
                )
            if batch and size + 1 + cost > MAX_PROMPT_CHARS:
                yield batch
                batch = []
                size = base
            size += cost + bool(batch)
            batch.append((key, comment))
        if batch:
            yield batch

    def _design(
        self, run: BrowserAutomationRun, batches: Sequence[list[tuple[str, str]]]
    ) -> TaxonomyResponse:
        def request(prompt: str) -> TaxonomyResponse:
            parsed = _strict_json(
                run.create_taxonomy(prompt, self.config.designer_url),
                DiscoveryErrorCode.INVALID_TAXONOMY,
                TaxonomyResponse,
            )
            assert isinstance(parsed, TaxonomyResponse)
            self._validate_taxonomy(parsed)
            return parsed

        candidates = [request(self._designer_prompt(batch)) for batch in batches]
        # Hierarchical reduction bounds every request, even for many corpus partitions.
        base = len(self._consolidation_prompt([]))
        while len(candidates) > 1:
            logger.info("taxonomy_discovery stage=consolidation candidates=%d", len(candidates))
            reduced: list[TaxonomyResponse] = []
            group: list[TaxonomyResponse] = []
            size = base
            for candidate in candidates:
                cost = len(candidate.model_dump_json())
                if base + cost > MAX_PROMPT_CHARS:
                    raise TaxonomyDiscoveryError(
                        DiscoveryErrorCode.INVALID_TAXONOMY,
                        "Una propuesta excede el presupuesto de consolidación.",
                    )
                if group and size + 1 + cost > MAX_PROMPT_CHARS:
                    reduced.append(
                        request(self._consolidation_prompt(group)) if len(group) > 1 else group[0]
                    )
                    group = []
                    size = base
                size += cost + bool(group)
                group.append(candidate)
            if group:
                reduced.append(
                    request(self._consolidation_prompt(group)) if len(group) > 1 else group[0]
                )
            if len(reduced) >= len(candidates):
                raise TaxonomyDiscoveryError(
                    DiscoveryErrorCode.INVALID_TAXONOMY,
                    "Las propuestas son demasiado extensas para consolidarlas sin perder información.",
                )
            candidates = reduced
        return candidates[0]

    def _classifier_batches(
        self, taxonomy: TaxonomyResponse, comments: Sequence[tuple[str, str]]
    ) -> Iterator[list[tuple[str, str]]]:
        # Greedy, linear packing: deterministic and bounded by both input and output.
        input_base = len(self._classifier_prompt(taxonomy, []))
        output_base = len('{"classifications":[]}')
        longest_pair = max(
            len(
                json.dumps(
                    {"lever": branch.lever, "sublever": sub},
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            for branch in taxonomy.taxonomy
            for sub in branch.sublevers
        )
        batch: list[tuple[str, str]] = []
        input_size, output_size = input_base, output_base
        for key, comment in comments:
            row_size = len(
                json.dumps(
                    {"id": key, "Comment": comment}, ensure_ascii=False, separators=(",", ":")
                )
            )
            result_size = (
                len(
                    json.dumps(
                        {"id": key, "primary_classification": {}},
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                )
                - 2
                + longest_pair
            )
            if (
                input_base + row_size > MAX_PROMPT_CHARS
                or output_base + result_size > MAX_RESPONSE_CHARS
            ):
                raise TaxonomyDiscoveryError(
                    DiscoveryErrorCode.INPUT_TOO_LARGE,
                    "Una fila no cabe en el presupuesto batch. No se truncará su contenido.",
                )
            if batch and (
                len(batch) >= self.config.batch_size
                or input_size + 1 + row_size > MAX_PROMPT_CHARS
                or output_size + 1 + result_size > MAX_RESPONSE_CHARS
            ):
                yield batch
                batch = []
                input_size, output_size = input_base, output_base
            input_size += row_size + bool(batch)
            output_size += result_size + bool(batch)
            batch.append((key, comment))
        if batch:
            yield batch

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

    def discover(self, comments: Sequence[tuple[str, str]]) -> dict[str, Any]:
        if not comments:
            return {"lever": [], "sublever": [], "provenance": [], "nodes": []}
        ids = [key for key, _ in comments]
        if len(ids) != len(set(ids)):
            raise TaxonomyDiscoveryError(
                DiscoveryErrorCode.INVALID_CLASSIFICATION, "Los IDs de entrada no son únicos."
            )
        # Preflight all individual rows before touching Chrome. Lists contain references,
        # not DataFrame copies or a serialized prompt of the entire corpus.
        designer_batches = list(self._designer_batches(comments))
        logger.info(
            "taxonomy_discovery stage=designer comments=%d batches=%d",
            len(comments),
            len(designer_batches),
        )

        # One persistent context owns Designer and all Classifier batches.
        with self.browser.automation() as run:
            taxonomy = self._design(run, designer_batches)
            categories = self._validate_taxonomy(taxonomy)
            logger.info("taxonomy_discovery stage=classifier comments=%d", len(comments))
            assignments: dict[str, PrimaryClassification] = {}
            for batch in self._classifier_batches(taxonomy, comments):
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
