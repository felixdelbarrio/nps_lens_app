"""Compact ZIP categories; business artifacts keep their original label pairs."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from nps_lens.services.taxonomy_prompts import FALLBACK_LEVER, FALLBACK_SUBLEVERS

CLASSIFICATION_BATCH_ROWS = 500
CLASSIFICATION_BATCH_BYTES = 300_000
CLASSIFIER_SCHEMA = "nps-lens-comments/5"
HELIX_SCHEMA = "nps-lens-helix/5"


def encode(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(encode(value)).hexdigest()


def comment_classification_fingerprint(comment: str) -> str:
    """Hash only the source text that can change a comment's semantic category."""

    return digest(comment)


def incident_classification_fingerprint(incident: dict[str, Any]) -> str:
    """Hash narrative and link date; operational routing fields remain mutable."""

    return digest([str(incident.get("description", "")), incident.get("date", "")])


def category_catalog(taxonomy: dict[str, Any]) -> dict[str, dict[str, str]]:
    # SOURCE/Manual are label-based domain catalogs, not old LLM response schemas.
    pairs = {}
    for branch in taxonomy["taxonomy"]:
        for sub in branch["sublevers"]:
            name = sub if isinstance(sub, str) else sub["name"]
            criterion = (
                label_criterion(branch["lever"], name) if isinstance(sub, str) else sub["criterion"]
            )
            if not branch["lever"].strip() or not name.strip() or not criterion.strip():
                raise ValueError(
                    "Categoría incompleta: se requieren Palanca, Subpalanca y criterion."
                )
            pair = (branch["lever"], name)
            if pair in pairs:
                raise ValueError("Categoría duplicada.")
            pairs[pair] = criterion
    return {
        f"c{index:03d}": {"lever": lever, "sublever": sub, "criterion": pairs[(lever, sub)]}
        for index, (lever, sub) in enumerate(sorted(pairs), 1)
    }


def label_criterion(lever: str, sublever: str) -> str:
    """Explicit literal policy for user-owned label catalogs; never invent boundaries."""
    if lever == FALLBACK_LEVER:
        return {
            FALLBACK_SUBLEVERS[
                0
            ]: "Usar solo cuando el significado del texto no pueda determinarse.",
            FALLBACK_SUBLEVERS[
                1
            ]: "Usar cuando el tema sea explícito pero ninguna categoría lo represente.",
        }.get(sublever, f"Usar solo para el significado explícito de {sublever}.")
    return f"Usar solo cuando el texto exprese {sublever} en la dimensión {lever}; no inferir causas ni temas no afirmados."


def taxonomy_fingerprint(taxonomy: dict[str, Any]) -> str:
    """Order-independent semantic identity shared by every classification stage."""
    return digest(category_catalog(taxonomy))


class CompactClassification(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    id: str = Field(min_length=1)
    primary: Optional[str]
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
        if any(catalog[key]["lever"] == FALLBACK_LEVER for key in self.secondary):
            raise ValueError("Una categoría de reserva no es un tema secundario.")

        def pair(key: str) -> dict[str, str]:
            return {field: catalog[key][field] for field in ("lever", "sublever")}

        return {
            "primary_classification": (
                pair(self.primary) if self.primary is not None else {"lever": "", "sublever": ""}
            ),
            "secondary_classifications": [pair(key) for key in self.secondary],
        }


class CompactComment(CompactClassification):
    primary: str


class CompactCommentResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    classifications: list[CompactComment]
