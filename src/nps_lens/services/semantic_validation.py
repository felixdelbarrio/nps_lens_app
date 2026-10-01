"""Grounding checks for LLM exchanges, without pretending to infer text meaning."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator
from typing_extensions import Annotated

Reason = Annotated[str, StringConstraints(strip_whitespace=True, min_length=12, max_length=2000)]


class GroundedDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    quotes: list[str] = Field(max_length=3)
    reason: Reason

    @field_validator("quotes")
    @classmethod
    def bounded_quotes(cls, values: list[str]) -> list[str]:
        if any(len(value) > 2000 for value in values):
            raise ValueError("Cada cita debe tener como máximo 2000 caracteres.")
        return values

    def validate_source(self, text: str, *, allow_empty: bool = False) -> None:
        if not self.quotes and not allow_empty:
            raise ValueError("Falta evidencia literal de la decisión.")
        if len(set(self.quotes)) != len(self.quotes):
            raise ValueError("Evidencia literal duplicada.")
        for quote in self.quotes:
            validate_quote(quote, text)


def validate_quote(quote: str, text: str) -> None:
    if not quote.strip() or quote not in text:
        raise ValueError("La evidencia debe ser una cita literal no vacía del texto original.")


def validate_decision(row: Any, catalog: dict[str, dict[str, str]], text: str) -> None:
    row.evidence.validate_source(text, allow_empty=not text.strip())
    if row.secondary and len(row.evidence.quotes) < 2:
        raise ValueError("Los temas secundarios requieren evidencia independiente explícita.")
    if any(catalog[key]["lever"] == "Sin clasificación temática" for key in row.secondary):
        raise ValueError("Una categoría de reserva no es un tema secundario.")
    if (
        row.primary is not None
        and catalog[row.primary]["lever"] == "Sin clasificación temática"
        and row.secondary
    ):
        raise ValueError("Una categoría de reserva no admite temas secundarios.")
