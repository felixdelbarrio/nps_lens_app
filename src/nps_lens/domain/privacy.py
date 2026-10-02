"""Shared operational snippet redaction; source corpora stay untouched.

Masks structured identifiers and named identity fields before clipping/formatting.
Unlabelled names cannot be exhaustively identified by regex; do not treat this as
an anonymisation guarantee for arbitrary free text.
"""

from __future__ import annotations

import re
from typing import Any

_EMAIL = re.compile(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b")
_IDENTIFIERS = re.compile(r"(?<!\w)(?:\+?\d[\d .()/\-]{6,}\d)(?!\w)")
_IDENTITY_FIELD = re.compile(
    r"(?im)\b(?:cuit|cuil|dni|nif|rfc|raz[oó]n\s+social|empresa|compa[nñ][ií]a|"
    r"cliente|nombre(?:\s+y\s+apellidos?)?|apellido|contacto|titular|"
    r"c[oó]digo\s+(?:de\s+)?empresa|identificador\s+personal|legajo|"
    r"tel[eé]fono|celular|m[oó]vil|e-?mail|correo(?:\s+electr[oó]nico)?)"
    r"\s*[:=]\s*[^\n;|]+"
)
_NAMED_PERSON = re.compile(
    r"\b(?:Sr\.?|Sra\.?|Don|Doña|Me llamo|Soy)\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){0,3}\b"
)
_COMPANY = re.compile(
    r"\b[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ-]*(?:\s+[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ-]*){0,5}\s+(?:S\.?A\.?S?\.?|S\.?R\.?L\.?|S\.?L\.?|LLC|Ltd)\b\.?"
)


def redact_operational_snippet(text: str) -> str:
    value = _IDENTITY_FIELD.sub("[dato reservado]", str(text or ""))
    value = _EMAIL.sub("[email reservado]", value)
    value = _IDENTIFIERS.sub("[identificador reservado]", value)
    value = _NAMED_PERSON.sub("[persona reservada]", value)
    return _COMPANY.sub("[empresa reservada]", value)


_TEXT_FIELDS = {
    "examples",
    "Incident Summary",
    "Detractor Comment",
    "incident_summary",
    "detractor_comment",
    "affected_task",
    "observed_symptom",
    "operational_recommendation",
    "comment",
    "Comment",
    "Comentario",
    "Comentarios",
    "comment_txt",
    "comment_norm",
    "summary",
    "description",
    "Detailed Description",
    "Detailed Decription",
    "bbva_detaileddescription",
    "Descripción",
    "Summary",
    "Resolution",
    "comment_quote",
    "incident_quote",
    "chain_story",
    "journey_route",
    "incident_examples",
    "comment_examples",
    "quotes",
    "text",
    "top_terms",
}


def redact_public_payload(value: Any, *, field: str = "") -> Any:
    """Preserve keys, IDs, URLs and numeric contracts; sanitize narrative values."""
    if isinstance(value, dict):
        # Formatting spans must never split identifiers before redaction.
        result = {key: redact_public_payload(item, field=key) for key, item in value.items()}
        for key in ("comment_segments", "summary_segments"):
            if isinstance(value.get(key), list):
                joined = "".join(
                    str(span.get("text", "")) for span in value[key] if isinstance(span, dict)
                )
                if redact_operational_snippet(joined) != joined:
                    result[key] = [{"text": redact_operational_snippet(joined), "bold": False}]
        return result
    if isinstance(value, list):
        return [redact_public_payload(item, field=field) for item in value]
    if isinstance(value, str):
        if re.search(
            r"(?i)^(cuit|cuil|dni|rfc|raz[oó]n social|tel[eé]fono|email|nombre|apellido|cliente|c[oó]digo de empresa)$",
            field,
        ):
            return "[dato reservado]" if value else ""
        if field in _TEXT_FIELDS:
            return redact_operational_snippet(value)
    return value
