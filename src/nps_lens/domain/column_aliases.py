from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Optional

COLUMN_ALIAS_SCHEMA_VERSION = "1.0"

NPS_COLUMN_SPECS: tuple[tuple[str, bool, tuple[str, ...]], ...] = (
    ("Fecha", True, ("Date", "GF CUST SURVEY RESPONSE DATE")),
    ("NPS", True, ("OPI-Sense", "NPS Response")),
    ("Canal", True, ("Channel",)),
    (
        "Comment",
        False,
        (
            "Text",
            "Verbatim valora la app SENDA",
            "Comment Response",
            "Comments",
            "Comentario",
            "Texto",
        ),
    ),
    ("ID", False, ("Id", "GF CUST SURVEY OPINION ID")),
    ("Palanca", False, ("Lever",)),
    ("Subpalanca", False, ("Sublever",)),
    (
        "UsuarioDecisión",
        False,
        (
            "Toma Decisión",
            "Usuario Decisión Final",
            "Decisión Usuario",
            "Usuario Decision",
        ),
    ),
    ("Browser", False, ("Navegador", "GF SURVEY ACC USER DEVICE DESC")),
    (
        "Operating System",
        False,
        (
            "GF OPERATING SYSTEM NAME",
            "Operating System Name",
            "Sistema Operativo",
        ),
    ),
    (
        "service_origin",
        False,
        ("Service Origin BUUG", "Service Origin BUG", "Service Origin"),
    ),
    ("service_origin_n1", False, ("Service Origin N1",)),
    ("service_origin_n2", False, ("Service Origin N2",)),
)

_HEADER_TOKEN_RE = re.compile(r"[^a-z0-9]+")


def normalize_column_header(value: object) -> str:
    """Return the single identity used for canonical headers and their aliases."""

    text = unicodedata.normalize("NFKD", str(value or "").strip())
    text = text.encode("ascii", "ignore").decode("ascii").casefold()
    return _HEADER_TOKEN_RE.sub("", text)


@dataclass(frozen=True)
class ColumnAliasField:
    canonical: str
    required: bool
    aliases: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "canonical": self.canonical,
            "required": self.required,
            "aliases": list(self.aliases),
        }


@dataclass(frozen=True)
class HeaderResolution:
    rename: dict[object, str]
    applied_aliases: tuple[tuple[str, str], ...]
    ambiguities: tuple[tuple[str, tuple[str, ...]], ...]


class ColumnAliasRegistry:
    def __init__(self, fields: Iterable[ColumnAliasField]) -> None:
        expected = {canonical: required for canonical, required, _aliases in NPS_COLUMN_SPECS}
        parsed = tuple(fields)
        if len(parsed) != len(expected) or {field.canonical for field in parsed} != set(expected):
            raise ValueError("La configuración debe incluir exactamente todos los campos NPS.")

        owners = {normalize_column_header(canonical): canonical for canonical in expected}
        normalized: list[ColumnAliasField] = []
        for field in parsed:
            if field.required != expected[field.canonical]:
                raise ValueError(
                    f"La obligatoriedad de '{field.canonical}' forma parte del contrato y no se puede editar."
                )
            aliases: list[str] = []
            local_keys: set[str] = set()
            for raw_alias in field.aliases:
                alias = str(raw_alias).strip()
                key = normalize_column_header(alias)
                if not key:
                    continue
                if key in local_keys:
                    raise ValueError(f"El alias '{alias}' está duplicado para '{field.canonical}'.")
                previous = owners.get(key)
                if previous and previous != field.canonical:
                    raise ValueError(
                        f"El alias '{alias}' colisiona entre '{previous}' y '{field.canonical}'."
                    )
                owners[key] = field.canonical
                local_keys.add(key)
                aliases.append(alias)
            normalized.append(ColumnAliasField(field.canonical, field.required, tuple(aliases)))
        order = {
            canonical: index
            for index, (canonical, _required, _aliases) in enumerate(NPS_COLUMN_SPECS)
        }
        self.fields = tuple(sorted(normalized, key=lambda field: order[field.canonical]))

    @classmethod
    def default(cls) -> "ColumnAliasRegistry":
        return cls(
            ColumnAliasField(canonical, required, aliases)
            for canonical, required, aliases in NPS_COLUMN_SPECS
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ColumnAliasRegistry":
        if (
            str(payload.get("schema_version", COLUMN_ALIAS_SCHEMA_VERSION))
            != COLUMN_ALIAS_SCHEMA_VERSION
        ):
            raise ValueError("Versión de configuración de alias de columnas no soportada.")
        raw_fields = payload.get("fields")
        if not isinstance(raw_fields, list):
            raise ValueError("La configuración de alias debe contener una lista 'fields'.")
        required_by_name = {
            canonical: required for canonical, required, _aliases in NPS_COLUMN_SPECS
        }
        fields: list[ColumnAliasField] = []
        for raw_field in raw_fields:
            if not isinstance(raw_field, Mapping):
                raise ValueError("Cada campo de alias debe ser un objeto.")
            canonical = str(raw_field.get("canonical", "")).strip()
            if canonical not in required_by_name:
                raise ValueError(f"Campo canónico NPS no soportado: {canonical or 'vacío'}.")
            aliases = raw_field.get("aliases", [])
            if not isinstance(aliases, list):
                raise ValueError(f"Los alias de '{canonical}' deben ser una lista.")
            requested_required = raw_field.get("required", required_by_name[canonical])
            if not isinstance(requested_required, bool):
                raise ValueError(f"La obligatoriedad de '{canonical}' debe ser booleana.")
            fields.append(
                ColumnAliasField(
                    canonical=canonical,
                    required=requested_required,
                    aliases=tuple(str(alias) for alias in aliases),
                )
            )
        return cls(fields)

    @classmethod
    def load(cls, path: Optional[Path]) -> "ColumnAliasRegistry":
        if path is None or not path.exists():
            return cls.default()
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("El fichero de alias de columnas no contiene un objeto JSON.")
        return cls.from_dict(payload)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": COLUMN_ALIAS_SCHEMA_VERSION,
            "fields": [field.to_dict() for field in self.fields],
        }

    def resolve(self, columns: Iterable[object]) -> HeaderResolution:
        source_columns = tuple(columns)
        by_key: dict[str, list[object]] = {}
        by_exact: dict[str, list[object]] = {}
        for column in source_columns:
            by_key.setdefault(normalize_column_header(column), []).append(column)
            by_exact.setdefault(str(column).strip(), []).append(column)

        rename: dict[object, str] = {column: str(column).strip() for column in source_columns}
        applied: list[tuple[str, str]] = []
        ambiguities: list[tuple[str, tuple[str, ...]]] = []
        for field in self.fields:
            exact_matches = by_exact.get(field.canonical, [])
            if exact_matches:
                if len(exact_matches) > 1:
                    ambiguities.append(
                        (field.canonical, tuple(str(value) for value in exact_matches))
                    )
                    continue
                rename[exact_matches[0]] = field.canonical
                continue
            canonical_key = normalize_column_header(field.canonical)
            canonical_matches = by_key.get(canonical_key, [])
            if canonical_matches:
                if len(canonical_matches) > 1:
                    ambiguities.append(
                        (field.canonical, tuple(str(value) for value in canonical_matches))
                    )
                    continue
                source = canonical_matches[0]
                rename[source] = field.canonical
                if str(source).strip() != field.canonical:
                    applied.append((str(source), field.canonical))
                continue

            alias_keys = tuple(
                key
                for key in (normalize_column_header(alias) for alias in field.aliases)
                if key != canonical_key
            )
            matches = [column for key in alias_keys for column in by_key.get(key, [])]
            if len(matches) > 1:
                ambiguities.append((field.canonical, tuple(str(value) for value in matches)))
            elif matches:
                source = matches[0]
                rename[source] = field.canonical
                applied.append((str(source), field.canonical))

        return HeaderResolution(rename, tuple(applied), tuple(ambiguities))
