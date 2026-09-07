from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, cast

import pandas as pd

EQUIVALENCE_SCHEMA_VERSION = "1.0"

_WHITESPACE_RE = re.compile(r"\s+")
_SEPARATOR_RE = re.compile(r"\s*(?:/|\\|&|\+|\||;|,)\s*")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_JOIN_WORDS = {"y", "e"}
_EMPTY_MARKERS = {"", "nan", "none", "null", "nat", "<na>"}


DEFAULT_EQUIVALENCES: dict[str, tuple[tuple[str, tuple[str, ...]], ...]] = {
    "Palanca": (
        (
            "Pagos/transferencias",
            (
                "pagos y transferencias",
                "pagos / transferencias",
                "pagos/ transferencias",
                "pagos/transferencias",
            ),
        ),
        ("Funcionamiento continuo", ("funcionamiento continuo",)),
        ("Agregar funcionalidad", ("agregar funcionalidad",)),
    ),
    "Subpalanca": (
        (
            "Pagos/transferencias",
            (
                "pagos y transferencias",
                "pagos / transferencias",
                "pagos/ transferencias",
                "pagos/transferencias",
            ),
        ),
        (
            "Practicidad/Facilidad de uso",
            ("practicidad / facilidad de uso", "practicidad/facilidad de uso"),
        ),
        ("Fallas en el login", ("fallas en el login",)),
        ("No funciona bien/falla", ("no funciona bien / falla", "no funciona bien/falla")),
        ("Interface/Actualización", ("interface / actualización", "interface/actualización")),
        ("Agregar funcionalidad", ("agregar funcionalidad",)),
        ("FAN", ("fan",)),
        ("Fan Web", ("fan web",)),
    ),
}


def clean_label(value: object) -> str:
    if value is None:
        return ""
    try:
        if bool(cast(Any, pd.isna)(value)):
            return ""
    except (TypeError, ValueError):
        pass
    text = unicodedata.normalize("NFKC", str(value)).strip()
    text = _WHITESPACE_RE.sub(" ", text)
    if text.casefold() in _EMPTY_MARKERS:
        return ""
    return _SEPARATOR_RE.sub("/", text)


def equivalence_key(value: object) -> str:
    """Return a punctuation-, accent-, whitespace- and case-insensitive identity.

    Spanish joiners are ignored so ``Pagos y transferencias`` and
    ``Pagos/transferencias`` share the same identity without using fuzzy matching.
    Fuzzy edit-distance matching is deliberately excluded: it creates silent false
    positives in business taxonomies.
    """

    text = clean_label(value).casefold()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    tokens = [token for token in _TOKEN_RE.findall(text) if token not in _JOIN_WORDS]
    return " ".join(tokens)


def _default_display(value: object) -> str:
    text = clean_label(value)
    if not text:
        return ""
    folded = text.casefold()
    return folded[:1].upper() + folded[1:]


@dataclass(frozen=True)
class EquivalenceGroup:
    canonical: str
    aliases: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {"canonical": self.canonical, "aliases": list(self.aliases)}


class EquivalenceRegistry:
    def __init__(self, groups: Mapping[str, Iterable[EquivalenceGroup]]) -> None:
        normalized: dict[str, tuple[EquivalenceGroup, ...]] = {}
        lookups: dict[str, dict[str, str]] = {}
        for dimension, raw_groups in groups.items():
            dimension_name = clean_label(dimension)
            if dimension_name not in {"Canal", "Palanca", "Subpalanca"}:
                continue
            dimension_groups: list[EquivalenceGroup] = []
            dimension_lookup: dict[str, str] = {}
            for raw_group in raw_groups:
                canonical = clean_label(raw_group.canonical)
                if not canonical:
                    continue
                aliases = tuple(
                    dict.fromkeys(
                        clean_label(alias) for alias in raw_group.aliases if clean_label(alias)
                    )
                )
                group = EquivalenceGroup(canonical=canonical, aliases=aliases)
                for candidate in (canonical, *aliases):
                    key = equivalence_key(candidate)
                    previous = dimension_lookup.get(key)
                    if previous and previous != canonical:
                        raise ValueError(
                            f"La equivalencia '{candidate}' está asignada a '{previous}' y '{canonical}' en {dimension_name}."
                        )
                    if key:
                        dimension_lookup[key] = canonical
                dimension_groups.append(group)
            normalized[dimension_name] = tuple(dimension_groups)
            lookups[dimension_name] = dimension_lookup
        self._groups = normalized
        self._lookups = lookups

    @classmethod
    def default(cls) -> "EquivalenceRegistry":
        return cls(
            {
                dimension: [EquivalenceGroup(canonical, aliases) for canonical, aliases in groups]
                for dimension, groups in DEFAULT_EQUIVALENCES.items()
            }
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "EquivalenceRegistry":
        dimensions = payload.get("dimensions", {})
        if not isinstance(dimensions, Mapping):
            raise ValueError("El registro de equivalencias debe contener un objeto 'dimensions'.")
        groups: dict[str, list[EquivalenceGroup]] = {}
        for dimension, raw_groups in dimensions.items():
            if not isinstance(raw_groups, list):
                raise ValueError(f"Las equivalencias de {dimension} deben ser una lista.")
            parsed: list[EquivalenceGroup] = []
            for raw_group in raw_groups:
                if not isinstance(raw_group, Mapping):
                    raise ValueError(f"Grupo inválido en {dimension}.")
                aliases = raw_group.get("aliases", [])
                if not isinstance(aliases, list):
                    raise ValueError(f"Los alias de {dimension} deben ser una lista.")
                parsed.append(
                    EquivalenceGroup(
                        canonical=clean_label(raw_group.get("canonical", "")),
                        aliases=tuple(clean_label(alias) for alias in aliases),
                    )
                )
            groups[str(dimension)] = parsed
        return cls(groups)

    @classmethod
    def load(cls, path: Path) -> "EquivalenceRegistry":
        if not path.exists():
            registry = cls.default()
            registry.save(path)
            return registry
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("El fichero de equivalencias no contiene un objeto JSON.")
        return cls.from_dict(payload)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)

    def normalize(self, dimension: str, value: object) -> str:
        clean = clean_label(value)
        if not clean:
            return ""
        canonical = self._lookups.get(clean_label(dimension), {}).get(equivalence_key(clean))
        return canonical or _default_display(clean)

    def key(self, dimension: str, value: object) -> str:
        return equivalence_key(self.normalize(dimension, value))

    def normalize_series(self, dimension: str, values: pd.Series[Any]) -> pd.Series[Any]:
        unique_values = values.drop_duplicates().tolist()
        lookup = {value: self.normalize(dimension, value) for value in unique_values}
        return values.map(lookup).fillna("")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": EQUIVALENCE_SCHEMA_VERSION,
            "dimensions": {
                dimension: [group.to_dict() for group in groups]
                for dimension, groups in self._groups.items()
            },
        }

    def collision_report(self, dimension: str, values: Iterable[object]) -> list[dict[str, object]]:
        observed: dict[str, set[str]] = {}
        for value in values:
            raw = clean_label(value)
            if not raw:
                continue
            observed.setdefault(self.key(dimension, raw), set()).add(raw)
        return [
            {
                "canonical": self.normalize(dimension, next(iter(raw_values))),
                "variants": sorted(raw_values),
            }
            for _, raw_values in sorted(observed.items())
            if len(raw_values) > 1
        ]
