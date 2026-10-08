from __future__ import annotations

import json
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
from io import StringIO
from pathlib import Path
from threading import RLock
from typing import Any, Iterator, Optional, cast

import pandas as pd
from dotenv import dotenv_values

from nps_lens.analytics.taxonomy import (
    MODES,
    SOURCE_COLUMNS,
    detect_taxonomy,
    labels,
    signature,
)
from nps_lens.domain.models import UploadContext
from nps_lens.domain.normalization import EquivalenceRegistry, semantic_series
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.classification_protocol import category_catalog, digest, taxonomy_fingerprint
from nps_lens.services.taxonomy_discovery import TaxonomyResponse
from nps_lens.services.taxonomy_prompts import COMMENT_CLASSIFIER_INSTRUCTIONS_VERSION
from nps_lens.settings import persist_ui_prefs

POLICIES = ("ACTIVE_ONLY", "SOURCE_AND_ACTIVE", "ALL_AVAILABLE")


def context_key(context: UploadContext) -> str:
    return context.service_origin


def original_labels(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy(deep=False)
    for column, source in SOURCE_COLUMNS.items():
        out[column] = labels(frame, source if source in frame else column)
    return out


def normalized_label_catalog(
    catalog: dict[str, Any], registry: EquivalenceRegistry
) -> dict[str, Any]:
    pairs = pd.DataFrame(
        [
            (branch["lever"], sub if isinstance(sub, str) else sub["name"])
            for branch in catalog["taxonomy"]
            for sub in branch["sublevers"]
        ],
        columns=["Palanca", "Subpalanca"],
    )
    pairs = registry.apply("nps", pairs).drop_duplicates()
    return {
        "taxonomy": [
            {"lever": lever, "sublevers": sorted(group.Subpalanca.tolist())}
            for lever, group in pairs.groupby("Palanca", sort=True)
        ]
    }


class TaxonomyResolver:
    """The only boundary replacing categorical columns before analytics. Never fits models."""

    def resolve(
        self,
        frame: pd.DataFrame,
        mode: str,
        registry: EquivalenceRegistry,
        artifact: Optional[dict[str, Any]] = None,
    ) -> pd.DataFrame:
        if mode not in MODES:
            raise ValueError("Taxonomía desconocida.")
        out = original_labels(frame)
        if mode in ("COMPLETED", "DISCOVERED"):
            if artifact is None:
                raise ValueError("Primero crea esta taxonomía desde Taxonomy Studio.")
            assignments = pd.DataFrame(
                {
                    "key": artifact["keys"],
                    "lever": artifact["lever"],
                    "sublever": artifact["sublever"],
                }
            ).set_index("key")
            keys = frame["_business_key"]
            out["Palanca"] = keys.map(assignments["lever"]).fillna("")
            out["Subpalanca"] = keys.map(assignments["sublever"]).fillna("")
            if mode == "DISCOVERED":
                out["Canal"] = registry.normalize_series("nps.Canal", out["Canal"])
        if mode in ("SOURCE", "COMPLETED"):
            out = registry.apply("nps", out)
        if mode == "DISCOVERED" and artifact and artifact.get("mode", mode) == "DISCOVERED":
            complete = out["Palanca"].str.strip().ne("") & out["Subpalanca"].str.strip().ne("")
            out.loc[~complete, ["Palanca", "Subpalanca"]] = ""
        out.attrs["taxonomy_mode"] = mode
        return out


class TaxonomyService:
    def __init__(
        self,
        repository: SqliteNpsRepository,
        equivalences_path: Any,
        dotenv_path: Optional[Path] = None,
    ) -> None:
        self.dotenv_path = dotenv_path
        raw = (
            dotenv_values(dotenv_path).get("NPS_LENS_CLASSIFICATION_FRAMEWORKS", "{}")
            if dotenv_path
            else "{}"
        )
        self.frameworks: dict[str, str] = json.loads(raw or "{}")
        self.repository = repository
        self.equivalences_path = equivalences_path
        self.resolver = TaxonomyResolver()
        EquivalenceRegistry.load(equivalences_path)
        self.lens_override: Optional[str] = None
        self._state_cache: dict[str, tuple[tuple[object, ...], dict[str, Any]]] = {}
        self._source_lock = RLock()
        self._source_key: Optional[tuple[object, ...]] = None
        self._source_frame = pd.DataFrame()
        self._cache: OrderedDict[str, dict[str, Any]] = OrderedDict()

    def _source_revision(self) -> tuple[object, ...]:
        revisions = []
        for path in (self.repository.db_path, Path(f"{self.repository.db_path}-wal")):
            try:
                stat = path.stat()
                revisions.append((stat.st_mtime_ns, stat.st_size))
            except FileNotFoundError:
                revisions.append((0, 0))
        return tuple(revisions)

    def state(self, context: UploadContext) -> dict[str, Any]:
        key = context_key(context)
        revision = self._source_revision()
        cached = self._state_cache.get(key)
        if cached and cached[0] == revision:
            return deepcopy(cached[1])
        with self.repository._connect() as connection:
            row = connection.execute(
                "SELECT payload FROM taxonomy_state WHERE context = ?", (context_key(context),)
            ).fetchone()
        state = (
            cast(dict[str, Any], json.loads(row[0]))
            if row
            else {
                "active": "SOURCE",
                "policy": "ACTIVE_ONLY",
                "artifacts": {},
            }
        )

        if not state.get("restored") and key in self.frameworks:
            state["active"] = self.frameworks[key]
        if state.get("active") not in MODES:
            state["active"] = "SOURCE"
        if len(self._state_cache) >= 4:
            self._state_cache.clear()
        self._state_cache[key] = (revision, state)
        return deepcopy(state)

    def save_state(self, context: UploadContext, state: dict[str, Any]) -> None:
        with self.repository._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO taxonomy_state VALUES (?, ?)",
                (context_key(context), json.dumps(state, ensure_ascii=False, allow_nan=False)),
            )

    def registry(self, context: UploadContext) -> EquivalenceRegistry:
        state = self.state(context)
        if state.get("restored"):
            return EquivalenceRegistry.from_dict(state["restored"]["equivalences"])
        if "equivalences" in state:
            return EquivalenceRegistry.from_dict(state["equivalences"])
        return EquivalenceRegistry.load(self.equivalences_path)

    def save_equivalences(self, context: UploadContext, registry: EquivalenceRegistry) -> None:
        state = self.state(context)
        if state.get("restored"):
            raise ValueError("Vuelve al dataset local para modificar conceptos.")
        state["equivalences"] = registry.to_dict()
        self.save_state(context, state)
        self._state_cache.clear()
        self._cache.clear()

    def classification_artifact(
        self, context: UploadContext, mode: str, *, fingerprint: str | None = None
    ) -> dict[str, Any]:
        state = self.state(context)
        signatures = state.get("artifacts" if mode == "DISCOVERED" else "llm_artifacts", {})
        current = self.artifact(signatures.get(mode, "")) or {}
        if fingerprint is not None:
            if (
                current.get("taxonomy_fingerprint") == fingerprint
                and current.get("config", {}).get("instructions_version")
                == COMMENT_CLASSIFIER_INSTRUCTIONS_VERSION
            ):
                return current
            with self.repository._connect() as db:
                row = db.execute(
                    "SELECT signature FROM taxonomy_artifacts WHERE context=? AND mode=? "
                    "AND json_extract(payload, '$.taxonomy_fingerprint')=? "
                    "AND json_extract(payload, '$.config.instructions_version')=? ORDER BY rowid DESC LIMIT 1",
                    (
                        context_key(context),
                        mode,
                        fingerprint,
                        COMMENT_CLASSIFIER_INSTRUCTIONS_VERSION,
                    ),
                ).fetchone()
            return (self.artifact(row[0]) or {}) if row else {}
        return current

    def clear_source_cache(self) -> None:
        with self._source_lock:
            self._source_key = None
            self._source_frame = pd.DataFrame()

    def source(self, context: UploadContext) -> pd.DataFrame:
        # Share one corpus load across dashboard, Studio and equivalence requests.
        # Keep only the latest owner/revision; callers receive independent frames.
        with self._source_lock:
            key = (context_key(context), self._source_revision())
            if key != self._source_key:
                restored = self.state(context).get("restored")
                if restored:
                    frame = pd.read_json(StringIO(json.dumps(restored["records"])), orient="table")
                    frame["Fecha"] = pd.to_datetime(frame["Fecha"], errors="coerce")
                else:
                    frame = self.repository.load_records_df(context)
                self._source_frame = frame
                self._source_key = key
            return self._source_frame.copy()

    def artifact(self, sig: str) -> Optional[dict[str, Any]]:
        if sig in self._cache:
            self._cache.move_to_end(sig)
            return self._cache[sig]
        with self.repository._connect() as connection:
            row = connection.execute(
                "SELECT payload FROM taxonomy_artifacts WHERE signature = ?", (sig,)
            ).fetchone()
        if not row:
            return None
        value = cast(dict[str, Any], json.loads(row[0]))
        self._cache[sig] = value
        while len(self._cache) > 2:
            self._cache.popitem(last=False)
        return value

    def available(
        self, context: UploadContext, frame: pd.DataFrame, registry: EquivalenceRegistry
    ) -> dict[str, Optional[dict[str, Any]]]:
        state = self.state(context)
        if state.get("restored"):
            return cast(dict[str, Optional[dict[str, Any]]], state["restored"]["taxonomies"])
        available: dict[str, Optional[dict[str, Any]]] = {"SOURCE": None}
        for mode, sig in state.get("artifacts", {}).items():
            if mode not in MODES:
                continue
            item = self.artifact(sig)
            if item:
                config = item.get("config", {})
                if (
                    config.get("method") == "chatgpt_zip"
                    and config.get("instructions_version")
                    != COMMENT_CLASSIFIER_INSTRUCTIONS_VERSION
                ):
                    continue
                if mode == "DISCOVERED" and item.get("taxonomy") != state.get(
                    "discovered_taxonomy"
                ):
                    continue
                available[mode] = item
        if state.get("discovered_taxonomy") and "DISCOVERED" not in available:
            available["DISCOVERED"] = {
                "taxonomy": state["discovered_taxonomy"],
                "keys": [],
                "lever": [],
                "sublever": [],
            }
        return available

    def resolve(
        self,
        context: UploadContext,
        frame: Optional[pd.DataFrame] = None,
        mode: Optional[str] = None,
    ) -> pd.DataFrame:
        frame = self.source(context) if frame is None else frame
        registry = self.registry(context)
        available = self.available(context, frame, registry)
        state = self.state(context)
        resolved = self._resolve_available(frame, mode, registry, available, state)
        selected = resolved.attrs["taxonomy_mode"]
        engine = "llm" if selected == "DISCOVERED" else "rules"
        if (
            not state.get("restored")
            and selected != "DISCOVERED"
            and state.get("comment_engine") == "llm"
        ):
            from nps_lens.services.taxonomy_exchange import TaxonomyExchange

            resolved = TaxonomyExchange(self, Path(".")).apply(context, resolved, selected)
            engine = "llm"
        if state.get("restored"):
            resolved.attrs["classification_signature"] = (available[selected] or {}).get(
                "classification_signature", state["restored"]["checksum"]
            )
            return resolved
        parent = (
            self.classification_artifact(context, selected)
            if engine == "llm"
            else available[selected]
        )
        if (parent or {}).get("config", {}).get("method") == "chatgpt_zip":
            engine = "llm"
        ordered = resolved.sort_values("_business_key")
        payload = {
            "mode": selected,
            "engine": engine,
            "taxonomy_signature": (parent or {}).get("signature", ""),
            "keys": ordered["_business_key"].tolist(),
            "lever": labels(ordered, "Palanca").tolist(),
            "sublever": labels(ordered, "Subpalanca").tolist(),
            "channel": labels(ordered, "Canal").tolist(),
            "provenance": "llm" if engine == "llm" else selected.lower(),
            "prompt_version": (parent or {}).get("config", {}).get("instructions_version"),
            "model_version": (parent or {}).get("model_version"),
            "input_signature": signature(frame, selected, {}),
        }
        sig = digest(payload)
        if self.artifact(sig) is None:
            payload["signature"] = sig
            with self.repository._connect() as connection:
                connection.execute(
                    "INSERT OR IGNORE INTO taxonomy_artifacts VALUES (?, ?, ?, ?)",
                    (sig, context_key(context), selected, json.dumps(payload, ensure_ascii=False)),
                )
            self._cache[sig] = payload
        resolved.attrs["classification_signature"] = sig
        resolved.attrs["classification_engine"] = engine
        return resolved

    def _resolve_available(
        self,
        frame: pd.DataFrame,
        mode: Optional[str],
        registry: EquivalenceRegistry,
        available: dict[str, Optional[dict[str, Any]]],
        state: dict[str, Any],
    ) -> pd.DataFrame:
        selected = mode or self.lens_override or state["active"]
        if selected not in available:
            if mode:
                raise ValueError(
                    "Taxonomía ausente o desactualizada: crea o regenera antes de usarla."
                )
            selected = "SOURCE"
        item = available[selected]
        # Frozen lenses carry their own assignments and equivalences.
        if state.get("restored") and item:
            out = self.resolver.resolve(frame, "DISCOVERED", registry, {**item, "mode": selected})
            if "channel" in item:
                lookup = pd.Series(item["channel"], index=item["keys"])
                out["Canal"] = frame["_business_key"].map(lookup)
            out.attrs["taxonomy_mode"] = selected
            return out
        resolved = self.resolver.resolve(frame, selected, registry, item)
        if selected == "DISCOVERED" and item and item.get("comment_hashes"):
            hashes = frame["Comment"].fillna("").map(lambda value: digest(str(value)))
            valid = frame["_business_key"].map(item["comment_hashes"]) == hashes
            resolved.loc[~valid, ["Palanca", "Subpalanca"]] = ""
        return resolved

    def catalog(
        self, context: UploadContext, mode: str, *, normalized: bool = True, semantic: bool = True
    ) -> dict[str, Any]:
        if mode not in MODES:
            raise ValueError("Taxonomía desconocida.")
        state = self.state(context)
        if normalized and state.get("restored"):
            frozen = state["restored"]["taxonomies"].get(mode, {}).get("taxonomy")
            if frozen:
                return cast(dict[str, Any], frozen)
        item = self.artifact(state.get("artifacts", {}).get(mode, ""))
        if mode == "DISCOVERED" and state.get("discovered_taxonomy"):
            catalog = state["discovered_taxonomy"]
        elif item and item.get("taxonomy"):
            catalog = item["taxonomy"]
        else:
            frame = (
                original_labels(self.source(context))
                if mode == "SOURCE"
                else self.resolve(context, mode=mode)
            )
            pairs = frame.loc[
                labels(frame, "Palanca").ne("") & labels(frame, "Subpalanca").ne(""),
                ["Palanca", "Subpalanca"],
            ].drop_duplicates()
            catalog = {
                "taxonomy": [
                    {"lever": lever, "sublevers": sorted(group.Subpalanca.tolist())}
                    for lever, group in pairs.groupby("Palanca", sort=True)
                ]
            }
        if mode == "DISCOVERED":
            try:
                TaxonomyResponse.model_validate(catalog)
            except ValueError as exc:
                raise ValueError(
                    "La taxonomía diseñada no contiene criterion; regenera con el diseñador actual."
                ) from exc
        if normalized and mode in ("SOURCE", "COMPLETED"):
            catalog = normalized_label_catalog(catalog, self.registry(context))
        if semantic and normalized and mode in ("SOURCE", "COMPLETED"):
            enrichment = (
                state.get("semantic_catalogs", {}).get(mode, {}).get(taxonomy_fingerprint(catalog))
            )
            if enrichment:
                catalog = enrichment["taxonomy"]
        return cast(dict[str, Any], catalog)

    def semantic_status(self, context: UploadContext, mode: str) -> dict[str, Any]:
        base = self.catalog(context, mode, semantic=False)
        entry = (
            self.state(context)
            .get("semantic_catalogs", {})
            .get(mode, {})
            .get(taxonomy_fingerprint(base), {})
        )
        total = sum(len(branch["sublevers"]) for branch in base["taxonomy"])
        return {
            "total": total,
            "received": total if entry else 0,
            "pending": 0 if entry else total,
            "levers": len(base["taxonomy"]),
            "sublevers": total,
            "taxonomy_fingerprint": taxonomy_fingerprint(entry.get("taxonomy", base)),
            "review": entry.get("review"),
        }

    def manual_draft(self, context: UploadContext, template: str = "CURRENT") -> dict[str, Any]:
        state = self.state(context)
        if template == "CURRENT":
            item = self.artifact(state.get("artifacts", {}).get("COMPLETED", "")) or {}
            if item.get("taxonomy"):
                return cast(dict[str, Any], item["taxonomy"])
            template = (
                "SOURCE"
                if self.catalog(context, "SOURCE", normalized=False)["taxonomy"]
                else "DISCOVERED"
            )
        if template == "NONE":
            return {"taxonomy": []}
        if template not in ("SOURCE", "DISCOVERED"):
            raise ValueError("Plantilla desconocida.")
        if template == "DISCOVERED":
            catalog = (
                self.catalog(context, "DISCOVERED")
                if state.get("discovered_taxonomy")
                else {"taxonomy": []}
            )
            return {
                "taxonomy": [
                    {
                        "lever": branch["lever"],
                        "sublevers": [sub["name"] for sub in branch["sublevers"]],
                    }
                    for branch in catalog["taxonomy"]
                ]
            }
        return self.catalog(context, template, normalized=False)

    def manual_info(self, context: UploadContext, template: str = "CURRENT") -> dict[str, Any]:
        state = self.state(context)
        revision = state.get("artifacts", {}).get("COMPLETED", "")
        item = self.artifact(revision) or {}
        return {
            **self.manual_draft(context, template),
            "revision": revision,
            "exists": bool(item.get("taxonomy")),
            "templates": ["NONE"]
            + [
                mode
                for mode in ("SOURCE", "DISCOVERED")
                if self.manual_draft(context, mode)["taxonomy"]
            ],
            "affected_comments": sum(
                bool(a and b)
                for a, b in zip(item.get("lever", []), item.get("sublever", []), strict=False)
            ),
        }

    def save_manual(
        self, context: UploadContext, branches: list[dict[str, Any]], *, template: str = "CURRENT"
    ) -> dict[str, Any]:
        state = self.state(context)
        if state.get("restored"):
            raise ValueError("Snapshot inmutable: vuelve al dataset local.")
        pairs: set[tuple[str, str]] = set()
        seen = set()
        mapping = {}
        for branch in branches:
            lever, subs = branch.get("lever"), branch.get("sublevers")
            if (
                not isinstance(lever, str)
                or not lever.strip()
                or not isinstance(subs, list)
                or not subs
            ):
                raise ValueError("Cada Palanca requiere un nombre y al menos una Subpalanca.")
            lever = lever.strip()
            if lever in seen:
                raise ValueError("Palancas duplicadas.")
            seen.add(lever)
            if not all(isinstance(sub, str) for sub in subs):
                raise ValueError("Las Subpalancas deben ser texto.")
            cleaned = [sub.strip() for sub in subs]
            if any(not sub for sub in cleaned) or len(set(cleaned)) != len(cleaned):
                raise ValueError("Subpalancas vacías o duplicadas.")
            pairs.update((lever, sub) for sub in cleaned)
            old = branch.get("previous_lever", lever)
            previous = branch.get("previous_sublevers", cleaned)
            if (
                not isinstance(old, str)
                or not isinstance(previous, list)
                or not all(isinstance(sub, str) for sub in previous)
                or len(previous) != len(cleaned)
            ):
                raise ValueError("Correspondencia de edición inválida.")
            mapping.update(
                {
                    (old, before): (lever, after)
                    for before, after in zip(previous, cleaned, strict=False)
                    if old and before
                }
            )
        if not pairs:
            raise ValueError("Crea al menos una Palanca/Subpalanca.")
        registry = self.registry(context)
        frame = (
            original_labels(self.source(context))
            .sort_values("_business_key")
            .reset_index(drop=True)
        )
        available = self.available(context, self.source(context), registry)
        base_mode = template
        if base_mode == "CURRENT":
            base_mode = "COMPLETED" if "COMPLETED" in available else "SOURCE"
            if (
                base_mode == "SOURCE"
                and not self.catalog(context, "SOURCE")["taxonomy"]
                and "DISCOVERED" in available
            ):
                base_mode = "DISCOVERED"
        if base_mode not in ("NONE", "SOURCE", "COMPLETED", "DISCOVERED"):
            raise ValueError("Plantilla desconocida.")
        if base_mode == "DISCOVERED" and base_mode not in available:
            base_mode = "NONE"
        # Editing operates on raw assignments; normalization is only a lens operation.
        base = (
            original_labels(frame)
            if base_mode in ("SOURCE", "NONE") or base_mode not in available
            else self.resolver.resolve(
                frame,
                base_mode,
                EquivalenceRegistry.from_dict({"dimensions": {}}),
                available[base_mode],
            )
        )
        assigned = [
            mapping.get(pair, pair)
            for pair in zip(labels(base, "Palanca"), labels(base, "Subpalanca"), strict=False)
        ]
        assigned = [
            pair if base_mode != "NONE" and pair in pairs else ("", "") for pair in assigned
        ]
        taxonomy = {
            "taxonomy": [
                {
                    "lever": lever,
                    "sublevers": sorted(sub for parent, sub in pairs if parent == lever),
                }
                for lever in sorted({parent for parent, _ in pairs})
            ]
        }
        config = {
            "method": "manual",
            "revision": uuid.uuid4().hex,
            "taxonomy": taxonomy,
            "mapping": sorted((a, b) for a, b in mapping.items()),
        }
        sig = signature(frame, "COMPLETED", config, "")
        artifact = {
            "mode": "COMPLETED",
            "signature": sig,
            "keys": frame["_business_key"].tolist(),
            "config": config,
            "lever": [a for a, _ in assigned],
            "sublever": [b for _, b in assigned],
            "taxonomy": taxonomy,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "nodes": [],
            "equivalences": {},
        }
        inherited_catalog = self.catalog(context, base_mode) if base_mode != "NONE" else None
        inherited_assignments = {}
        if inherited_catalog:
            from nps_lens.services.taxonomy_exchange import TaxonomyExchange

            expected = {
                (row["lever"], row["sublever"])
                for row in category_catalog(inherited_catalog).values()
            }
            normalized_manual = normalized_label_catalog(taxonomy, registry)
            manual_pairs = {
                (category["lever"], category["sublever"])
                for category in category_catalog(normalized_manual).values()
            }
            if expected == manual_pairs:
                if any(
                    isinstance(sub, dict)
                    for branch in inherited_catalog["taxonomy"]
                    for sub in branch["sublevers"]
                ):
                    state.setdefault("semantic_catalogs", {}).setdefault("COMPLETED", {})[
                        taxonomy_fingerprint(normalized_manual)
                    ] = {
                        "taxonomy": inherited_catalog,
                    }
                inherited_assignments = TaxonomyExchange(self, Path(".")).assignments(
                    context, frame, base_mode
                )
        state.setdefault("artifacts", {})["COMPLETED"] = sig
        with self.repository._connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO taxonomy_artifacts VALUES (?, ?, ?, ?)",
                (sig, context_key(context), "COMPLETED", json.dumps(artifact, ensure_ascii=False)),
            )
            db.execute(
                "INSERT OR REPLACE INTO taxonomy_state VALUES (?, ?)",
                (context_key(context), json.dumps(state, ensure_ascii=False)),
            )
        self._cache.clear()
        self._state_cache.clear()
        if inherited_assignments:
            exchange = TaxonomyExchange(self, Path("."))
            with self.repository._connect() as db:
                exchange._persist_assignments(
                    db,
                    context,
                    frame,
                    {
                        "mode": "COMPLETED",
                        "taxonomy": inherited_catalog,
                        "manual_revision": sig,
                        "instructions_version": COMMENT_CLASSIFIER_INSTRUCTIONS_VERSION,
                    },
                    inherited_assignments,
                )
            exchange._clear_caches()
        return taxonomy

    def guard_export(self, context: UploadContext, mode: str) -> None:
        state = self.state(context)
        proposal = state.get("proposed_discovered_taxonomy")
        if (
            mode == "DISCOVERED"
            and proposal
            and taxonomy_fingerprint(proposal) != taxonomy_fingerprint(self.catalog(context, mode))
        ):
            raise ValueError(
                "Acepta o descarta explícitamente la propuesta DISCOVERED antes de exportar."
            )

    def configure(self, context: UploadContext, changes: dict[str, Any]) -> dict[str, Any]:
        state = self.state(context)
        if (
            changes.get("accept_proposal") or changes.get("discard_proposal")
        ) and "active" in changes:
            raise ValueError("Revisa la propuesta y cambia el Marco en acciones separadas.")
        if changes.get("accept_proposal") and changes.get("discard_proposal"):
            raise ValueError("Elige aceptar o descartar la propuesta.")
        if changes.get("discard_proposal"):
            state.pop("proposed_discovered_taxonomy", None)
            state.pop("proposed_discovered_fingerprint", None)
        if changes.get("accept_proposal") and state.get("proposed_discovered_taxonomy"):
            state["discovered_taxonomy"] = state.pop("proposed_discovered_taxonomy")
            state["taxonomy_fingerprint"] = state.pop("proposed_discovered_fingerprint")
            for field in ("artifacts", "llm_artifacts"):
                artifacts = state.get(field, {})
                item = self.artifact(artifacts.get("DISCOVERED", ""))
                if item and item.get("taxonomy_fingerprint") != state["taxonomy_fingerprint"]:
                    artifacts.pop("DISCOVERED", None)
        if "active" in changes:
            frame, registry = self.source(context), self.registry(context)
            available = self.available(context, frame, registry)
            mode = changes["active"]
            if mode not in available or not self.catalog(context, mode)["taxonomy"]:
                raise ValueError(
                    "La taxonomía seleccionada debe estar disponible y contener categorías."
                )
            state["active"] = mode
        if "policy" in changes:
            if changes["policy"] not in POLICIES:
                raise ValueError("Política de snapshot desconocida.")
            state["policy"] = changes["policy"]
        if "active" in changes:
            preferences = {**self.frameworks, context_key(context): state["active"]}
            persist_ui_prefs(
                self.dotenv_path,
                {"classification_frameworks": json.dumps(preferences, ensure_ascii=False)},
            )
            self.frameworks = preferences
        self.save_state(context, state)
        return {key: state[key] for key in ("active", "policy")}

    def studio(self, context: UploadContext) -> dict[str, Any]:
        frame, registry = self.source(context), self.registry(context)
        available = self.available(context, frame, registry)
        state = self.state(context)
        cards = []
        for mode in MODES:
            if mode not in available:
                cards.append(
                    {"mode": mode, "available": False, "stale": mode in state.get("artifacts", {})}
                )
                continue
            resolved = self._resolve_available(frame, mode, registry, available, state)
            classified = labels(resolved, "Palanca").str.strip().ne("") & labels(
                resolved, "Subpalanca"
            ).str.strip().ne("")
            item = available[mode] or {}
            cards.append(
                {
                    "mode": mode,
                    "available": True,
                    "selectable": mode != "SOURCE" or bool(classified.any()),
                    "levers": (
                        len(item["taxonomy"]["taxonomy"])
                        if item.get("taxonomy")
                        else int(labels(resolved, "Palanca").replace("", pd.NA).nunique())
                    ),
                    "sublevers": (
                        sum(len(branch["sublevers"]) for branch in item["taxonomy"]["taxonomy"])
                        if item.get("taxonomy")
                        else int(
                            resolved.loc[classified, ["Palanca", "Subpalanca"]]
                            .drop_duplicates()
                            .shape[0]
                        )
                    ),
                    "coverage": float(classified.mean()) if len(frame) else 0,
                    "equivalence_groups": sum(
                        len(v)
                        for k, v in registry.to_dict()["dimensions"].items()
                        if k.startswith("nps.")
                    ),
                    "created_at": item.get("created_at"),
                }
            )
        selected = state["active"] if state["active"] in available else "SOURCE"
        active_fingerprint = taxonomy_fingerprint(self.catalog(context, selected))
        return {
            "detection": {
                **detect_taxonomy(frame),
                "originals_unavailable": int(
                    frame.get("source_preserved", pd.Series(1, index=frame.index)).eq(0).sum()
                ),
            },
            "taxonomies": cards,
            "active": selected,
            "requested_active": state["active"],
            "policy": state["policy"],
            "restored": bool(state.get("restored")),
            "discovered_catalog_available": bool(state.get("discovered_taxonomy")),
            "active_fingerprint": active_fingerprint,
            "proposed_discovered_fingerprint": state.get("proposed_discovered_fingerprint"),
            "proposed_discovered_taxonomy": state.get("proposed_discovered_taxonomy"),
            "designer_review": state.get("designer_review"),
        }

    def explore(
        self, context: UploadContext, mode: str, offset: int = 0, limit: int = 100
    ) -> dict[str, Any]:
        state = self.state(context)
        if (
            mode == "DISCOVERED"
            and state.get("proposed_discovered_taxonomy")
            and not state.get("discovered_taxonomy")
            and not state.get("restored")
        ):
            return {
                "mode": mode,
                "rows": [],
                "total": 0,
                "note": "Revisa la propuesta pendiente en Crear Taxonomía.",
            }
        frame = self.resolve(context, mode=mode)
        if (
            mode == "DISCOVERED"
            and not (labels(frame, "Palanca").ne("") & labels(frame, "Subpalanca").ne("")).any()
        ):
            return {
                "mode": mode,
                "rows": [],
                "total": 0,
                "note": "Clasifica comentarios con esta taxonomía para ver volumen, NPS y ejemplos.",
            }
        work = pd.DataFrame(
            {
                "Palanca": labels(frame, "Palanca").replace("", "Sin clasificar"),
                "Subpalanca": labels(frame, "Subpalanca").replace("", "Sin clasificar"),
                "score": pd.to_numeric(frame["NPS"], errors="coerce"),
            }
        )
        work["promoters"], work["neutrals"], work["detractors"] = (
            work.score.ge(9),
            work.score.between(7, 8),
            work.score.le(6),
        )
        summary = (
            work.groupby(["Palanca", "Subpalanca"], sort=True, observed=True)
            .agg(
                volume=("score", "size"),
                score=("score", "mean"),
                promoters=("promoters", "sum"),
                neutrals=("neutrals", "sum"),
                detractors=("detractors", "sum"),
            )
            .reset_index()
        )
        summary["share"] = summary.volume / max(len(frame), 1)
        summary["nps"] = (summary.promoters - summary.detractors) / summary.volume * 100
        item = self.available(context, self.source(context), self.registry(context)).get(mode) or {}
        nodes = {(n["parent"], n["label"]): n for n in item.get("nodes", [])}
        rows = summary.iloc[offset : offset + min(limit, 100)].to_dict("records")
        criteria = {
            (category["lever"], category["sublever"]): category["criterion"]
            for category in category_catalog(self.catalog(context, mode)).values()
        }
        for row in rows:
            row["criterion"] = criteria.get((row["Palanca"], row["Subpalanca"]), "")
            row["examples"] = nodes.get((row["Palanca"], row["Subpalanca"]), {}).get("examples", [])
            row["provenance"] = mode
        # Heuristics are reported as review suggestions, not errors.
        multi = work.groupby("Subpalanca")["Palanca"].nunique()
        return {
            "mode": mode,
            "rows": rows,
            "total": len(summary),
            "audit": {
                "multi_parent_sublevers": multi[multi.gt(1)].index.tolist()[:50],
                "generic_labels": summary.loc[
                    summary.Palanca.str.casefold().isin(["otros", "general", "sin clasificar"]),
                    "Palanca",
                ]
                .unique()
                .tolist(),
                "note": "Indicios para revisar; no son errores confirmados.",
            },
            "equivalences": item.get(
                "equivalences", self.registry(context).to_dict() if mode == "SOURCE" else {}
            ),
        }

    def compare(
        self, context: UploadContext, left: str, right: str, offset: int = 0, limit: int = 100
    ) -> dict[str, Any]:
        source = self.source(context)
        a, b = self.resolve(context, source, left), self.resolve(context, source, right)
        classified = []
        for frame in (a, b):
            categories = frame[["_business_key", "Palanca", "Subpalanca"]].set_index(
                "_business_key"
            )
            valid = categories.apply(semantic_series).ne("").all(axis=1)
            classified.append(categories.loc[valid])
        pairs = (
            classified[0]
            .rename(columns={"Palanca": "from_lever", "Subpalanca": "from_sublever"})
            .join(
                classified[1].rename(columns={"Palanca": "to_lever", "Subpalanca": "to_sublever"}),
                how="inner",
            )
        )
        counts = cast(
            "pd.Series[int]", pairs.groupby(list(pairs.columns), observed=True).size()
        ).reset_index(name="volume")
        counts["share"] = counts.volume / counts.groupby(
            ["from_lever", "from_sublever"]
        ).volume.transform("sum")
        return {
            "left": left,
            "right": right,
            "rows": counts.iloc[offset : offset + min(limit, 100)].to_dict("records"),
            "total": len(source),
            "left_classified": len(classified[0]),
            "right_classified": len(classified[1]),
            "comparable": len(pairs),
            "groups": len(counts),
            "note": (
                "Distribución de las mismas respuestas entre lentes; una dispersión puede sugerir mezcla temática."
                if len(pairs)
                else "No hay respuestas comparables. Clasifica primero los comentarios con ambas taxonomías."
            ),
        }

    def snapshot(
        self,
        context: UploadContext,
        helix: Optional[pd.DataFrame] = None,
        analysis_config: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        frame, registry, state = self.source(context), self.registry(context), self.state(context)
        available = self.available(context, frame, registry)
        active = state["active"]
        if active not in available:
            raise ValueError(
                "El marco de clasificación no está disponible para guardar el snapshot."
            )
        modes = (
            list(available)
            if state["policy"] == "ALL_AVAILABLE"
            else (
                list(dict.fromkeys(["SOURCE", active]))
                if state["policy"] == "SOURCE_AND_ACTIVE"
                else [active]
            )
        )
        taxonomies = {}
        for mode in modes:
            resolved = self.resolve(context, frame, mode)
            item = dict(available[mode] or {})
            item.update(
                {
                    "taxonomy": self.catalog(context, mode),
                    "classification_signature": resolved.attrs["classification_signature"],
                    "keys": frame["_business_key"].tolist(),
                    "lever": labels(resolved, "Palanca").tolist(),
                    "sublever": labels(resolved, "Subpalanca").tolist(),
                    "channel": labels(resolved, "Canal").tolist(),
                }
            )
            taxonomies[mode] = item
        payload = {
            "schema_version": "taxonomy-1",
            "analysis_config": analysis_config
            or state.get("restored", {}).get("analysis_config", {}),
            "context": asdict(context),
            "active": active,
            "policy": state["policy"],
            "equivalences": registry.to_dict(),
            "taxonomies": taxonomies,
            "records": json.loads(frame.to_json(orient="table", date_format="iso")),
            "helix": (
                json.loads(helix.to_json(orient="table", date_format="iso"))
                if helix is not None
                else None
            ),
        }
        payload["checksum"] = sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        return payload

    def restore(self, context: UploadContext, payload: dict[str, Any]) -> None:
        checksum = payload.get("checksum")
        body = {k: v for k, v in payload.items() if k != "checksum"}
        if (
            payload.get("schema_version") != "taxonomy-1"
            or sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            != checksum
        ):
            raise ValueError("Snapshot inválido o alterado.")
        if payload.get("active") not in payload.get("taxonomies", {}):
            raise ValueError("Faltan las asignaciones de la lente activa.")
        frame = pd.read_json(StringIO(json.dumps(payload["records"])), orient="table")
        if "_business_key" not in frame or not frame["_business_key"].is_unique:
            raise ValueError("Identidades del snapshot inválidas.")
        registry = EquivalenceRegistry.from_dict(payload["equivalences"])
        for mode, item in payload["taxonomies"].items():
            if mode not in MODES:
                raise ValueError("Taxonomía desconocida en snapshot.")
            if set(item.get("keys", [])) != set(frame["_business_key"]):
                raise ValueError("Faltan asignaciones del corpus en el snapshot.")
            self.resolver.resolve(frame, "DISCOVERED", registry, {**item, "mode": mode})
        state = self.state(context)
        state.update(
            {
                "restored": payload,
                "active": payload["active"],
                "policy": payload["policy"],
            }
        )
        self.save_state(context, state)

    def resume_local(self, context: UploadContext) -> None:
        state = self.state(context)
        state.pop("restored", None)
        state["active"] = self.frameworks.get(context_key(context), "SOURCE")
        self.save_state(context, state)

    @contextmanager
    def snapshot_lens(self, context: UploadContext) -> Iterator[None]:
        previous = self.lens_override
        self.lens_override = self.state(context)["active"]
        try:
            self.resolve(context, mode=self.lens_override)
            yield
        finally:
            self.lens_override = previous
