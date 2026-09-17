from __future__ import annotations

import json
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
from io import StringIO
from typing import Any, Iterator, Optional, cast

import pandas as pd

from nps_lens.analytics.taxonomy import (
    MODES,
    SOURCE_COLUMNS,
    TaxonomyConfig,
    complete,
    detect_taxonomy,
    labels,
    signature,
)
from nps_lens.domain.models import UploadContext
from nps_lens.domain.normalization import EquivalenceRegistry
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.taxonomy_discovery import TaxonomyDiscoveryProvider

POLICIES = ("ACTIVE_ONLY", "SOURCE_AND_ACTIVE", "ALL_AVAILABLE")


def context_key(context: UploadContext) -> str:
    return json.dumps(asdict(context), sort_keys=True, ensure_ascii=False)


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
        out = frame.copy(deep=False)
        for column, source in SOURCE_COLUMNS.items():
            out[column] = labels(frame, source if source in frame else column)
        if mode == "NORMALIZED":
            out = registry.apply("nps", out)
        elif mode in ("COMPLETED", "DISCOVERED"):
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
            if not keys.isin(assignments.index).all():
                raise ValueError(
                    "La taxonomía no corresponde a este corpus; regenera antes de usarla."
                )
            out["Palanca"] = keys.map(assignments["lever"]).fillna("")
            out["Subpalanca"] = keys.map(assignments["sublever"]).fillna("")
            out["Canal"] = registry.normalize_series("nps.Canal", out["Canal"])
        out.attrs["taxonomy_mode"] = mode
        return out


class TaxonomyService:
    def __init__(
        self,
        repository: SqliteNpsRepository,
        equivalences_path: Any,
        discovery_provider: Optional[TaxonomyDiscoveryProvider] = None,
    ) -> None:
        self.repository = repository
        self.equivalences_path = equivalences_path
        self.resolver = TaxonomyResolver()
        EquivalenceRegistry.load(equivalences_path)
        self.lens_override: Optional[str] = None
        self._state_cache: dict[str, tuple[int, dict[str, Any]]] = {}
        self._cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.discovery_provider = discovery_provider

    def set_discovery_provider(
        self, discovery_provider: Optional[TaxonomyDiscoveryProvider]
    ) -> None:
        if self.discovery_provider is not None:
            if discovery_provider is not None and (
                self.discovery_provider.signature_config() == discovery_provider.signature_config()
            ):
                return
            self.discovery_provider.disconnect()
        self.discovery_provider = discovery_provider

    def discovery_status(self, *, check_session: bool = False) -> dict[str, Any]:
        if self.discovery_provider is None:
            return {"method": "local", "session": "not_connected"}
        return {
            **self.discovery_provider.signature_config(),
            "session": self.discovery_provider.session_status(),
        }

    def connect_discovery(self) -> dict[str, Any]:
        if self.discovery_provider is None:
            raise ValueError("Selecciona ChatGPT automatizado antes de conectar.")
        return {
            **self.discovery_provider.signature_config(),
            "session": self.discovery_provider.connect(),
        }

    def verify_discovery_connection(self) -> dict[str, Any]:
        if self.discovery_provider is None:
            raise ValueError("Selecciona ChatGPT automatizado antes de verificar.")
        return {
            **self.discovery_provider.signature_config(),
            "session": self.discovery_provider.verify_connection(),
        }

    def disconnect_discovery(self) -> dict[str, Any]:
        if self.discovery_provider is not None:
            self.discovery_provider.disconnect()
        return self.discovery_status(check_session=True)

    def state(self, context: UploadContext) -> dict[str, Any]:
        key = context_key(context)
        revision = self.repository.db_path.stat().st_mtime_ns
        cached = self._state_cache.get(key)
        if cached and cached[0] == revision:
            return dict(cached[1])
        with self.repository._connect() as connection:
            row = connection.execute(
                "SELECT payload FROM taxonomy_state WHERE context = ?", (context_key(context),)
            ).fetchone()
        state = (
            cast(dict[str, Any], json.loads(row[0]))
            if row
            else {
                "active": "NORMALIZED",
                "default": "NORMALIZED",
                "policy": "ACTIVE_ONLY",
                "artifacts": {},
            }
        )

        if len(self._state_cache) >= 4:
            self._state_cache.clear()
        self._state_cache[key] = (revision, state)
        return dict(state)

    def save_state(self, context: UploadContext, state: dict[str, Any]) -> None:
        with self.repository._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO taxonomy_state VALUES (?, ?)",
                (context_key(context), json.dumps(state, ensure_ascii=False, allow_nan=False)),
            )

    def registry(self, context: UploadContext) -> EquivalenceRegistry:
        restored = self.state(context).get("restored")
        return (
            EquivalenceRegistry.from_dict(restored["equivalences"])
            if restored
            else EquivalenceRegistry.load(self.equivalences_path)
        )

    def source(self, context: UploadContext) -> pd.DataFrame:
        restored = self.state(context).get("restored")
        if restored:
            frame = pd.read_json(StringIO(json.dumps(restored["records"])), orient="table")
            frame["Fecha"] = pd.to_datetime(frame["Fecha"], errors="coerce")
            return frame
        return self.repository.load_records_df(context)

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
        available: dict[str, Optional[dict[str, Any]]] = {"SOURCE": None, "NORMALIZED": None}
        for mode, sig in state.get("artifacts", {}).items():
            item = self.artifact(sig)
            if item:
                base = self.resolver.resolve(frame, "NORMALIZED", registry)
                config: TaxonomyConfig | dict[str, Any] = (
                    TaxonomyConfig(**item["config"])
                    if mode == "COMPLETED"
                    else cast(dict[str, Any], item["config"])
                )
                expected = signature(base, mode, config, registry.signature("nps"))
                if sig == expected:
                    available[mode] = item
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
        selected = mode or self.lens_override or self.state(context)["active"]
        if selected not in available:
            if mode:
                raise ValueError(
                    "Taxonomía ausente o desactualizada: crea o regenera antes de usarla."
                )
            selected = "NORMALIZED"
        item = available[selected]
        # Frozen SOURCE/NORMALIZED also carry assignments, independent of current equivalences.
        if self.state(context).get("restored") and item:
            out = self.resolver.resolve(frame, "COMPLETED", registry, item)
            if "channel" in item:
                lookup = pd.Series(item["channel"], index=item["keys"])
                out["Canal"] = frame["_business_key"].map(lookup)
            out.attrs["taxonomy_mode"] = selected
            return out
        return self.resolver.resolve(frame, selected, registry, item)

    def generate(
        self,
        context: UploadContext,
        mode: str,
        config: Optional[TaxonomyConfig] = None,
        regenerate: bool = False,
    ) -> dict[str, Any]:
        if mode not in ("COMPLETED", "DISCOVERED"):
            raise ValueError("Solo COMPLETED y DISCOVERED requieren generación.")
        state = self.state(context)
        if state.get("restored"):
            raise ValueError("Snapshot inmutable: vuelve al dataset local antes de generar.")
        registry = self.registry(context)
        frame = (
            self.resolver.resolve(self.source(context), "NORMALIZED", registry)
            .sort_values("_business_key")
            .reset_index(drop=True)
        )
        provider = self.discovery_provider
        if mode == "COMPLETED":
            artifact_config: dict[str, Any] = asdict(config or TaxonomyConfig())
        else:
            if provider is None:
                raise ValueError("Selecciona ChatGPT automatizado en Taxonomy Studio.")
            artifact_config = provider.signature_config()
        sig = signature(frame, mode, artifact_config, registry.signature("nps"))
        artifact = None if regenerate else self.artifact(sig)
        hit = artifact is not None
        if artifact is None:
            if mode == "COMPLETED":
                artifact = complete(frame, config or TaxonomyConfig())
            else:
                assert provider is not None
                comments = list(
                    zip(
                        frame["_business_key"].astype(str).tolist(),
                        frame["Comment"].astype("string").fillna("").tolist(),
                    )
                )
                artifact = provider.discover(comments)
            artifact.update(
                {
                    "mode": mode,
                    "signature": sig,
                    "keys": frame["_business_key"].tolist(),
                    "config": artifact_config,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "equivalences": registry.to_dict() if mode == "COMPLETED" else {},
                }
            )
            state.setdefault("artifacts", {})[mode] = sig
            # Artifact and active state become visible in the same SQLite transaction.
            with self.repository._connect() as connection:
                connection.execute(
                    "INSERT OR REPLACE INTO taxonomy_artifacts VALUES (?, ?, ?, ?)",
                    (
                        sig,
                        context_key(context),
                        mode,
                        json.dumps(artifact, ensure_ascii=False, allow_nan=False),
                    ),
                )
                connection.execute(
                    "INSERT OR REPLACE INTO taxonomy_state VALUES (?, ?)",
                    (context_key(context), json.dumps(state, ensure_ascii=False, allow_nan=False)),
                )
            self._cache[sig] = artifact
        else:
            state.setdefault("artifacts", {})[mode] = sig
            self.save_state(context, state)
        return {
            "mode": mode,
            "signature": sig,
            "cache_hit": hit,
            "quality": artifact.get("quality", []),
        }

    def configure(self, context: UploadContext, changes: dict[str, Any]) -> dict[str, Any]:
        state = self.state(context)
        frame, registry = self.source(context), self.registry(context)
        available = self.available(context, frame, registry)
        for field in ("active", "default"):
            if field in changes:
                if changes[field] not in available:
                    raise ValueError(
                        "La taxonomía seleccionada debe estar disponible y actualizada."
                    )
                state[field] = changes[field]
        if "policy" in changes:
            if changes["policy"] not in POLICIES:
                raise ValueError("Política de snapshot desconocida.")
            state["policy"] = changes["policy"]
        self.save_state(context, state)
        return {key: state[key] for key in ("active", "default", "policy")}

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
            resolved = self.resolve(context, frame, mode)
            classified = labels(resolved, "Palanca").str.strip().ne("") & labels(
                resolved, "Subpalanca"
            ).str.strip().ne("")
            item = available[mode] or {}
            quality = item.get("quality", [])
            scores = [q["macro_f1"] for q in quality if q.get("macro_f1") is not None]
            cards.append(
                {
                    "mode": mode,
                    "available": True,
                    "levers": int(labels(resolved, "Palanca").replace("", pd.NA).nunique()),
                    "sublevers": int(
                        resolved.loc[classified, ["Palanca", "Subpalanca"]]
                        .drop_duplicates()
                        .shape[0]
                    ),
                    "coverage": float(classified.mean()) if len(frame) else 0,
                    "macro_f1": sum(scores) / len(scores) if scores else None,
                    "quality": quality,
                    "equivalence_groups": sum(
                        len(v)
                        for k, v in registry.to_dict()["dimensions"].items()
                        if k.startswith("nps.")
                    ),
                    "created_at": item.get("created_at"),
                }
            )
        selected = state["active"] if state["active"] in available else "NORMALIZED"
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
            "default": state["default"],
            "policy": state["policy"],
            "restored": bool(state.get("restored")),
        }

    def explore(
        self, context: UploadContext, mode: str, offset: int = 0, limit: int = 100
    ) -> dict[str, Any]:
        frame = self.resolve(context, mode=mode)
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
        for row in rows:
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
                "equivalences", self.registry(context).to_dict() if mode == "NORMALIZED" else {}
            ),
        }

    def compare(
        self, context: UploadContext, left: str, right: str, offset: int = 0, limit: int = 100
    ) -> dict[str, Any]:
        source = self.source(context)
        a, b = self.resolve(context, source, left), self.resolve(context, source, right)
        pairs = pd.DataFrame(
            {
                "from_lever": labels(a, "Palanca"),
                "from_sublever": labels(a, "Subpalanca"),
                "to_lever": labels(b, "Palanca"),
                "to_sublever": labels(b, "Subpalanca"),
            }
        )
        counts = cast(
            pd.DataFrame,
            (
                pairs.assign(volume=1)
                .groupby(list(pairs.columns), dropna=False, observed=True, as_index=False)["volume"]
                .sum()
            ),
        )
        counts["share"] = counts.volume / counts.groupby(
            ["from_lever", "from_sublever"]
        ).volume.transform("sum")
        return {
            "left": left,
            "right": right,
            "rows": counts.iloc[offset : offset + min(limit, 100)].to_dict("records"),
            "total": len(counts),
            "note": "Distribución de las mismas respuestas entre lentes; una dispersión puede sugerir mezcla temática.",
        }

    def snapshot(
        self,
        context: UploadContext,
        helix: Optional[pd.DataFrame] = None,
        analysis_config: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        frame, registry, state = self.source(context), self.registry(context), self.state(context)
        available = self.available(context, frame, registry)
        active = state["default"]
        if active not in available:
            raise ValueError(
                "La taxonomía por defecto del snapshot está desactualizada o no existe."
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
            self.resolver.resolve(frame, "COMPLETED", registry, item)
        state = self.state(context)
        state.update(
            {
                "restored": payload,
                "active": payload["active"],
                "default": payload["active"],
                "policy": payload["policy"],
            }
        )
        self.save_state(context, state)

    def resume_local(self, context: UploadContext) -> None:
        state = self.state(context)
        state.pop("restored", None)
        state["active"] = state["default"] = "NORMALIZED"
        self.save_state(context, state)

    @contextmanager
    def snapshot_lens(self, context: UploadContext) -> Iterator[None]:
        previous = self.lens_override
        self.lens_override = self.state(context)["default"]
        try:
            self.resolve(context, mode=self.lens_override)
            yield
        finally:
            self.lens_override = previous
