"""Helix classifications per taxonomy, independent of causal presentation methods."""

from __future__ import annotations

import logging
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from nps_lens.analytics.linking_policy import (
    LINK_MAX_DAYS_APART,
    LINK_TOP_K_PER_INCIDENT,
    evaluation_diagnostic,
    temporal_mask,
)
from nps_lens.analytics.nps_helix_link import (
    build_incident_text,
    build_nps_topic,
    retrieve_incident_candidates,
)
from nps_lens.analytics.taxonomy import MODES
from nps_lens.domain.models import UploadContext
from nps_lens.domain.record_identity import analytical_response_ids
from nps_lens.ingest.helix_dates import incident_occurrence_dates
from nps_lens.services.analysis_horizon import analysis_horizon, eligible_helix
from nps_lens.services.classification_protocol import (
    HELIX_SCHEMA,
    CompactClassification,
    category_catalog,
    digest,
    encode,
    incident_classification_fingerprint,
    taxonomy_fingerprint,
)
from nps_lens.services.semantic_validation import validate_quote
from nps_lens.services.taxonomy_exchange import (
    TaxonomyExchange,
    bounded_batches,
    read_response,
    strict_json,
    validate_manifest,
    validate_payload,
    write_classification_zips,
)
from nps_lens.services.taxonomy_prompts import (
    FALLBACK_LEVER,
    HELIX_INSTRUCTIONS_VERSION,
)
from nps_lens.services.taxonomy_service import TaxonomyService, context_key

logger = logging.getLogger(__name__)


class EvidenceLink(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    nps_id: str
    confidence: float = Field(ge=0, le=1)
    incident_quote: str = Field(min_length=1, max_length=2000)
    comment_quote: str = Field(min_length=1, max_length=2000)
    same_task: bool = False
    same_symptom: bool = False
    affected_task: str = Field(default="", max_length=160)
    observed_symptom: str = Field(default="", max_length=160)


class IncidentClassification(CompactClassification):
    links: list[EvidenceLink] = Field(max_length=LINK_TOP_K_PER_INCIDENT)


class IncidentResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    classifications: list[IncidentClassification]


_HELIX_IGNORABLE_ANNOTATIONS = frozenset({"evidence", "reason", "rationale"})


def normalize_helix_payload(payload: Any) -> Any:
    """Drop only known non-semantic LLM annotations at the Helix boundary.

    Some LLMs add explanatory ``evidence``/``reason``/``rationale`` fields despite
    the explicit ZIP contract.  Those fields never participate in classification or
    linking decisions, so accepting them as annotations is safe.  Unknown extras are
    deliberately left untouched and are still rejected by the strict Pydantic models,
    preserving schema-drift detection.
    """
    if not isinstance(payload, dict):
        return payload
    rows = payload.get("classifications")
    if not isinstance(rows, list):
        return payload

    row_fields = frozenset(IncidentClassification.model_fields)
    link_fields = frozenset(EvidenceLink.model_fields)
    normalized_rows: list[Any] = []
    changed = False
    for row in rows:
        if not isinstance(row, dict):
            normalized_rows.append(row)
            continue

        normalized_row = row
        extras = set(row) - row_fields
        if extras and extras <= _HELIX_IGNORABLE_ANNOTATIONS:
            normalized_row = {key: value for key, value in row.items() if key in row_fields}
            changed = True

        links = normalized_row.get("links")
        if isinstance(links, list):
            normalized_links: list[Any] = []
            links_changed = False
            for link in links:
                if not isinstance(link, dict):
                    normalized_links.append(link)
                    continue
                link_extras = set(link) - link_fields
                if link_extras and link_extras <= _HELIX_IGNORABLE_ANNOTATIONS:
                    normalized_links.append(
                        {key: value for key, value in link.items() if key in link_fields}
                    )
                    links_changed = True
                else:
                    normalized_links.append(link)
            if links_changed:
                normalized_row = {**normalized_row, "links": normalized_links}
                changed = True

        normalized_rows.append(normalized_row)

    if not changed:
        return payload
    return {**payload, "classifications": normalized_rows}


class HelixExchange:
    def __init__(self, taxonomy: TaxonomyService, downloads: Path):
        self.taxonomy = taxonomy
        self.repository = taxonomy.repository
        self.downloads = downloads
        with self.repository._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "CREATE TABLE IF NOT EXISTS helix_exchange (id TEXT PRIMARY KEY, context TEXT NOT NULL, payload TEXT NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS helix_incident_categories (context TEXT NOT NULL, scope TEXT NOT NULL, incident TEXT NOT NULL, fingerprint TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(context, scope, incident))"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS helix_incident_links (context TEXT NOT NULL, scope TEXT NOT NULL, incident TEXT NOT NULL, fingerprint TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(context, scope, incident))"
            )
            if db.execute(
                "SELECT 1 FROM sqlite_master WHERE name='helix_classifications'"
            ).fetchone():
                # One transactional migration. Recover the original narrative from
                # retained requests; never guess a semantic identity from a quote.
                legacy = db.execute(
                    "SELECT context, scope, incident, fingerprint, payload "
                    "FROM helix_classifications ORDER BY rowid"
                ).fetchall()
                legacy_scopes = {(row[0], row[1]) for row in legacy}
                narratives = {}
                modes = {}
                migrated_exchange_ids: set[str] = set()
                for exchange_id, ctx, payload in db.execute(
                    "SELECT id, context, payload FROM helix_exchange"
                ):
                    try:
                        job = strict_json(payload.encode())
                        taxonomy_scopes = job["manifest"]["taxonomy_scopes"]
                        batches = job["batches"]
                    except (AttributeError, KeyError, TypeError, ValueError):
                        continue
                    if not isinstance(taxonomy_scopes, dict) or not isinstance(batches, dict):
                        continue
                    for mode, scope in taxonomy_scopes.items():
                        if isinstance(mode, str) and isinstance(scope, str):
                            modes[(ctx, scope)] = mode
                            if (ctx, scope) in legacy_scopes:
                                migrated_exchange_ids.add(exchange_id)
                    for batch in batches.values():
                        if not isinstance(batch, list):
                            continue
                        for row in batch:
                            if (
                                not isinstance(row, dict)
                                or not isinstance(row.get("id"), str)
                                or not isinstance(row.get("description"), str)
                            ):
                                continue
                            narratives[
                                (
                                    ctx,
                                    row["id"],
                                    digest([row.get("description", ""), row.get("date", "")]),
                                )
                            ] = row
                migrated = 0
                for ctx, scope, key, fingerprint, payload in legacy:
                    try:
                        item = strict_json(payload.encode())
                    except (AttributeError, TypeError, ValueError):
                        continue
                    original = narratives.get((ctx, key, fingerprint))
                    mode = modes.get((ctx, scope))
                    if (
                        original is None
                        or mode is None
                        or not isinstance(item, dict)
                        or not isinstance(item.get("taxonomy_fingerprint"), str)
                        or not isinstance(item.get("lever"), str)
                        or not isinstance(item.get("sublever"), str)
                        or not isinstance(item.get("secondary_classifications"), list)
                    ):
                        continue
                    category = {
                        k: v
                        for k, v in item.items()
                        if k not in {"links", "candidates", "evidence_hashes", "linking_version"}
                    }
                    category["instructions_version"] = HELIX_INSTRUCTIONS_VERSION
                    category_scope = self.category_scope(mode, item["taxonomy_fingerprint"])
                    db.execute(
                        "INSERT OR REPLACE INTO helix_incident_categories VALUES (?, ?, ?, ?, ?)",
                        (
                            ctx,
                            category_scope,
                            key,
                            incident_classification_fingerprint(original),
                            encode(category).decode(),
                        ),
                    )
                    migrated += 1
                # Old links used ID-based candidate padding and must be reevaluated.
                db.execute("DROP TABLE helix_classifications")
                if migrated_exchange_ids:
                    db.executemany(
                        "DELETE FROM helix_exchange WHERE id=?",
                        [(exchange_id,) for exchange_id in migrated_exchange_ids],
                    )
                logger.info(
                    "Migración Helix completada: migrated=%d discarded=%d",
                    migrated,
                    len(legacy) - migrated,
                )

    @staticmethod
    def category_scope(mode: str, fingerprint: str) -> str:
        return digest([mode, fingerprint, HELIX_INSTRUCTIONS_VERSION])

    def inputs(
        self,
        context: UploadContext,
        incidents: pd.DataFrame,
        mode: str,
        *,
        channel_assignments: list[str] | None = None,
        **scope: Any,
    ) -> dict[str, Any]:
        if self.taxonomy.state(context).get("restored"):
            raise ValueError("Vuelve al dataset local para usar el intercambio Helix.")
        if mode not in MODES:
            raise ValueError("Selecciona una lente disponible.")
        source = self.taxonomy.source(context).sort_values("_business_key")
        horizon = analysis_horizon(source, **scope)
        if "Fecha" in source:
            dates = pd.to_datetime(source["Fecha"], errors="coerce").dt.date
            source = source.loc[
                (
                    dates.between(horizon.link_comment_start, horizon.link_comment_end)
                    if horizon.link_comment_start and horizon.link_comment_end
                    else dates.notna() & False
                )
            ]
        if channel_assignments:
            source = source.loc[
                self.taxonomy.registry(context)
                .normalize_series(
                    "nps.Canal", source["source_channel" if "source_channel" in source else "Canal"]
                )
                .str.strip()
                .str.casefold()
                .eq(str(scope.get("score_channel") or "").strip().casefold())
            ]
        all_incidents = incidents
        _, incidents = eligible_helix(incidents, horizon, channel_assignments or [])
        available = self.taxonomy.available(context, source, self.taxonomy.registry(context))
        frame = (
            self.taxonomy.resolve(context, source, mode)
            if mode in available
            else source.assign(Palanca="", Subpalanca="")
        )
        frame.attrs["classification_signature"] = frame.attrs.get(
            "classification_signature", digest([mode, "classification_pending"])
        )
        frame.attrs["classification_pending"] = mode not in available or bool(
            frame["Palanca"].fillna("").eq("").any() | frame["Subpalanca"].fillna("").eq("").any()
        )
        exchange = TaxonomyExchange(self.taxonomy, self.downloads)
        classified = exchange.assignments(context, source, mode)
        resolved = {mode: frame}
        catalogs = {mode: self.taxonomy.catalog(context, mode) for mode in resolved}
        if any(not catalog["taxonomy"] for catalog in catalogs.values()):
            raise ValueError("La lente activa no contiene Palancas/Subpalancas.")
        ids = incidents.get("Incident Number", pd.Series(dtype=str)).astype(str)
        if len(ids) != len(incidents) or not ids.is_unique or ids.str.strip().eq("").any():
            raise ValueError("Las incidencias requieren IDs únicos y no vacíos.")
        rows = [
            {"id": key, "description": text}
            for key, text in zip(ids, incidents["_incident_semantic_text"], strict=False)
        ]
        for row, date in zip(rows, incident_occurrence_dates(incidents)[0], strict=False):
            row["date"] = date.isoformat() if pd.notna(date) else ""
        labels = {
            mode: frame[["Palanca", "Subpalanca"]].to_numpy().tolist()
            for mode, frame in resolved.items()
        }
        comments = [
            {
                "id": key,
                "Comment": str(comment),
                "secondary_classifications": classified.get(key, {}).get(
                    "secondary_classifications", []
                ),
                "taxonomies": {
                    mode: {
                        "lever": str(pairs[i][0]),
                        "sublever": str(pairs[i][1]),
                    }
                    for mode, pairs in labels.items()
                },
            }
            for i, (key, comment) in enumerate(
                zip(analytical_response_ids(source), source["Comment"].fillna(""), strict=False)
            )
        ]
        for row, date in zip(
            comments,
            pd.to_datetime(
                source.get("Fecha", pd.Series(pd.NaT, index=source.index)), errors="coerce"
            ),
            strict=False,
        ):
            row["date"] = date.isoformat() if pd.notna(date) else ""
        classification_scopes = {
            mode: self.category_scope(mode, taxonomy_fingerprint(catalogs[mode]))
        }
        scopes = {
            mode: digest(
                [
                    classification_scopes[mode],
                    frame.attrs["classification_signature"],
                    "llm",
                    "contextual-retrieval/1",
                    horizon.max_days_apart,
                    LINK_TOP_K_PER_INCIDENT,
                    HELIX_SCHEMA,
                    HELIX_INSTRUCTIONS_VERSION,
                    comments,
                ]
            )
        }
        return {
            "frame": frame,
            "all_incident_frame": all_incidents,
            "scope": scope,
            "analysis_horizon": horizon.payload(),
            "incident_frame": incidents,
            "modes": list(resolved),
            "taxonomies": catalogs,
            "scopes": scopes,
            "classification_scopes": classification_scopes,
            "incidents": rows,
            "comments": comments,
        }

    def classifications(
        self, context: UploadContext, inputs: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        fingerprints = {
            row["id"]: incident_classification_fingerprint(row) for row in inputs["incidents"]
        }
        result: dict[str, dict[str, Any]] = {}
        with self.repository._connect() as db:
            for mode, scope in inputs["classification_scopes"].items():
                result[mode] = {}
                for key, fingerprint, payload in db.execute(
                    "SELECT incident, fingerprint, payload FROM helix_incident_categories WHERE context=? AND scope=?",
                    (context_key(context), scope),
                ):
                    if fingerprints.get(key) != fingerprint:
                        continue
                    item = strict_json(payload.encode())
                    if item.get("instructions_version") == HELIX_INSTRUCTIONS_VERSION:
                        result[mode][key] = item
        return result

    def current(self, context: UploadContext, inputs: dict[str, Any]) -> dict[str, dict[str, Any]]:
        categories = self.classifications(context, inputs)
        incidents = {row["id"]: row for row in inputs["incidents"]}
        current: dict[str, dict[str, Any]] = {}
        with self.repository._connect() as db:
            for mode, scope in inputs["scopes"].items():
                current[mode] = {}
                for key, fingerprint, payload in db.execute(
                    "SELECT incident, fingerprint, payload FROM helix_incident_links WHERE context=? AND scope=?",
                    (context_key(context), scope),
                ):
                    linking = strict_json(payload.encode())
                    if linking.get("linking_version") != HELIX_INSTRUCTIONS_VERSION:
                        continue
                    category = categories[mode].get(key)
                    if category is not None and fingerprint == digest([category, incidents[key]]):
                        current[mode][key] = {**category, **linking}
        return current

    def status(self, context: UploadContext, inputs: dict[str, Any]) -> dict[str, Any]:
        classifications = self.classifications(context, inputs)
        current = self.current(context, inputs)
        total = len(inputs["incidents"])
        counts = {
            mode: {"received": len(rows), "pending": total - len(rows)}
            for mode, rows in classifications.items()
        }
        mode = inputs["modes"][0]
        rows = classifications[mode]
        categories = Counter(
            (row["lever"], row["sublever"]) for row in rows.values() if row["lever"]
        )
        classified = sum(
            count for (lever, _), count in categories.items() if lever != FALLBACK_LEVER
        )
        return {
            "analysis_horizon": inputs["analysis_horizon"],
            "mode": mode,
            "taxonomy_fingerprint": taxonomy_fingerprint(inputs["taxonomies"][mode]),
            "total": total,
            "received": len(rows),
            "classified": classified,
            "multiple": sum(bool(row.get("secondary_classifications")) for row in rows.values()),
            "unassigned": len(rows) - classified,
            "coverage": classified / total if total else 0,
            "categories": [
                {"lever": lever, "sublever": sub, "count": count}
                for (lever, sub), count in categories.most_common()
            ],
            "taxonomies": counts,
            "pending": sum(item["pending"] for item in counts.values()),
            "link_pending": sum(
                len(set(classifications[mode]) - set(current[mode])) for mode in inputs["modes"]
            ),
            "ready": bool(total and all(len(current[mode]) == total for mode in inputs["modes"])),
        }

    def export(
        self,
        context: UploadContext,
        inputs: dict[str, Any],
        *,
        reevaluate: bool = False,
        single_zip: bool = False,
    ) -> dict[str, Any]:
        for mode in inputs["modes"]:
            self.taxonomy.guard_export(context, mode)
        known = self.current(context, inputs)
        categories = self.classifications(context, inputs)
        pending = []
        for row in inputs["incidents"]:
            modes = [
                mode
                for mode in inputs["modes"]
                if (row["id"] in categories[mode] if reevaluate else row["id"] not in known[mode])
            ]
            if modes:
                pending.append({**row, "pending_taxonomies": modes})
        if not pending:
            return {"saved_paths": [], "saved_directory": None, "batches": 0, "pending": 0}
        job_id = uuid.uuid4().hex
        catalogs = {
            mode: category_catalog(catalog) for mode, catalog in inputs["taxonomies"].items()
        }
        category_ids = {
            mode: {(pair["lever"], pair["sublever"]): key for key, pair in catalog.items()}
            for mode, catalog in catalogs.items()
        }
        mode = inputs["modes"][0]
        comments = {row["id"]: row for row in inputs["comments"]}
        candidates: dict[str, list[dict[str, Any]]] = {}
        pending_ids = {row["id"] for row in pending}
        pending_frame = inputs["incident_frame"].loc[
            inputs["incident_frame"]["Incident Number"].astype(str).isin(pending_ids)
        ]
        if categories[mode]:
            pending_frame = pending_frame.assign(
                Palanca=pending_frame["Incident Number"].map(
                    lambda key: categories[mode].get(str(key), {}).get("lever", "")
                ),
                Subpalanca=pending_frame["Incident Number"].map(
                    lambda key: categories[mode].get(str(key), {}).get("sublever", "")
                ),
            )
        retrieved = retrieve_incident_candidates(
            inputs["frame"],
            pending_frame,
            max_days_apart=inputs["analysis_horizon"]["max_days_apart"],
        )
        for link in retrieved.itertuples(index=False):
            row = comments[link.nps_id]
            primary = category_ids[mode].get(
                (row["taxonomies"][mode]["lever"], row["taxonomies"][mode]["sublever"])
            )
            if primary is None or catalogs[mode][primary]["lever"] == FALLBACK_LEVER:
                continue
            candidates.setdefault(str(link.incident_id), []).append(
                {
                    "id": row["id"],
                    "Comment": row["Comment"],
                    "primary": primary,
                    "text_similarity": float(str(link.text_similarity)),
                    "secondary": [
                        category_ids[mode][(pair["lever"], pair["sublever"])]
                        for pair in row["secondary_classifications"]
                    ],
                }
            )
        for row in pending:
            row["candidates"] = candidates.get(row["id"], [])
            category = categories[mode].get(row["id"])
            if category is not None:
                row["classification"] = {
                    "primary": category_ids[mode].get((category["lever"], category["sublever"])),
                    "secondary": [
                        category_ids[mode][(pair["lever"], pair["sublever"])]
                        for pair in category["secondary_classifications"]
                    ],
                }
        diagnostic = {
            "classification_signature": inputs["frame"].attrs["classification_signature"],
            **evaluation_diagnostic(
                eligible=int(retrieved.attrs.get("eligible_incidents", len(pending))),
                candidate_count=sum(len(row["candidates"]) for row in pending),
                with_candidates=sum(bool(row["candidates"]) for row in pending),
                reason=(
                    "classification_pending"
                    if inputs["frame"].attrs.get("classification_pending")
                    else "evaluation_pending" if candidates else "no_candidates"
                ),
            ),
        }
        batches = bounded_batches(pending, "incidents")
        manifest = {
            "scope": inputs["scope"],
            "analysis_horizon": inputs["analysis_horizon"],
            "schema_version": HELIX_SCHEMA,
            "instructions_version": HELIX_INSTRUCTIONS_VERSION,
            "reevaluate": reevaluate,
            "taxonomies_sha256": digest(catalogs),
            "taxonomy_fingerprint": taxonomy_fingerprint(inputs["taxonomies"][mode]),
            "taxonomy_mode": mode,
            "job_id": job_id,
            "stage": "helix",
            "taxonomy_scopes": inputs["scopes"],
            "classification_scopes": inputs["classification_scopes"],
            "linking_diagnostics": diagnostic,
            "batches": [
                {"id": key, "count": len(rows), "sha256": digest({"incidents": rows})}
                for key, rows in batches.items()
            ],
        }
        shared: dict[str, Any] = {"taxonomies.json": catalogs}
        result = write_classification_zips(
            self.downloads,
            "incidencias_helix",
            manifest,
            batches,
            shared,
            "incidents",
            single_zip=single_zip,
        )
        candidate_ids = {candidate["id"] for row in pending for candidate in row["candidates"]}
        job = {
            "stage": "helix",
            "manifest": manifest,
            "batches": batches,
            "taxonomies": inputs["taxonomies"],
            "comments": {key: comments[key] for key in candidate_ids},
        }
        with self.repository._connect() as db:
            db.execute(
                "INSERT INTO helix_exchange VALUES (?, ?, ?)",
                (job_id, context_key(context), encode(job).decode()),
            )
            db.execute(
                "DELETE FROM helix_exchange WHERE context=? AND json_extract(payload, '$.stage')='complete' "
                "AND id NOT IN (SELECT id FROM helix_exchange WHERE context=? "
                "AND json_extract(payload, '$.stage')='complete' ORDER BY rowid DESC LIMIT 3)",
                (context_key(context), context_key(context)),
            )
        return {**result, "pending": len(pending), "diagnostics": diagnostic}

    def import_response(
        self, context: UploadContext, inputs: dict[str, Any], content: bytes
    ) -> dict[str, Any]:
        if self.taxonomy.state(context).get("restored"):
            raise ValueError("Vuelve al dataset local para usar el intercambio Helix.")
        manifest, files = read_response(content, "helix")
        with self.repository._connect() as db:
            found = db.execute(
                "SELECT payload FROM helix_exchange WHERE id=? AND context=?",
                (manifest.get("job_id"), context_key(context)),
            ).fetchone()
        if not found:
            raise ValueError("El intercambio no pertenece a este dataset.")
        job = strict_json(found[0].encode())
        allowed_batches = validate_manifest(manifest, job["manifest"])
        mode = manifest["taxonomy_mode"]
        if mode not in MODES or mode not in job["taxonomies"]:
            raise ValueError("El job Helix no conserva una lente válida.")
        frozen_taxonomy = job["taxonomies"][mode]
        if manifest["taxonomy_fingerprint"] != taxonomy_fingerprint(frozen_taxonomy):
            raise ValueError("taxonomy_fingerprint incompatible con el job exportado.")
        expected_rows = [row for batch in job["batches"].values() for row in batch]
        source = {
            row["id"]: {
                key: value
                for key, value in row.items()
                if key not in {"pending_taxonomies", "candidates", "classification"}
            }
            for row in expected_rows
        }
        # Validate original IDs even when the user has since changed the horizon.
        original_frame = inputs["all_incident_frame"]
        original_frame = original_frame.loc[
            original_frame["Incident Number"].astype(str).isin(source)
        ]
        current_source = {
            str(key): {
                "id": str(key),
                "description": text,
                "date": stamp.isoformat() if pd.notna(stamp) else "",
            }
            for key, text, stamp in zip(
                original_frame["Incident Number"],
                build_incident_text(original_frame),
                incident_occurrence_dates(original_frame)[0],
                strict=False,
            )
        }
        if current_source != source:
            raise ValueError("Las incidencias han cambiado; exporta de nuevo.")
        comments = job["comments"]
        catalogs = {mode: category_catalog(frozen_taxonomy)}
        frozen_inputs = {
            "taxonomies": job["taxonomies"],
            "analysis_horizon": manifest["analysis_horizon"],
            "incidents": list(source.values()),
            "modes": [mode],
            "classification_scopes": manifest["classification_scopes"],
            "scopes": manifest["taxonomy_scopes"],
        }
        validated: dict[tuple[str, str], dict[str, Any]] = {}
        if not files:
            raise ValueError("No hay lotes de respuesta.")
        for name, payload in files.items():
            key = name.removeprefix("results/").removesuffix(".json")
            if name != f"results/{key}.json" or key not in allowed_batches:
                raise ValueError("Lote de respuesta desconocido.")
            response = validate_payload(IncidentResponse, normalize_helix_payload(payload), name)
            expected = job["batches"][key]
            if [row.id for row in response.classifications] != [row["id"] for row in expected]:
                raise ValueError("IDs u orden de incidencias incorrectos.")
            for row, original in zip(response.classifications, expected, strict=False):
                if source.get(row.id) != {
                    k: v
                    for k, v in original.items()
                    if k not in {"pending_taxonomies", "candidates", "classification"}
                }:
                    raise ValueError("Las incidencias han cambiado; exporta de nuevo.")
                if "classification" in original and original["classification"] != {
                    "primary": row.primary,
                    "secondary": row.secondary,
                }:
                    raise ValueError(
                        "La clasificación de incidencia ya existe; conserva sus categorías y evalúa solo vínculos."
                    )
                expanded = row.expand(catalogs[mode])
                primary = expanded["primary_classification"]
                supplied = {candidate["id"] for candidate in original["candidates"]}
                if len({link.nps_id for link in row.links}) != len(row.links):
                    raise ValueError("Vínculos NPS duplicados.")
                for link in row.links:
                    if link.same_task is not True or link.same_symptom is not True:
                        raise ValueError("Un vínculo requiere same_task=true y same_symptom=true.")
                    comment = comments.get(link.nps_id)
                    if not comment or link.nps_id not in supplied:
                        raise ValueError(
                            "Vínculo a comentario desconocido o no candidato de esta incidencia."
                        )
                    if row.primary is None:
                        raise ValueError("Una incidencia sin clasificación no admite vínculos.")
                    if not temporal_mask(
                        comment["date"],
                        original["date"],
                        manifest["analysis_horizon"]["max_days_apart"],
                    ):
                        raise ValueError("Vínculo fuera de la ventana temporal por pareja.")
                    validate_quote(link.incident_quote, original["description"])
                    validate_quote(link.comment_quote, comment["Comment"])
                    if primary["lever"] == "Sin clasificación temática":
                        raise ValueError("Las categorías de reserva no justifican vínculos NPS.")
                validated[(mode, row.id)] = {
                    "classified_at": datetime.now(timezone.utc).isoformat(),
                    "instructions_version": HELIX_INSTRUCTIONS_VERSION,
                    "taxonomy_fingerprint": manifest["taxonomy_fingerprint"],
                    **primary,
                    "secondary_classifications": expanded["secondary_classifications"],
                    "candidates": {
                        candidate["id"]: candidate["text_similarity"]
                        for candidate in original["candidates"]
                    },
                    "links": [link.model_dump() for link in row.links],
                }
        existing = self.current(context, frozen_inputs)
        categories = self.classifications(context, frozen_inputs)
        decisions: dict[tuple[str, str], tuple[Any, ...]] = {}
        for mode in frozen_inputs["modes"]:
            candidates = {
                **categories[mode],
                **{key: value for (lens, key), value in validated.items() if lens == mode},
            }
            for key, value in candidates.items():
                incident = source[key]
                identity = (mode, incident["description"])
                decision = (
                    value["lever"],
                    value["sublever"],
                    tuple(
                        sorted(
                            (pair["lever"], pair["sublever"])
                            for pair in value["secondary_classifications"]
                        )
                    ),
                )
                if identity in decisions and decisions[identity] != decision:
                    raise ValueError(
                        "Narrativas idénticas tienen categorías diferentes entre lotes. Revisa el criterio; no se importó nada."
                    )
                decisions[identity] = decision
        with self.repository._connect() as db:
            for (mode, key), value in validated.items():
                category = {k: v for k, v in value.items() if k not in {"links", "candidates"}}
                previous = categories[mode].get(key)
                if previous is not None:
                    category["classified_at"] = previous["classified_at"]
                    if category != previous:
                        raise ValueError(
                            "Una incidencia ya tiene una respuesta diferente para esa taxonomía."
                        )
                links = {k: value[k] for k in ("links", "candidates")}
                links["linking_version"] = HELIX_INSTRUCTIONS_VERSION
                combined = {**category, **links}
                if (
                    key in existing[mode]
                    and combined != existing[mode][key]
                    and not manifest.get("reevaluate")
                ):
                    raise ValueError(
                        "Una incidencia ya tiene una respuesta diferente para esa taxonomía."
                    )
                db.execute(
                    "INSERT OR REPLACE INTO helix_incident_categories VALUES (?, ?, ?, ?, ?)",
                    (
                        context_key(context),
                        manifest["classification_scopes"][mode],
                        key,
                        incident_classification_fingerprint(source[key]),
                        encode(category).decode(),
                    ),
                )
                db.execute(
                    "INSERT OR REPLACE INTO helix_incident_links VALUES (?, ?, ?, ?, ?)",
                    (
                        context_key(context),
                        manifest["taxonomy_scopes"][mode],
                        key,
                        digest([category, source[key]]),
                        encode(links).decode(),
                    ),
                )
            if set(source).issubset(
                set(existing[mode]) | {key for lens, key in validated if lens == mode}
            ):
                job["stage"] = "complete"
                db.execute(
                    "UPDATE helix_exchange SET payload=? WHERE id=?",
                    (encode(job).decode(), manifest["job_id"]),
                )
        return self.status(context, frozen_inputs)

    def links(
        self,
        context: UploadContext,
        inputs: dict[str, Any],
        focus: pd.DataFrame,
        incidents: pd.DataFrame,
        *,
        max_days_apart: int = LINK_MAX_DAYS_APART,
    ) -> pd.DataFrame:
        mode = inputs["modes"][0]
        current = self.current(context, inputs)[mode]
        incident_ids = set(incidents["Incident Number"].astype(str))
        if not incident_ids or not incident_ids.issubset(current):
            raise ValueError(
                "Importa la respuesta Helix completa para la lente activa antes de usar vinculación semántica LLM."
            )
        nps_topics = dict(zip(analytical_response_ids(focus), build_nps_topic(focus), strict=False))
        nps_dates = dict(
            zip(
                analytical_response_ids(focus),
                pd.to_datetime(
                    focus.get("Fecha", pd.Series(index=focus.index, dtype="datetime64[ns]")),
                    errors="coerce",
                ),
                strict=False,
            )
        )
        incident_dates = dict(
            zip(
                incidents["Incident Number"].astype(str),
                incident_occurrence_dates(incidents)[0],
                strict=False,
            )
        )
        rows = [
            {
                "nps_id": link["nps_id"],
                "incident_id": key,
                "text_similarity": row["candidates"][link["nps_id"]],
                "semantic_confidence": link["confidence"],
                "comment_quote": link["comment_quote"],
                "incident_quote": link["incident_quote"],
                "same_task": link.get("same_task", False),
                "same_symptom": link.get("same_symptom", False),
                "affected_task": link.get("affected_task", ""),
                "observed_symptom": link.get("observed_symptom", ""),
                "causal_engine": "llm",
                "causal_window_days": max_days_apart,
                "nps_topic": nps_topics[link["nps_id"]],
                "incident_topic": " · ".join(
                    pair["lever"] + " > " + pair["sublever"]
                    for pair in [row, *row.get("secondary_classifications", [])]
                ),
            }
            for key, row in current.items()
            if key in incident_ids
            for link in row["links"]
            if link["nps_id"] in nps_topics
            and temporal_mask(
                nps_dates.get(link["nps_id"]), incident_dates.get(key), max_days_apart
            )
        ]
        result = pd.DataFrame(
            rows,
            columns=[
                "nps_id",
                "incident_id",
                "text_similarity",
                "nps_topic",
                "incident_topic",
                "semantic_confidence",
                "comment_quote",
                "incident_quote",
                "same_task",
                "same_symptom",
                "affected_task",
                "observed_symptom",
                "causal_engine",
                "causal_window_days",
            ],
        )

        candidates = {
            key: [
                comment
                for comment in current[key]["candidates"]
                if comment in nps_topics
                and temporal_mask(nps_dates.get(comment), incident_dates.get(key), max_days_apart)
            ]
            for key in incident_ids
        }
        result.attrs = cast(
            Any,
            {
                **result.attrs,
                **evaluation_diagnostic(
                    eligible=len(incident_ids),
                    candidate_count=sum(map(len, candidates.values())),
                    with_candidates=sum(bool(values) for values in candidates.values()),
                    matches=len(result),
                    reason=(
                        "classification_pending"
                        if inputs["frame"].attrs.get("classification_pending")
                        else ""
                    ),
                ),
            },
        )
        return result
