"""Helix classifications per taxonomy, independent of causal presentation methods."""

from __future__ import annotations

import uuid
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from nps_lens.analytics.nps_helix_link import build_incident_display_text, build_nps_topic
from nps_lens.analytics.taxonomy import MODES
from nps_lens.domain.models import UploadContext
from nps_lens.domain.record_identity import analytical_response_ids
from nps_lens.ingest.helix_dates import incident_occurrence_dates
from nps_lens.services.classification_protocol import (
    HELIX_SCHEMA,
    CompactClassification,
    category_catalog,
    digest,
    encode,
    incident_classification_fingerprint,
)
from nps_lens.services.semantic_validation import (
    GroundedDecision,
    Reason,
    validate_decision,
    validate_quote,
)
from nps_lens.services.taxonomy_exchange import (
    TaxonomyExchange,
    bounded_batches,
    read_response,
    strict_json,
    validate_manifest,
    validate_payload,
    write_numbered_zips,
)
from nps_lens.services.taxonomy_prompts import FALLBACK_LEVER, INSTRUCTIONS_VERSION
from nps_lens.services.taxonomy_service import TaxonomyService, context_key


class EvidenceLink(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    nps_id: str
    confidence: float = Field(ge=0, le=1)
    incident_quote: str = Field(min_length=1, max_length=2000)
    comment_quote: str = Field(min_length=1, max_length=2000)
    reason: Reason = Field(max_length=1000)


class IncidentClassification(CompactClassification):
    # Helix responses produced by the external LLM can omit the advisory
    # classification-level evidence block. Keep the field strict when present,
    # but allow omission for linkless rows so a non-causal classification can
    # still be imported. Rows with NPS links continue to require evidence.
    evidence: GroundedDecision | None = None
    rationale: str = Field(min_length=1, max_length=2000)
    links: list[EvidenceLink] = Field(max_length=20)


class IncidentResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    classifications: list[IncidentClassification]


class HelixExchange:
    def __init__(self, taxonomy: TaxonomyService, downloads: Path):
        self.taxonomy = taxonomy
        self.repository = taxonomy.repository
        self.downloads = downloads
        with self.repository._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS helix_exchange (id TEXT PRIMARY KEY, context TEXT NOT NULL, payload TEXT NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS helix_classifications (context TEXT NOT NULL, scope TEXT NOT NULL, incident TEXT NOT NULL, fingerprint TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(context, scope, incident))"
            )

    def inputs(self, context: UploadContext, incidents: pd.DataFrame, mode: str) -> dict[str, Any]:
        if self.taxonomy.state(context).get("restored"):
            raise ValueError("Vuelve al dataset local para usar el intercambio Helix.")
        if mode not in MODES:
            raise ValueError("Selecciona una lente disponible.")
        source = self.taxonomy.source(context).sort_values("_business_key")
        available = self.taxonomy.available(context, source, self.taxonomy.registry(context))
        frame = (
            self.taxonomy.resolve(context, source, mode)
            if mode in available
            else source.assign(Palanca="", Subpalanca="")
        )
        exchange = TaxonomyExchange(self.taxonomy, self.downloads)
        classified = exchange.assignments(context, source, mode)
        if classified:
            frame = frame.copy()
            for column, field in (("Palanca", "lever"), ("Subpalanca", "sublever")):
                mapped = frame["_business_key"].map(
                    {key: row["primary_classification"][field] for key, row in classified.items()}
                )
                frame[column] = mapped.fillna(frame[column])
        resolved = {mode: frame}
        catalogs = {mode: self.taxonomy.catalog(context, mode) for mode in resolved}
        if any(not catalog["taxonomy"] for catalog in catalogs.values()):
            raise ValueError("La lente activa no contiene Palancas/Subpalancas.")
        ids = incidents.get("Incident Number", pd.Series(dtype=str)).astype(str)
        if len(ids) != len(incidents) or not ids.is_unique or ids.str.strip().eq("").any():
            raise ValueError("Las incidencias requieren IDs únicos y no vacíos.")
        rows = [
            {"id": key, "description": text, "source_service_n2": str(n2)}
            for key, text, n2 in zip(
                ids,
                build_incident_display_text(incidents),
                incidents.get("BBVA_SourceServiceN2", pd.Series("", index=incidents.index)).fillna(
                    ""
                ),
            )
        ]
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
                zip(analytical_response_ids(source), source["Comment"].fillna(""))
            )
        ]
        scopes = {
            mode: digest(
                [
                    HELIX_SCHEMA,
                    INSTRUCTIONS_VERSION,
                    mode,
                    catalogs[mode],
                    comments,
                    (
                        self.taxonomy.state(context).get("artifacts", {}).get("COMPLETED", "")
                        if mode == "COMPLETED"
                        else ""
                    ),
                ]
            )
        }
        return {
            "frame": frame,
            "modes": list(resolved),
            "taxonomies": catalogs,
            "scopes": scopes,
            "incidents": rows,
            "comments": comments,
        }

    def current(self, context: UploadContext, inputs: dict[str, Any]) -> dict[str, dict[str, Any]]:
        fingerprints = {
            row["id"]: incident_classification_fingerprint(row) for row in inputs["incidents"]
        }
        comments = {row["id"]: row for row in inputs["comments"]}
        evidence: dict[str, str] = {}
        current: dict[str, dict[str, Any]] = {}
        with self.repository._connect() as db:
            for mode, scope in inputs["scopes"].items():
                rows = db.execute(
                    "SELECT incident, fingerprint, payload FROM helix_classifications WHERE context=? AND scope=?",
                    (context_key(context), scope),
                ).fetchall()
                current[mode] = {}
                for key, fingerprint, payload in rows:
                    if fingerprints.get(key) != fingerprint:
                        continue
                    item = strict_json(payload.encode())
                    if item.get("instructions_version") != INSTRUCTIONS_VERSION:
                        continue
                    for comment, expected in item.get("evidence_hashes", {}).items():
                        if comment not in evidence:
                            evidence[comment] = (
                                digest(comments[comment]) if comment in comments else ""
                            )
                        if evidence[comment] != expected:
                            break
                    else:
                        current[mode][key] = item
        return current

    def status(self, context: UploadContext, inputs: dict[str, Any]) -> dict[str, Any]:
        current = self.current(context, inputs)
        total = len(inputs["incidents"])
        counts = {
            mode: {"received": len(rows), "pending": total - len(rows)}
            for mode, rows in current.items()
        }
        rows = current[inputs["modes"][0]]
        categories = Counter(
            (row["lever"], row["sublever"]) for row in rows.values() if row["lever"]
        )
        classified = sum(
            count for (lever, _), count in categories.items() if lever != FALLBACK_LEVER
        )
        usage = Counter(link["nps_id"] for row in rows.values() for link in row["links"])
        return {
            "total": total,
            "received": len(rows),
            "classified": classified,
            "multiple": sum(bool(row.get("secondary_classifications")) for row in rows.values()),
            "unassigned": len(rows) - classified,
            "coverage": classified / total if total else 0,
            "links": sum(usage.values()),
            "linked_comments": len(usage),
            "max_comment_reuse": max(usage.values(), default=0),
            "categories": [
                {"lever": lever, "sublever": sub, "count": count}
                for (lever, sub), count in categories.most_common()
            ],
            "taxonomies": counts,
            "pending": sum(item["pending"] for item in counts.values()),
            "ready": bool(total and all(item["pending"] == 0 for item in counts.values())),
        }

    def export(self, context: UploadContext, inputs: dict[str, Any]) -> dict[str, Any]:
        known = self.current(context, inputs)
        pending = []
        for row in inputs["incidents"]:
            modes = [mode for mode in inputs["modes"] if row["id"] not in known[mode]]
            if modes:
                pending.append({**row, "pending_taxonomies": modes})
        if not pending:
            return {"saved_paths": [], "saved_directory": None, "batches": 0, "pending": 0}
        job_id = uuid.uuid4().hex
        batches = bounded_batches(pending, "incidents")
        catalogs = {
            mode: category_catalog(catalog) for mode, catalog in inputs["taxonomies"].items()
        }
        category_ids = {
            mode: {(pair["lever"], pair["sublever"]): key for key, pair in catalog.items()}
            for mode, catalog in catalogs.items()
        }
        mode = inputs["modes"][0]
        evidence = bounded_batches(
            (
                {
                    "id": row["id"],
                    "Comment": row["Comment"],
                    "primary": category_ids[mode].get(
                        (row["taxonomies"][mode]["lever"], row["taxonomies"][mode]["sublever"])
                    ),
                    "secondary": [
                        category_ids[mode][(pair["lever"], pair["sublever"])]
                        for pair in row["secondary_classifications"]
                    ],
                }
                for row in inputs["comments"]
            ),
            "comments",
        )
        manifest = {
            "schema_version": HELIX_SCHEMA,
            "instructions_version": INSTRUCTIONS_VERSION,
            "taxonomies_sha256": digest(catalogs),
            "job_id": job_id,
            "stage": "helix",
            "taxonomy_scopes": inputs["scopes"],
            "batches": [
                {"id": key, "count": len(rows), "sha256": digest({"incidents": rows})}
                for key, rows in batches.items()
            ],
        }
        shared: dict[str, Any] = {"taxonomies.json": catalogs}
        shared.update(
            {f"comments/{key}.json": {"comments": rows} for key, rows in evidence.items()}
        )
        result = write_numbered_zips(
            self.downloads, "incidencias_helix", manifest, batches, shared, "incidents"
        )
        job = {"manifest": manifest, "batches": batches}
        with self.repository._connect() as db:
            db.execute(
                "INSERT INTO helix_exchange VALUES (?, ?, ?)",
                (job_id, context_key(context), encode(job).decode()),
            )
            db.execute(
                "DELETE FROM helix_exchange WHERE context=? AND id NOT IN (SELECT id FROM helix_exchange WHERE context=? ORDER BY rowid DESC LIMIT 3)",
                (context_key(context), context_key(context)),
            )
        return {**result, "pending": len(pending)}

    def import_response(
        self, context: UploadContext, inputs: dict[str, Any], content: bytes
    ) -> dict[str, Any]:
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
        if manifest["taxonomy_scopes"] != inputs["scopes"]:
            raise ValueError(
                "La selección, las taxonomías o el corpus han cambiado; exporta de nuevo."
            )
        source = {row["id"]: row for row in inputs["incidents"]}
        comments = {row["id"]: row for row in inputs["comments"]}
        catalogs = {
            mode: category_catalog(catalog) for mode, catalog in inputs["taxonomies"].items()
        }
        validated: dict[tuple[str, str], dict[str, Any]] = {}
        if not files:
            raise ValueError("No hay lotes de respuesta.")
        for name, payload in files.items():
            key = name.removeprefix("results/").removesuffix(".json")
            if name != f"results/{key}.json" or key not in allowed_batches:
                raise ValueError("Lote de respuesta desconocido.")
            response = validate_payload(IncidentResponse, payload, name)
            expected = job["batches"][key]
            if [row.id for row in response.classifications] != [row["id"] for row in expected]:
                raise ValueError("IDs u orden de incidencias incorrectos.")
            for row, original in zip(response.classifications, expected):
                if source.get(row.id) != {
                    k: v for k, v in original.items() if k != "pending_taxonomies"
                }:
                    raise ValueError("Las incidencias han cambiado; exporta de nuevo.")
                mode = inputs["modes"][0]
                expanded = row.expand(catalogs[mode])
                if row.evidence is None:
                    if row.links:
                        raise ValueError(
                            "Una clasificación Helix con vínculos NPS requiere evidence literal."
                        )
                else:
                    validate_decision(row, catalogs[mode], original["description"])
                primary = expanded["primary_classification"]
                assigned = [
                    (pair["lever"], pair["sublever"])
                    for pair in [primary, *expanded["secondary_classifications"]]
                ]
                if len({link.nps_id for link in row.links}) != len(row.links):
                    raise ValueError("Vínculos NPS duplicados.")
                for link in row.links:
                    comment = comments.get(link.nps_id)
                    comment_pairs = (
                        {
                            (
                                comment["taxonomies"][mode]["lever"],
                                comment["taxonomies"][mode]["sublever"],
                            ),
                            *(
                                (pair["lever"], pair["sublever"])
                                for pair in comment["secondary_classifications"]
                            ),
                        }
                        if comment
                        else set()
                    )
                    if (
                        not comment
                        or not comment_pairs.intersection(assigned)
                        or row.primary is None
                    ):
                        raise ValueError("Vínculo a comentario desconocido o a otra categoría.")
                    validate_quote(link.incident_quote, original["description"])
                    validate_quote(link.comment_quote, comment["Comment"])
                    if primary["lever"] == "Sin clasificación temática":
                        raise ValueError("Las categorías de reserva no justifican vínculos NPS.")
                validated[(mode, row.id)] = {
                    "instructions_version": INSTRUCTIONS_VERSION,
                    "evidence": row.evidence.model_dump() if row.evidence is not None else None,
                    **primary,
                    "secondary_classifications": expanded["secondary_classifications"],
                    "rationale": row.rationale,
                    "links": [link.model_dump() for link in row.links],
                    "evidence_hashes": {
                        link.nps_id: digest(comments[link.nps_id]) for link in row.links
                    },
                }
        existing = self.current(context, inputs)
        decisions: dict[tuple[str, str], tuple[Any, ...]] = {}
        for mode in inputs["modes"]:
            candidates = {
                **existing[mode],
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
        if any(
            key in existing[mode] and existing[mode][key] != value
            for (mode, key), value in validated.items()
        ):
            raise ValueError("Una incidencia ya tiene una respuesta diferente para esa taxonomía.")
        with self.repository._connect() as db:
            db.executemany(
                "INSERT OR REPLACE INTO helix_classifications VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        context_key(context),
                        inputs["scopes"][mode],
                        key,
                        incident_classification_fingerprint(source[key]),
                        encode(value).decode(),
                    )
                    for (mode, key), value in validated.items()
                ],
            )
            db.execute(
                "DELETE FROM helix_classifications WHERE context=? AND scope NOT IN (SELECT scope FROM helix_classifications WHERE context=? GROUP BY scope ORDER BY MAX(rowid) DESC LIMIT 9)",
                (context_key(context), context_key(context)),
            )
        return self.status(context, inputs)

    def links(
        self,
        context: UploadContext,
        inputs: dict[str, Any],
        focus: pd.DataFrame,
        incidents: pd.DataFrame,
        *,
        max_days_apart: int = 90,
    ) -> pd.DataFrame:
        mode = inputs["modes"][0]
        current = self.current(context, inputs)[mode]
        incident_ids = set(incidents["Incident Number"].astype(str))
        if not incident_ids or not incident_ids.issubset(current):
            raise ValueError(
                "Importa la respuesta Helix completa para la lente activa antes de usar causalidad LLM."
            )
        nps_topics = dict(zip(analytical_response_ids(focus), build_nps_topic(focus)))
        nps_dates = dict(
            zip(
                analytical_response_ids(focus),
                pd.to_datetime(
                    focus.get("Fecha", pd.Series(index=focus.index, dtype="datetime64[ns]")),
                    errors="coerce",
                ),
            )
        )
        incident_dates = dict(
            zip(incidents["Incident Number"].astype(str), incident_occurrence_dates(incidents)[0])
        )
        rows = [
            {
                "nps_id": link["nps_id"],
                "incident_id": key,
                "similarity": link["confidence"],
                "nps_topic": nps_topics[link["nps_id"]],
                "incident_topic": " · ".join(
                    pair["lever"] + " > " + pair["sublever"]
                    for pair in [row, *row.get("secondary_classifications", [])]
                ),
                "llm_rationale": row["rationale"],
            }
            for key, row in current.items()
            if key in incident_ids
            for link in row["links"]
            if link["nps_id"] in nps_topics
            and pd.notna(nps_dates.get(link["nps_id"]))
            and pd.notna(incident_dates.get(key))
            and abs((nps_dates[link["nps_id"]].normalize() - incident_dates[key].normalize()).days)
            <= max_days_apart
        ]
        return pd.DataFrame(
            rows,
            columns=[
                "nps_id",
                "incident_id",
                "similarity",
                "nps_topic",
                "incident_topic",
                "llm_rationale",
            ],
        )
