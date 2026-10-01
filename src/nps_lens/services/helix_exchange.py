"""Helix classifications per taxonomy, independent of causal presentation methods."""

from __future__ import annotations

import uuid
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from nps_lens.analytics.linking_policy import LINK_MAX_DAYS_APART, LINK_TOP_K_PER_INCIDENT
from nps_lens.analytics.nps_helix_link import (
    build_incident_text,
    build_nps_topic,
    retrieve_incident_candidates,
)
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
            {"id": key, "description": text}
            for key, text in zip(ids, build_incident_text(incidents))
        ]
        for row, date in zip(rows, incident_occurrence_dates(incidents)[0]):
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
                zip(analytical_response_ids(source), source["Comment"].fillna(""))
            )
        ]
        for row, date in zip(
            comments,
            pd.to_datetime(
                source.get("Fecha", pd.Series(pd.NaT, index=source.index)), errors="coerce"
            ),
        ):
            row["date"] = date.isoformat() if pd.notna(date) else ""
        scopes = {
            mode: digest(
                [
                    HELIX_SCHEMA,
                    INSTRUCTIONS_VERSION,
                    mode,
                    taxonomy_fingerprint(catalogs[mode]),
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
            "incident_frame": incidents,
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
                semantic_fingerprint = taxonomy_fingerprint(inputs["taxonomies"][mode])
                rows = db.execute(
                    "SELECT incident, fingerprint, payload FROM helix_classifications WHERE context=? AND scope=?",
                    (context_key(context), scope),
                ).fetchall()
                current[mode] = {}
                for key, fingerprint, payload in rows:
                    if fingerprints.get(key) != fingerprint:
                        continue
                    item = strict_json(payload.encode())
                    if (
                        item.get("instructions_version") != INSTRUCTIONS_VERSION
                        or item.get("taxonomy_fingerprint") != semantic_fingerprint
                    ):
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
        retrieved = retrieve_incident_candidates(inputs["frame"], inputs["incident_frame"])
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
                    "secondary": [
                        category_ids[mode][(pair["lever"], pair["sublever"])]
                        for pair in row["secondary_classifications"]
                    ],
                }
            )
        for row in pending:
            row["candidates"] = candidates.get(row["id"], [])
        batches = bounded_batches(pending, "incidents")
        manifest = {
            "schema_version": HELIX_SCHEMA,
            "instructions_version": INSTRUCTIONS_VERSION,
            "taxonomies_sha256": digest(catalogs),
            "taxonomy_fingerprint": taxonomy_fingerprint(inputs["taxonomies"][mode]),
            "job_id": job_id,
            "stage": "helix",
            "taxonomy_scopes": inputs["scopes"],
            "batches": [
                {"id": key, "count": len(rows), "sha256": digest({"incidents": rows})}
                for key, rows in batches.items()
            ],
        }
        shared: dict[str, Any] = {"taxonomies.json": catalogs}
        result = write_numbered_zips(
            self.downloads, "incidencias_helix", manifest, batches, shared, "incidents"
        )
        candidate_ids = {candidate["id"] for row in pending for candidate in row["candidates"]}
        job = {
            "manifest": manifest,
            "batches": batches,
            "comment_hashes": {key: digest(comments[key]) for key in candidate_ids},
        }
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
        mode = inputs["modes"][0]
        if manifest["taxonomy_fingerprint"] != taxonomy_fingerprint(inputs["taxonomies"][mode]):
            raise ValueError("taxonomy_fingerprint incompatible; exporta de nuevo.")
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
            response = validate_payload(IncidentResponse, normalize_helix_payload(payload), name)
            expected = job["batches"][key]
            if [row.id for row in response.classifications] != [row["id"] for row in expected]:
                raise ValueError("IDs u orden de incidencias incorrectos.")
            for row, original in zip(response.classifications, expected):
                if source.get(row.id) != {
                    k: v
                    for k, v in original.items()
                    if k not in {"pending_taxonomies", "candidates"}
                }:
                    raise ValueError("Las incidencias han cambiado; exporta de nuevo.")
                mode = inputs["modes"][0]
                expanded = row.expand(catalogs[mode])
                primary = expanded["primary_classification"]
                supplied = {candidate["id"] for candidate in original["candidates"]}
                if len({link.nps_id for link in row.links}) != len(row.links):
                    raise ValueError("Vínculos NPS duplicados.")
                for link in row.links:
                    comment = comments.get(link.nps_id)
                    if not comment or link.nps_id not in supplied:
                        raise ValueError(
                            "Vínculo a comentario desconocido o no candidato de esta incidencia."
                        )
                    if job["comment_hashes"].get(link.nps_id) != digest(comment):
                        raise ValueError("La evidencia NPS ha cambiado; exporta de nuevo.")
                    if row.primary is None:
                        raise ValueError("Una incidencia sin clasificación no admite vínculos.")
                    validate_quote(link.incident_quote, original["description"])
                    validate_quote(link.comment_quote, comment["Comment"])
                    if primary["lever"] == "Sin clasificación temática":
                        raise ValueError("Las categorías de reserva no justifican vínculos NPS.")
                validated[(mode, row.id)] = {
                    "instructions_version": INSTRUCTIONS_VERSION,
                    "taxonomy_fingerprint": manifest["taxonomy_fingerprint"],
                    **primary,
                    "secondary_classifications": expanded["secondary_classifications"],
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
        max_days_apart: int = LINK_MAX_DAYS_APART,
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
            ],
        )
