"""Helix classifications per taxonomy, independent of causal presentation methods."""

from __future__ import annotations

import io
import uuid
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from nps_lens.analytics.nps_helix_link import build_incident_display_text, build_nps_topic
from nps_lens.analytics.taxonomy import MODES
from nps_lens.domain.models import UploadContext
from nps_lens.domain.record_identity import analytical_response_ids
from nps_lens.platform.downloads import persist_download
from nps_lens.services.taxonomy_exchange import (
    BATCH_ROWS,
    MAX_EXPANDED_BYTES,
    MAX_MEMBER_BYTES,
    MAX_MEMBERS,
    MAX_ZIP_BYTES,
    digest,
    encode,
    read_response,
    strict_json,
)
from nps_lens.services.taxonomy_prompts import PROJECT_INSTRUCTIONS
from nps_lens.services.taxonomy_service import TaxonomyService, context_key


class EvidenceLink(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    nps_id: str
    confidence: float = Field(ge=0, le=1)


class TaxonomyAssignment(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    taxonomy_mode: str
    lever: str
    sublever: str
    rationale: str = Field(min_length=1, max_length=2000)
    links: list[EvidenceLink] = Field(max_length=20)


class IncidentClassification(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    id: str
    assignments: list[TaxonomyAssignment] = Field(min_length=1, max_length=3)


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

    def inputs(
        self, context: UploadContext, incidents: pd.DataFrame, modes: list[str]
    ) -> dict[str, Any]:
        if self.taxonomy.state(context).get("restored"):
            raise ValueError("Vuelve al dataset local para usar el intercambio Helix.")
        if not modes or len(modes) != len(set(modes)) or any(mode not in MODES for mode in modes):
            raise ValueError("Selecciona entre una y tres taxonomías disponibles.")
        source = self.taxonomy.source(context).sort_values("_business_key")
        resolved = {
            mode: self.taxonomy.resolve(context, source, mode) for mode in MODES if mode in modes
        }
        catalogs = {mode: self.taxonomy.catalog(context, mode) for mode in resolved}
        if any(not catalog["taxonomy"] for catalog in catalogs.values()):
            raise ValueError("Alguna taxonomía seleccionada no contiene Palancas/Subpalancas.")
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
                    "helix-taxonomy/2",
                    mode,
                    catalogs[mode],
                    [(row["id"], row["Comment"], row["taxonomies"][mode]) for row in comments],
                ]
            )
            for mode in resolved
        }
        return {
            "modes": list(resolved),
            "taxonomies": catalogs,
            "scopes": scopes,
            "incidents": rows,
            "comments": comments,
        }

    def current(self, context: UploadContext, inputs: dict[str, Any]) -> dict[str, dict[str, Any]]:
        fingerprints = {row["id"]: digest(row) for row in inputs["incidents"]}
        current: dict[str, dict[str, Any]] = {}
        with self.repository._connect() as db:
            for mode, scope in inputs["scopes"].items():
                rows = db.execute(
                    "SELECT incident, fingerprint, payload FROM helix_classifications WHERE context=? AND scope=?",
                    (context_key(context), scope),
                ).fetchall()
                current[mode] = {
                    key: strict_json(payload.encode())
                    for key, fingerprint, payload in rows
                    if fingerprints.get(key) == fingerprint
                }
        return current

    def status(self, context: UploadContext, inputs: dict[str, Any]) -> dict[str, Any]:
        current = self.current(context, inputs)
        total = len(inputs["incidents"])
        counts = {
            mode: {"received": len(rows), "pending": total - len(rows)}
            for mode, rows in current.items()
        }
        return {
            "taxonomies": counts,
            "pending": sum(item["pending"] for item in counts.values()),
            "ready": bool(total and all(item["pending"] == 0 for item in counts.values())),
        }

    def export(self, context: UploadContext, inputs: dict[str, Any]) -> dict[str, Any]:
        known = self.current(context, inputs)
        pending = [
            {
                **row,
                "pending_taxonomies": [
                    mode for mode in inputs["modes"] if row["id"] not in known[mode]
                ],
            }
            for row in inputs["incidents"]
        ]
        pending = [row for row in pending if row["pending_taxonomies"]]
        if not pending:
            raise ValueError("No hay incidencias pendientes para las taxonomías seleccionadas.")
        job_id = uuid.uuid4().hex
        batches = {
            f"{i // BATCH_ROWS + 1:06d}": pending[i : i + BATCH_ROWS]
            for i in range(0, len(pending), BATCH_ROWS)
        }
        manifest = {
            "schema_version": "nps-lens-helix/2",
            "job_id": job_id,
            "stage": "helix",
            "taxonomy_scopes": inputs["scopes"],
            "batches": [
                {"id": key, "count": len(rows), "sha256": digest({"incidents": rows})}
                for key, rows in batches.items()
            ],
        }
        files = {"manifest.json": manifest, "taxonomies.json": inputs["taxonomies"]}
        files.update(
            {f"incidents/{key}.json": {"incidents": rows} for key, rows in batches.items()}
        )
        files.update(
            {
                f"comments/{i // BATCH_ROWS + 1:06d}.json": {
                    "comments": inputs["comments"][i : i + BATCH_ROWS]
                }
                for i in range(0, len(inputs["comments"]), BATCH_ROWS)
            }
        )
        raw_files = {name: encode(payload) for name, payload in files.items()}
        if (
            len(files) >= MAX_MEMBERS
            or any(len(raw) > MAX_MEMBER_BYTES for raw in raw_files.values())
            or sum(map(len, raw_files.values())) > MAX_EXPANDED_BYTES
        ):
            raise ValueError("El corpus supera los límites del intercambio ZIP; reduce el dataset.")
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
            for name, raw in raw_files.items():
                archive.writestr(name, raw)
            archive.writestr("INSTRUCCIONES.txt", PROJECT_INSTRUCTIONS["helix"])
        if len(output.getvalue()) > MAX_ZIP_BYTES:
            raise ValueError("ZIP demasiado grande.")
        path = persist_download(output.getvalue(), f"nps-lens-helix-{job_id}.zip", self.downloads)
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
        return {"saved_path": str(path), "pending": len(pending)}

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
        if manifest != job["manifest"] or manifest["taxonomy_scopes"] != inputs["scopes"]:
            raise ValueError(
                "La selección, las taxonomías o el corpus han cambiado; exporta de nuevo."
            )
        source = {row["id"]: row for row in inputs["incidents"]}
        comments = {row["id"]: row for row in inputs["comments"]}
        pairs = {
            mode: {
                (branch["lever"], sub)
                for branch in catalog["taxonomy"]
                for sub in branch["sublevers"]
            }
            for mode, catalog in inputs["taxonomies"].items()
        }
        validated: dict[tuple[str, str], dict[str, Any]] = {}
        if not files:
            raise ValueError("No hay lotes de respuesta.")
        for name, payload in files.items():
            key = name.removeprefix("results/").removesuffix(".json")
            if name != f"results/{key}.json" or key not in job["batches"]:
                raise ValueError("Lote de respuesta desconocido.")
            response = IncidentResponse.model_validate(payload)
            expected = job["batches"][key]
            if [row.id for row in response.classifications] != [row["id"] for row in expected]:
                raise ValueError("IDs u orden de incidencias incorrectos.")
            for row, original in zip(response.classifications, expected):
                if source.get(row.id) != {
                    k: v for k, v in original.items() if k != "pending_taxonomies"
                }:
                    raise ValueError("Las incidencias han cambiado; exporta de nuevo.")
                if [assignment.taxonomy_mode for assignment in row.assignments] != original[
                    "pending_taxonomies"
                ]:
                    raise ValueError(
                        "Cada incidencia requiere exactamente las taxonomías pendientes, en orden."
                    )
                for assignment in row.assignments:
                    mode = assignment.taxonomy_mode
                    if (assignment.lever, assignment.sublever) not in pairs[mode] and (
                        assignment.lever or assignment.sublever
                    ):
                        raise ValueError("Palanca/Subpalanca fuera de la taxonomía.")
                    if len({link.nps_id for link in assignment.links}) != len(assignment.links):
                        raise ValueError("Vínculos NPS duplicados.")
                    for link in assignment.links:
                        comment = comments.get(link.nps_id)
                        if (
                            not comment
                            or comment["taxonomies"][mode]
                            != {"lever": assignment.lever, "sublever": assignment.sublever}
                            or not assignment.lever
                        ):
                            raise ValueError("Vínculo a comentario desconocido o a otra categoría.")
                    validated[(mode, row.id)] = assignment.model_dump()
        existing = self.current(context, inputs)
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
                        digest(source[key]),
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
    ) -> pd.DataFrame:
        if len(inputs["modes"]) != 1 or not self.status(context, inputs)["ready"]:
            raise ValueError(
                "Importa la respuesta Helix completa para la lente activa antes de usar causalidad LLM."
            )
        mode = inputs["modes"][0]
        nps_topics = dict(zip(analytical_response_ids(focus), build_nps_topic(focus)))
        incident_ids = set(incidents["Incident Number"].astype(str))
        rows = [
            {
                "nps_id": link["nps_id"],
                "incident_id": key,
                "similarity": link["confidence"],
                "nps_topic": nps_topics[link["nps_id"]],
                "incident_topic": row["lever"] + " > " + row["sublever"],
                "llm_rationale": row["rationale"],
            }
            for key, row in self.current(context, inputs)[mode].items()
            if key in incident_ids
            for link in row["links"]
            if link["nps_id"] in nps_topics
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
