"""Local, validated Helix classification exchange; no model or browser execution."""

from __future__ import annotations

import io
import uuid
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from nps_lens.analytics.nps_helix_link import build_incident_display_text, build_nps_topic
from nps_lens.domain.causal_methods import TOUCHPOINT_MODE_OPTIONS
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
    read_zip,
    strict_json,
)
from nps_lens.services.taxonomy_prompts import PROJECT_INSTRUCTIONS
from nps_lens.services.taxonomy_service import TaxonomyService, context_key

HELIX_CLASSIFIER_URL = "https://chatgpt.com/g/g-p-6aba1edf109c81a4880f5420a0105b37"


class EvidenceLink(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    nps_id: str
    confidence: float = Field(ge=0, le=1)


class IncidentClassification(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    id: str
    lever: str
    sublever: str
    entity: str
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

    def inputs(
        self, context: UploadContext, incidents: pd.DataFrame, mode: str, method: str
    ) -> dict[str, Any]:
        if self.taxonomy.state(context).get("restored"):
            raise ValueError("Vuelve al dataset local para usar el intercambio Helix.")
        if method not in TOUCHPOINT_MODE_OPTIONS:
            raise ValueError("Método causal desconocido.")
        frame = self.taxonomy.resolve(context, mode=mode).sort_values("_business_key")
        taxonomy = self.taxonomy.catalog(context, mode)
        if not taxonomy["taxonomy"]:
            raise ValueError("La taxonomía no contiene Palancas/Subpalancas.")
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
        comments = [
            {"id": key, "Comment": str(comment), "lever": str(lever), "sublever": str(sub)}
            for key, comment, lever, sub in zip(
                analytical_response_ids(frame),
                frame["Comment"].fillna(""),
                frame["Palanca"].fillna(""),
                frame["Subpalanca"].fillna(""),
            )
        ]
        scope = digest([mode, method, taxonomy, comments])
        return {
            "mode": mode,
            "method": method,
            "taxonomy": taxonomy,
            "scope": scope,
            "incidents": rows,
            "comments": comments,
        }

    def current(self, context: UploadContext, inputs: dict[str, Any]) -> dict[str, Any]:
        fingerprints = {row["id"]: digest(row) for row in inputs["incidents"]}
        with self.repository._connect() as db:
            rows = db.execute(
                "SELECT incident, fingerprint, payload FROM helix_classifications WHERE context=? AND scope=?",
                (context_key(context), inputs["scope"]),
            ).fetchall()
        return {
            key: strict_json(payload.encode())
            for key, fingerprint, payload in rows
            if fingerprints.get(key) == fingerprint
        }

    def status(self, context: UploadContext, inputs: dict[str, Any]) -> dict[str, Any]:
        received = len(self.current(context, inputs))
        total = len(inputs["incidents"])
        return {
            "received": received,
            "pending": total - received,
            "ready": bool(total and received == total),
            "engine": self.taxonomy.state(context).get("causal_engine", "rules"),
        }

    def export(self, context: UploadContext, inputs: dict[str, Any]) -> dict[str, Any]:
        known = self.current(context, inputs)
        pending = [row for row in inputs["incidents"] if row["id"] not in known]
        if not pending:
            raise ValueError("No hay incidencias pendientes para esta taxonomía y método.")
        job_id = uuid.uuid4().hex
        batches = {
            f"{i // BATCH_ROWS + 1:06d}": pending[i : i + BATCH_ROWS]
            for i in range(0, len(pending), BATCH_ROWS)
        }
        manifest = {
            "schema_version": "nps-lens-helix/1",
            "job_id": job_id,
            "stage": "helix",
            "taxonomy_mode": inputs["mode"],
            "causal_method": inputs["method"],
            "scope_sha256": inputs["scope"],
            "taxonomy_sha256": digest(inputs["taxonomy"]),
            "batches": [
                {"id": key, "count": len(rows), "sha256": digest({"incidents": rows})}
                for key, rows in batches.items()
            ],
        }
        files = {"manifest.json": manifest, "taxonomy.json": inputs["taxonomy"]}
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
        files = read_zip(content)
        manifest = files.pop("manifest.json", None)
        if not isinstance(manifest, dict):
            raise ValueError("Falta manifest.json.")
        with self.repository._connect() as db:
            found = db.execute(
                "SELECT payload FROM helix_exchange WHERE id=? AND context=?",
                (manifest.get("job_id"), context_key(context)),
            ).fetchone()
        if not found:
            raise ValueError("El intercambio no pertenece a este dataset.")
        job = strict_json(found[0].encode())
        if manifest != job["manifest"] or manifest["scope_sha256"] != inputs["scope"]:
            raise ValueError("La taxonomía, el método o el corpus han cambiado; exporta de nuevo.")
        source = {row["id"]: row for row in inputs["incidents"]}
        comments = {row["id"]: row for row in inputs["comments"]}
        pairs = {
            (branch["lever"], sub)
            for branch in inputs["taxonomy"]["taxonomy"]
            for sub in branch["sublevers"]
        }
        validated = {}
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
                if source.get(row.id) != original:
                    raise ValueError("Las incidencias han cambiado; exporta de nuevo.")
                if (row.lever, row.sublever) not in pairs and (row.lever or row.sublever):
                    raise ValueError("Palanca/Subpalanca fuera de la taxonomía.")
                if len({link.nps_id for link in row.links}) != len(row.links):
                    raise ValueError("Vínculos NPS duplicados.")
                for link in row.links:
                    comment = comments.get(link.nps_id)
                    if not comment or (comment["lever"], comment["sublever"]) != (
                        row.lever,
                        row.sublever,
                    ):
                        raise ValueError("Vínculo a comentario desconocido o a otra categoría.")
                method = inputs["method"]
                required = {
                    "palanca_touchpoint": row.lever,
                    "domain_touchpoint": row.sublever,
                    "bbva_source_service_n2": original["source_service_n2"],
                }.get(method)
                if required is not None and row.entity != required:
                    raise ValueError("La entidad no corresponde al método causal.")
                if row.links and not row.entity.strip():
                    raise ValueError("Los vínculos requieren una entidad causal.")
                validated[row.id] = row.model_dump()
        existing = self.current(context, inputs)
        if any(key in existing and existing[key] != value for key, value in validated.items()):
            raise ValueError("Una incidencia ya tiene una respuesta diferente.")
        with self.repository._connect() as db:
            db.executemany(
                "INSERT OR REPLACE INTO helix_classifications VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        context_key(context),
                        inputs["scope"],
                        key,
                        digest(source[key]),
                        encode(value).decode(),
                    )
                    for key, value in validated.items()
                ],
            )
            # Retain only the five most recently used scopes for this owner.
            db.execute(
                "DELETE FROM helix_classifications WHERE context=? AND scope NOT IN (SELECT scope FROM helix_classifications WHERE context=? GROUP BY scope ORDER BY MAX(rowid) DESC LIMIT 5)",
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
        if not self.status(context, inputs)["ready"]:
            raise ValueError(
                "Importa el ZIP Helix completo para la taxonomía y método actuales antes de usar causalidad LLM."
            )
        nps_topics = dict(zip(analytical_response_ids(focus), build_nps_topic(focus)))
        incident_ids = set(incidents["Incident Number"].astype(str))
        rows = [
            {
                "nps_id": link["nps_id"],
                "incident_id": key,
                "similarity": link["confidence"],
                "nps_topic": nps_topics[link["nps_id"]],
                "incident_topic": row["entity"],
                "llm_entity": row["entity"],
                "llm_rationale": row["rationale"],
                "palanca": row["lever"],
                "subpalanca": row["sublever"],
            }
            for key, row in self.current(context, inputs).items()
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
                "llm_entity",
                "llm_rationale",
                "palanca",
                "subpalanca",
            ],
        )
