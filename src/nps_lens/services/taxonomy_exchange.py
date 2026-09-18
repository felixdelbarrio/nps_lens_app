"""Versioned, resumable ZIP exchange. Archives are never extracted or executed."""

from __future__ import annotations

import hashlib
import io
import json
import stat
import uuid
import zipfile
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nps_lens.analytics.taxonomy import signature
from nps_lens.domain.models import UploadContext
from nps_lens.platform.downloads import persist_download
from nps_lens.services.taxonomy_discovery import (
    ClassificationResponse,
    TaxonomyResponse,
    TaxonomyValidator,
)
from nps_lens.services.taxonomy_prompts import INSTRUCTIONS_VERSION, PROJECT_INSTRUCTIONS
from nps_lens.services.taxonomy_service import TaxonomyService, context_key

MAX_ZIP_BYTES = 32 * 1024 * 1024
MAX_EXPANDED_BYTES = 128 * 1024 * 1024
MAX_MEMBER_BYTES = 2 * 1024 * 1024
MAX_MEMBERS = 4096
BATCH_ROWS = 200
BATCH_BYTES = 80_000
MAX_RETAINED_JOBS = 3


def encode(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(encode(value)).hexdigest()


def strict_json(raw: bytes) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("JSON con claves duplicadas.")
            result[key] = value
        return result

    def invalid(value: str) -> None:
        raise ValueError("Constante JSON no válida.")

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid)
    except (UnicodeError, RecursionError) as exc:
        raise ValueError("JSON UTF-8 inválido.") from exc


def read_zip(content: bytes) -> dict[str, Any]:
    if len(content) > MAX_ZIP_BYTES:
        raise ValueError("ZIP demasiado grande (máximo 32 MiB).")
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            members = archive.infolist()
            names = [item.filename for item in members]
            if not members or len(members) > MAX_MEMBERS or len(names) != len(set(names)):
                raise ValueError("ZIP vacío, con demasiados ficheros o nombres duplicados.")
            if sum(item.file_size for item in members) > MAX_EXPANDED_BYTES:
                raise ValueError("ZIP expandido demasiado grande (máximo 128 MiB).")
            result = {}
            for item in members:
                name = item.filename
                if (
                    item.is_dir()
                    or name.startswith("/")
                    or "\\" in name
                    or any(part in ("", ".", "..") for part in name.split("/"))
                    or stat.S_ISLNK(item.external_attr >> 16)
                    or item.flag_bits & 1
                    or item.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
                    or item.file_size > MAX_MEMBER_BYTES
                    or not name.endswith(".json")
                ):
                    raise ValueError("El ZIP contiene una entrada no permitida.")
                with archive.open(item) as handle:
                    raw = handle.read(MAX_MEMBER_BYTES + 1)
                if len(raw) > MAX_MEMBER_BYTES:
                    raise ValueError("Fichero JSON demasiado grande.")
                result[name] = strict_json(raw)
            return result
    except (zipfile.BadZipFile, RuntimeError, EOFError, NotImplementedError, zlib.error) as exc:
        raise ValueError("ZIP corrupto o no compatible.") from exc


class TaxonomyExchange:
    def __init__(self, taxonomy: TaxonomyService, downloads: Path):
        self.taxonomy = taxonomy
        self.repository = taxonomy.repository
        self.downloads = downloads
        with self.repository._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS taxonomy_exchange (id TEXT PRIMARY KEY, context TEXT NOT NULL, payload TEXT NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS taxonomy_exchange_batches (job TEXT NOT NULL, batch TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(job, batch))"
            )

    def _frame(self, context: UploadContext) -> Any:
        if self.taxonomy.state(context).get("restored"):
            raise ValueError("Snapshot inmutable: vuelve al dataset local.")
        return (
            self.taxonomy.resolver.resolve(
                self.taxonomy.source(context), "NORMALIZED", self.taxonomy.registry(context)
            )
            .sort_values("_business_key")
            .reset_index(drop=True)
        )

    def _corpus(self, context: UploadContext, frame: Any) -> str:
        return digest(
            [
                context_key(context),
                list(
                    zip(
                        frame["_business_key"].astype(str),
                        frame["Comment"].astype("string").fillna(""),
                    )
                ),
            ]
        )

    def _save(self, context: UploadContext, job: dict[str, Any]) -> None:
        with self.repository._connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO taxonomy_exchange VALUES (?, ?, ?)",
                (job["id"], context_key(context), encode(job).decode()),
            )
            stale = [
                row[0]
                for row in db.execute(
                    "SELECT id FROM taxonomy_exchange WHERE context=? "
                    "ORDER BY rowid DESC LIMIT -1 OFFSET ?",
                    (context_key(context), MAX_RETAINED_JOBS),
                ).fetchall()
            ]
            if stale:
                placeholders = ",".join("?" for _ in stale)
                db.execute(
                    f"DELETE FROM taxonomy_exchange_batches WHERE job IN ({placeholders})",
                    stale,
                )
                db.execute(
                    f"DELETE FROM taxonomy_exchange WHERE id IN ({placeholders})",
                    stale,
                )

    def _load(self, context: UploadContext, job_id: str) -> dict[str, Any]:
        with self.repository._connect() as db:
            row = db.execute(
                "SELECT payload FROM taxonomy_exchange WHERE id=? AND context=?",
                (job_id, context_key(context)),
            ).fetchone()
        if row is None:
            raise ValueError("El ZIP no corresponde a un intercambio de este dataset.")
        return dict(strict_json(row[0].encode()))

    def _manifest(self, job: dict[str, Any], stage: str) -> dict[str, Any]:
        return {
            "schema_version": "nps-lens-zip/1",
            "job_id": job["id"],
            "stage": stage,
            "corpus_sha256": job["corpus"],
            "taxonomy_sha256": digest(job["taxonomy"]) if stage == "classifier" else None,
            "batches": [
                {"id": key, "count": len(rows), "sha256": digest({"comments": rows})}
                for key, rows in job["batches"].items()
            ],
        }

    def export(self, context: UploadContext) -> dict[str, Any]:
        frame = self._frame(context)
        if frame.empty:
            raise ValueError("No hay comentarios que exportar.")
        batches: dict[str, list[dict[str, str]]] = {}
        rows: list[dict[str, str]] = []
        size = total = 0
        for index, comment in enumerate(frame["Comment"].astype("string").fillna("")):
            row = {"id": str(index + 1), "Comment": str(comment)}
            cost = len(encode(row)) + 1
            if cost > BATCH_BYTES:
                raise ValueError("Un comentario supera 80.000 bytes; no se truncará.")
            if rows and (len(rows) >= BATCH_ROWS or size + cost > BATCH_BYTES):
                batches[f"{len(batches) + 1:06d}"] = rows
                rows, size = [], 0
            rows.append(row)
            size += cost
            total += cost
        if rows:
            batches[f"{len(batches) + 1:06d}"] = rows
        if total > MAX_EXPANDED_BYTES // 2 or len(batches) > MAX_MEMBERS - 4:
            raise ValueError(
                "Corpus demasiado grande para un intercambio ZIP (64 MiB o 4092 lotes). Divide el dataset explícitamente."
            )
        job = {
            "id": uuid.uuid4().hex,
            "corpus": self._corpus(context, frame),
            "batches": batches,
            "taxonomy": None,
            "stage": "designer",
            "instructions_version": INSTRUCTIONS_VERSION,
        }
        result = self._write(job)
        self._save(context, job)
        return result

    def _write(self, job: dict[str, Any]) -> dict[str, Any]:
        stage = job["stage"]
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
            archive.writestr("manifest.json", encode(self._manifest(job, stage)))
            archive.writestr("INSTRUCCIONES.txt", PROJECT_INSTRUCTIONS[stage])
            if stage == "classifier":
                archive.writestr("taxonomy.json", encode(job["taxonomy"]))
            for key, rows in job["batches"].items():
                archive.writestr(f"comments/{key}.json", encode({"comments": rows}))
        content = output.getvalue()
        if len(content) > MAX_ZIP_BYTES:
            raise ValueError("El ZIP de entrada supera 32 MiB.")
        path = persist_download(content, f"nps-lens-{stage}-{job['id']}.zip", self.downloads)
        return {
            "job_id": job["id"],
            "stage": stage,
            "saved_path": str(path),
            "batches": len(job["batches"]),
        }

    def import_response(self, context: UploadContext, content: bytes) -> dict[str, Any]:
        files = read_zip(content)
        manifest = files.pop("manifest.json", None)
        if not isinstance(manifest, dict) or not isinstance(manifest.get("job_id"), str):
            raise ValueError("Falta manifest.json válido.")
        job = self._load(context, manifest["job_id"])
        stage = manifest.get("stage")
        if stage not in ("designer", "classifier") or json.dumps(
            manifest, sort_keys=True
        ) != json.dumps(self._manifest(job, stage), sort_keys=True):
            raise ValueError("El manifiesto no coincide con la exportación.")
        frame = self._frame(context)
        if self._corpus(context, frame) != job["corpus"]:
            raise ValueError("El corpus ha cambiado. Exporta un nuevo ZIP; no se importó nada.")
        if stage == "designer":
            if set(files) != {"taxonomy.json"}:
                raise ValueError("Designer debe devolver solo manifest.json y taxonomy.json.")
            taxonomy = TaxonomyResponse.model_validate(files["taxonomy.json"])
            TaxonomyValidator._validate_taxonomy(taxonomy)
            value = taxonomy.model_dump()
            if job["taxonomy"] is not None and job["taxonomy"] != value:
                raise ValueError("Este intercambio ya tiene otra taxonomía. Inicia uno nuevo.")
            if job["stage"] == "complete":
                return {"job_id": job["id"], "stage": "complete"}
            job.update(taxonomy=value, stage="classifier")
            result = self._write(job)
            self._save(context, job)
            return result
        if job["taxonomy"] is None:
            raise ValueError("Importa primero la taxonomía.")
        taxonomy = TaxonomyResponse.model_validate(job["taxonomy"])
        categories = TaxonomyValidator._validate_taxonomy(taxonomy)
        if not files:
            raise ValueError("No hay resultados de clasificación.")
        validated = {}
        for name, payload in files.items():
            key = name.removeprefix("results/").removesuffix(".json")
            if name != f"results/{key}.json" or key not in job["batches"]:
                raise ValueError("Fichero de resultado no esperado.")
            response = ClassificationResponse.model_validate(payload)
            expected = [row["id"] for row in job["batches"][key]]
            if [row.id for row in response.classifications] != expected:
                raise ValueError("Los IDs y el orden deben coincidir exactamente con el lote.")
            TaxonomyValidator._validate_batch(response, expected, categories)
            validated[key] = encode(response.model_dump()).decode()
        with self.repository._connect() as db:
            existing = dict(
                db.execute(
                    "SELECT batch, payload FROM taxonomy_exchange_batches WHERE job=?", (job["id"],)
                ).fetchall()
            )
            for key, payload in validated.items():
                if key in existing and existing[key] != payload:
                    raise ValueError("Un lote ya importado tiene un resultado diferente.")
            existing.update(validated)
            if len(existing) == len(job["batches"]) and job["stage"] != "complete":
                assignments = [
                    row["primary_classification"]
                    for key in job["batches"]
                    for row in strict_json(existing[key].encode())["classifications"]
                ]
                config = {
                    "method": "chatgpt_zip",
                    "taxonomy_sha256": digest(job["taxonomy"]),
                    "instructions_version": job["instructions_version"],
                }
                registry = self.taxonomy.registry(context)
                sig = signature(frame, "DISCOVERED", config, registry.signature("nps"))
                artifact = {
                    "mode": "DISCOVERED",
                    "signature": sig,
                    "keys": frame["_business_key"].tolist(),
                    "config": config,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "lever": [row["lever"] for row in assignments],
                    "sublever": [row["sublever"] for row in assignments],
                    "provenance": ["chatgpt_zip"] * len(assignments),
                    "nodes": [],
                    "taxonomy": job["taxonomy"],
                    "equivalences": {},
                }
                state = self.taxonomy.state(context)
                state.setdefault("artifacts", {})["DISCOVERED"] = sig
                db.execute(
                    "INSERT OR REPLACE INTO taxonomy_artifacts VALUES (?, ?, ?, ?)",
                    (sig, context_key(context), "DISCOVERED", encode(artifact).decode()),
                )
                db.execute(
                    "INSERT OR REPLACE INTO taxonomy_state VALUES (?, ?)",
                    (context_key(context), encode(state).decode()),
                )
                job["stage"] = "complete"
                db.execute(
                    "UPDATE taxonomy_exchange SET payload=? WHERE id=?",
                    (encode(job).decode(), job["id"]),
                )
            db.executemany(
                "INSERT OR IGNORE INTO taxonomy_exchange_batches VALUES (?, ?, ?)",
                [(job["id"], key, payload) for key, payload in validated.items()],
            )
        self.taxonomy._state_cache.clear()
        self.taxonomy._cache.clear()
        return {
            "job_id": job["id"],
            "stage": job["stage"],
            "received": len(existing),
            "batches": len(job["batches"]),
            "pending": [key for key in job["batches"] if key not in existing],
        }
