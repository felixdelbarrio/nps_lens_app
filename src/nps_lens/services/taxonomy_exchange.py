"""Versioned, bounded ZIP exchange with strict JSON payloads for the GPT projects."""

from __future__ import annotations

import io
import json
import re
import stat
import tempfile
import uuid
import zipfile
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, TypeVar

from pydantic import BaseModel, ValidationError

from nps_lens.analytics.taxonomy import signature
from nps_lens.domain.models import UploadContext
from nps_lens.platform.downloads import persist_download
from nps_lens.services.classification_protocol import (
    CLASSIFICATION_BATCH_BYTES,
    CLASSIFICATION_BATCH_ROWS,
    CLASSIFIER_SCHEMA,
    HELIX_SCHEMA,
    CompactCommentResponse,
    category_catalog,
    comment_classification_fingerprint,
    digest,
    encode,
    taxonomy_fingerprint,
)
from nps_lens.services.taxonomy_discovery import (
    TaxonomyDesignResponse,
    TaxonomyValidator,
)
from nps_lens.services.taxonomy_prompts import (
    FALLBACK_LEVER,
    FALLBACK_SUBLEVERS,
    INSTRUCTIONS_VERSION,
    PROJECT_INSTRUCTIONS,
)
from nps_lens.services.taxonomy_service import TaxonomyService, context_key

MAX_ZIP_BYTES = 32 * 1024 * 1024
MAX_EXPANDED_BYTES = 128 * 1024 * 1024
MAX_MEMBER_BYTES = 2 * 1024 * 1024
MAX_MEMBERS = 4096
BATCH_ROWS = 200
BATCH_BYTES = 80_000
MAX_RETAINED_JOBS = 3


def bounded_batches(
    items: Iterable[dict[str, Any]],
    field: str,
) -> dict[str, list[dict[str, Any]]]:
    batches: dict[str, list[dict[str, Any]]] = {}
    rows: list[dict[str, Any]] = []
    overhead = len(encode({field: []}))
    size = overhead
    for row in items:
        cost = len(encode(row))
        if overhead + cost > CLASSIFICATION_BATCH_BYTES:
            raise ValueError(
                f"Un elemento supera {CLASSIFICATION_BATCH_BYTES:,} bytes; no se truncará."
            )
        if rows and (
            len(rows) >= CLASSIFICATION_BATCH_ROWS or size + 1 + cost > CLASSIFICATION_BATCH_BYTES
        ):
            batches[f"{len(batches) + 1:06d}"] = rows
            rows, size = [], overhead
        size += cost + bool(rows)
        rows.append(row)
    if rows:
        batches[f"{len(batches) + 1:06d}"] = rows
    return batches


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


Model = TypeVar("Model", bound=BaseModel)


def validate_payload(model: type[Model], payload: Any, filename: str) -> Model:
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        error = exc.errors(include_input=False, include_url=False)[0]
        location = ".".join(map(str, error["loc"]))[:120]
        raise ValueError(
            f"{filename}: formato inválido en {location}. "
            "Revisa las instrucciones del proyecto y vuelve a generar este lote. No se importó nada."
        ) from exc


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


def read_response(content: bytes, stage: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """One bounded ZIP contract for all three projects; JSON is validated inside it."""
    if stage not in ("designer", "classifier", "helix", "normalizer"):
        raise ValueError("Proyecto desconocido.")
    files = read_zip(content)
    manifest = files.pop("manifest.json", None)
    schema = {
        "classifier": CLASSIFIER_SCHEMA,
        "helix": HELIX_SCHEMA,
        "normalizer": "nps-lens-normalization/2",
    }.get(stage, "nps-lens-taxonomy/4")
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != schema
        or manifest.get("stage") != stage
        or manifest.get("instructions_version") != INSTRUCTIONS_VERSION
        or not isinstance(manifest.get("job_id"), str)
        or not manifest["job_id"]
    ):
        raise ValueError(
            f"manifest.json incompatible con el proyecto seleccionado; se requiere {schema}. Exporta un nuevo ZIP."
        )
    valid = (
        set(files) == {"taxonomy.json" if stage == "designer" else "equivalences.json"}
        if stage in ("designer", "normalizer")
        else bool(files) and all(re.fullmatch(r"results/[0-9]{6}\.json", name) for name in files)
    )
    if not valid:
        expected = {"designer": "taxonomy.json", "normalizer": "equivalences.json"}.get(
            stage, "results/NNNNNN.json"
        )
        raise ValueError(
            f"El ZIP de respuesta debe contener únicamente manifest.json y {expected}."
        )
    return manifest, files


def validate_manifest(manifest: dict[str, Any], expected: dict[str, Any]) -> set[str]:
    """A response belongs to the exact batches advertised by its input ZIP."""
    batches = manifest.get("batches")
    known = {row["id"]: row for row in expected["batches"]}
    if not isinstance(batches, list) or not batches or manifest != {**expected, "batches": batches}:
        raise ValueError("El manifiesto no coincide con la exportación.")
    ids: set[str] = set()
    for batch in batches:
        key = batch.get("id") if isinstance(batch, dict) else None
        if not isinstance(key, str) or key in ids or known.get(key) != batch:
            raise ValueError("El manifiesto contiene un lote desconocido, alterado o repetido.")
        ids.add(key)
    return ids


def write_numbered_zips(
    downloads: Path,
    label: str,
    manifest: dict[str, Any],
    batches: dict[str, list[dict[str, Any]]],
    shared: dict[str, Any],
    field: str,
) -> dict[str, Any]:
    """Publish a complete folder atomically, compressing shared evidence only once."""
    instructions = PROJECT_INSTRUCTIONS[manifest["stage"]].encode()
    expanded = len(instructions)
    if len(shared) + 3 > MAX_MEMBERS:
        raise ValueError("Demasiados ficheros para un intercambio ZIP.")
    base = io.BytesIO()
    with zipfile.ZipFile(base, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("INSTRUCCIONES.txt", instructions)
        for name, payload in shared.items():
            raw = encode(payload)
            expanded += len(raw)
            if len(raw) > MAX_MEMBER_BYTES or expanded > MAX_EXPANDED_BYTES:
                raise ValueError(
                    "El corpus supera los límites del intercambio ZIP; reduce el dataset."
                )
            archive.writestr(name, raw)
    base_content = base.getvalue()
    if len(base_content) > MAX_ZIP_BYTES:
        raise ValueError("El ZIP de entrada supera 32 MiB.")
    directory = downloads / f"{label}-{datetime.now():%Y%m%d-%H%M%S}-{manifest['job_id']}"
    downloads.mkdir(parents=True, exist_ok=True)
    paths = []
    with tempfile.TemporaryDirectory(prefix=".nps-lens-", dir=downloads) as temporary:
        for index, batch in enumerate(manifest["batches"], 1):
            key = batch["id"]
            parts = {
                "manifest.json": encode({**manifest, "batches": [batch]}),
                f"{field}/{key}.json": encode({field: batches[key]}),
            }
            if (
                any(len(raw) > MAX_MEMBER_BYTES for raw in parts.values())
                or expanded + sum(map(len, parts.values())) > MAX_EXPANDED_BYTES
            ):
                raise ValueError(
                    "El corpus supera los límites del intercambio ZIP; reduce el dataset."
                )
            output = io.BytesIO(base_content)
            with zipfile.ZipFile(output, "a", zipfile.ZIP_DEFLATED) as archive:
                for name, raw in parts.items():
                    archive.writestr(name, raw)
            content = output.getvalue()
            if len(content) > MAX_ZIP_BYTES:
                raise ValueError("El ZIP de entrada supera 32 MiB.")
            name = f"{index}_{len(batches)}_{label}.zip"
            persist_download(content, name, Path(temporary))
            paths.append(str(directory / name))
        Path(temporary).rename(directory)
    return {"saved_paths": paths, "saved_directory": str(directory), "batches": len(paths)}


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
                self.taxonomy.source(context), "SOURCE", self.taxonomy.registry(context)
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
            "schema_version": CLASSIFIER_SCHEMA if stage == "classifier" else "nps-lens-taxonomy/4",
            "job_id": job["id"],
            "instructions_version": job["instructions_version"],
            "stage": stage,
            "corpus_sha256": job["corpus"],
            "taxonomy_mode": job["mode"],
            "manual_revision": job["manual_revision"],
            "taxonomy_fingerprint": (
                taxonomy_fingerprint(job["taxonomy"]) if stage == "classifier" else None
            ),
            "taxonomy_sha256": (
                digest({"categories": category_catalog(job["taxonomy"])})
                if stage == "classifier"
                else None
            ),
            "batches": [
                {"id": key, "count": len(rows), "sha256": digest({"comments": rows})}
                for key, rows in job["batches"].items()
            ],
        }

    def assignments(self, context: UploadContext, frame: Any, mode: str) -> dict[str, Any]:
        artifact = self.taxonomy.classification_artifact(context, mode)
        catalog = self.taxonomy.catalog(context, mode)
        revision = (
            self.taxonomy.state(context).get("artifacts", {}).get("COMPLETED", "")
            if mode == "COMPLETED"
            else ""
        )
        if (
            artifact.get("config", {}).get("instructions_version") != INSTRUCTIONS_VERSION
            or artifact.get("taxonomy_fingerprint") != taxonomy_fingerprint(catalog)
            or artifact.get("config", {}).get("manual_revision", "") != revision
        ):
            return {}
        hashes = artifact.get("comment_hashes", {})
        comments = dict(zip(frame["_business_key"], frame["Comment"].fillna("")))
        secondary = artifact.get("secondary_classifications", {})
        by_key = {
            key: {
                "primary_classification": {"lever": lever, "sublever": sub},
                "secondary_classifications": secondary.get(key, []),
            }
            for key, lever, sub in zip(
                artifact.get("keys", []), artifact.get("lever", []), artifact.get("sublever", [])
            )
            if lever and sub and lever.strip() and sub.strip()
        }
        by_fingerprint = {hashes[key]: value for key, value in by_key.items() if hashes.get(key)}
        retained = {}
        for key, comment in comments.items():
            fingerprint = comment_classification_fingerprint(comment)
            value = (
                by_key.get(key)
                if hashes.get(key) == fingerprint
                else by_fingerprint.get(fingerprint)
            )
            if value is not None:
                retained[key] = value
        return retained

    def progress(self, context: UploadContext) -> dict[str, Any]:
        frame = self._frame(context)
        state = self.taxonomy.state(context)
        mode = self.taxonomy.state(context)["active"]
        catalog = self.taxonomy.catalog(context, mode)
        assignments = self.assignments(context, frame, mode) if catalog["taxonomy"] else {}
        received = len(assignments)
        designer = state.get("designer_progress", {})
        designed = (
            designer.get("received", 0)
            if designer.get("corpus") == self._corpus(context, frame)
            else 0
        )
        return {
            "mode": mode,
            "total": len(frame),
            "received": received,
            "pending": len(frame) - received,
            "multiple": sum(bool(row["secondary_classifications"]) for row in assignments.values()),
            "designer": {
                "total": len(frame),
                "received": designed,
                "pending": len(frame) - designed,
                "levers": len(state.get("discovered_taxonomy", {}).get("taxonomy", [])),
                "sublevers": sum(
                    len(branch["sublevers"])
                    for branch in state.get("discovered_taxonomy", {}).get("taxonomy", [])
                ),
            },
        }

    def apply(self, context: UploadContext, frame: Any, mode: str) -> Any:
        assignments = self.assignments(context, frame, mode)
        out = frame.copy()
        keys = out["_business_key"]
        for column, field in (("Palanca", "lever"), ("Subpalanca", "sublever")):
            out[column] = keys.map(
                {key: row["primary_classification"][field] for key, row in assignments.items()}
            ).fillna("")
        out["Temas adicionales"] = keys.map(
            {
                key: " · ".join(
                    pair["lever"] + " > " + pair["sublever"]
                    for pair in row["secondary_classifications"]
                )
                for key, row in assignments.items()
            }
        ).fillna("")
        out.attrs["taxonomy_mode"] = mode
        return out

    def export(self, context: UploadContext, stage: str) -> dict[str, Any]:
        if stage not in ("designer", "classifier"):
            raise ValueError("Proyecto desconocido.")
        pending = stage == "classifier"
        frame = self._frame(context)
        full_frame = frame
        state = self.taxonomy.state(context)
        mode = self.taxonomy.state(context)["active"] if pending else "DISCOVERED"
        taxonomy = self.taxonomy.catalog(context, mode) if pending else None
        revision = state.get("artifacts", {}).get("COMPLETED", "") if mode == "COMPLETED" else ""
        groups: dict[str, list[str]] = {}
        if pending:
            if not taxonomy or not taxonomy["taxonomy"]:
                raise ValueError("Crea o importa primero la taxonomía de la lente LLM.")
            retained = self.assignments(context, frame, mode)
            frame = frame.loc[~frame["_business_key"].isin(retained)]
            fallback = {"lever": FALLBACK_LEVER, "sublever": FALLBACK_SUBLEVERS[0]}
            categories = category_catalog(taxonomy)
            if any(
                all(pair[field] == fallback[field] for field in fallback)
                for pair in categories.values()
            ):
                empty_keys = frame.loc[frame["Comment"].fillna("").eq(""), "_business_key"]
                if len(empty_keys):
                    retained.update(
                        {
                            key: {
                                "primary_classification": fallback,
                                "secondary_classifications": [],
                            }
                            for key in empty_keys
                        }
                    )
                    with self.repository._connect() as db:
                        self._persist_assignments(
                            db,
                            context,
                            full_frame,
                            {
                                "mode": mode,
                                "taxonomy": taxonomy,
                                "manual_revision": revision,
                                "instructions_version": INSTRUCTIONS_VERSION,
                            },
                            retained,
                        )
                    self._clear_caches()
                    frame = frame.loc[~frame["_business_key"].isin(retained)]
            # Sorted business keys choose stable representatives; text is never normalized.
            for key, comment in zip(frame["_business_key"], frame["Comment"].fillna("")):
                if comment in groups:
                    groups[comment].append(key)
                else:
                    groups[comment] = [key]
            rows = [{"id": str(i), "Comment": text} for i, text in enumerate(groups, 1)]
            if not rows:
                return {
                    "stage": "complete",
                    "saved_paths": [],
                    "saved_directory": None,
                    "batches": 0,
                }
            batches = bounded_batches(rows, "comments")
        else:
            if frame.empty:
                raise ValueError("No hay comentarios que exportar.")
            batches = {}
            rows = []
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
        if not pending and (total > MAX_EXPANDED_BYTES // 2 or len(batches) > MAX_MEMBERS - 4):
            raise ValueError(
                "Corpus demasiado grande para un intercambio ZIP (64 MiB o 4092 lotes). Divide el dataset explícitamente."
            )
        job = {
            "id": uuid.uuid4().hex,
            "corpus": self._corpus(context, full_frame),
            "groups": {str(i): keys for i, keys in enumerate(groups.values(), 1)},
            "mode": mode,
            "manual_revision": revision,
            "batches": batches,
            "taxonomy": taxonomy,
            "stage": "classifier" if pending else "designer",
            "instructions_version": INSTRUCTIONS_VERSION,
        }
        if not pending:
            self._save(context, job)
            return self._write(job)
        result = write_numbered_zips(
            self.downloads,
            "comentarios",
            self._manifest(job, "classifier"),
            batches,
            {"taxonomy.json": {"categories": categories}},
            "comments",
        )
        self._save(context, job)
        return {**result, "job_id": job["id"], "stage": "classifier"}

    def _write(self, job: dict[str, Any]) -> dict[str, Any]:
        stage = job["stage"]
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
            archive.writestr("manifest.json", encode(self._manifest(job, stage)))
            archive.writestr("INSTRUCCIONES.txt", PROJECT_INSTRUCTIONS[stage])
            for key, rows in job["batches"].items():
                archive.writestr(f"comments/{key}.json", encode({"comments": rows}))
        content = output.getvalue()
        if len(content) > MAX_ZIP_BYTES:
            raise ValueError("El ZIP de entrada supera 32 MiB.")
        path = persist_download(content, f"nps-lens-{stage}-{job['id']}.zip", self.downloads)
        return {
            "job_id": job["id"],
            "instructions_version": job["instructions_version"],
            "stage": stage,
            "saved_path": str(path),
            "batches": len(job["batches"]),
        }

    def import_response(self, context: UploadContext, content: bytes, stage: str) -> dict[str, Any]:
        manifest, files = read_response(content, stage)
        frame = self._frame(context)
        if stage == "designer":
            if manifest.get("corpus_sha256") != self._corpus(context, frame):
                raise ValueError(
                    "El corpus ha cambiado o el ZIP pertenece a otro dataset. Exporta de nuevo."
                )
            taxonomy = validate_payload(
                TaxonomyDesignResponse, files["taxonomy.json"], "taxonomy.json"
            )
            job = self._load(context, manifest["job_id"])
            if manifest != self._manifest(job, "designer"):
                raise ValueError("El manifiesto del diseñador ha cambiado; exporta de nuevo.")
            comments = frame["Comment"].fillna("").astype(str)
            if comments.str.strip().ne("").any() and not taxonomy.review.quotes:
                raise ValueError("La revisión de taxonomía requiere evidencia del corpus.")
            if any(
                not quote.strip() or not any(quote in text for text in comments)
                for quote in taxonomy.review.quotes
            ):
                raise ValueError("La revisión contiene evidencia ajena al corpus.")
            TaxonomyValidator._validate_taxonomy(taxonomy)
            state = self.taxonomy.state(context)
            state["discovered_taxonomy"] = taxonomy.model_dump(exclude={"review"})
            state["taxonomy_fingerprint"] = taxonomy_fingerprint(state["discovered_taxonomy"])
            state["designer_review"] = taxonomy.review.model_dump()
            state["designer_progress"] = {
                "received": len(frame),
                "corpus": self._corpus(context, frame),
            }
            # A new catalog must not expose assignments from a different catalog.
            previous = self.taxonomy.artifact(state.get("artifacts", {}).get("DISCOVERED", ""))
            if previous and previous.get("taxonomy") != state["discovered_taxonomy"]:
                state["artifacts"].pop("DISCOVERED", None)
            self.taxonomy.save_state(context, state)
            return {"stage": "designer", "imported": True}
        files = {
            name.removeprefix("results/").removesuffix(".json"): value
            for name, value in files.items()
        }
        job = self._load(context, manifest["job_id"])
        mode = job.get("mode")
        if mode not in ("SOURCE", "COMPLETED", "DISCOVERED"):
            raise ValueError("Exporta un nuevo intercambio con la lente LLM seleccionada.")
        if job["taxonomy"] != self.taxonomy.catalog(context, mode) or (
            mode == "COMPLETED"
            and job["manual_revision"]
            != self.taxonomy.state(context).get("artifacts", {}).get(mode, "")
        ):
            raise ValueError("La taxonomía ha cambiado; exporta de nuevo.")
        allowed_batches = validate_manifest(manifest, self._manifest(job, "classifier"))
        if self._corpus(context, frame) != job["corpus"]:
            raise ValueError("El corpus ha cambiado. Exporta un nuevo ZIP; no se importó nada.")
        if job["taxonomy"] is None:
            raise ValueError("Importa primero la taxonomía.")
        categories = category_catalog(job["taxonomy"])
        if not files:
            raise ValueError("No hay resultados de clasificación.")
        validated = {}
        for key, payload in files.items():
            if key not in allowed_batches:
                raise ValueError("Fichero de resultado no esperado en este ZIP.")
            response = validate_payload(CompactCommentResponse, payload, f"results/{key}.json")
            expected = [row["id"] for row in job["batches"][key]]
            if [row.id for row in response.classifications] != expected:
                raise ValueError("Los IDs y el orden deben coincidir exactamente con el lote.")
            for decision in response.classifications:
                decision.expand(categories)
            validated[key] = encode(
                {
                    "classifications": [
                        {
                            "id": row.id,
                            **row.expand(categories),
                        }
                        for row in response.classifications
                    ]
                }
            ).decode()
        retained = self.assignments(context, frame, mode)
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
            merged = retained
            for payload in existing.values():
                for row in strict_json(payload.encode())["classifications"]:
                    value = {key: value for key, value in row.items() if key != "id"}
                    for business_key in job["groups"][row["id"]]:
                        if business_key in merged and merged[business_key] != value:
                            raise ValueError(
                                "Un comentario ya tiene una respuesta diferente para esa taxonomía."
                            )
                        merged[business_key] = value
            self._persist_assignments(db, context, frame, job, merged)
            job["stage"] = "complete" if len(existing) == len(job["batches"]) else "classifier"
            db.execute(
                "UPDATE taxonomy_exchange SET payload=? WHERE id=?",
                (encode(job).decode(), job["id"]),
            )
            db.executemany(
                "INSERT OR IGNORE INTO taxonomy_exchange_batches VALUES (?, ?, ?)",
                [(job["id"], key, payload) for key, payload in validated.items()],
            )
        self._clear_caches()
        return {
            "job_id": job["id"],
            "stage": job["stage"],
            "received": len(existing),
            "batches": len(job["batches"]),
            "pending": [key for key in job["batches"] if key not in existing],
            "progress": {
                "total": len(frame),
                "received": len(merged),
                "pending": len(frame) - len(merged),
            },
        }

    def _persist_assignments(
        self,
        db: Any,
        context: UploadContext,
        frame: Any,
        job: dict[str, Any],
        merged: dict[str, Any],
    ) -> None:
        mode = job["mode"]
        assignments = [
            merged.get(key, {}).get("primary_classification", {"lever": "", "sublever": ""})
            for key in frame["_business_key"]
        ]
        config = {
            "method": "chatgpt_zip",
            "taxonomy_mode": mode,
            "taxonomy_fingerprint": taxonomy_fingerprint(job["taxonomy"]),
            "manual_revision": job["manual_revision"],
            "instructions_version": job["instructions_version"],
        }
        sig = signature(frame, "DISCOVERED", config)
        artifact = {
            "mode": "DISCOVERED",
            "signature": sig,
            "keys": frame["_business_key"].tolist(),
            "config": config,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "lever": [row["lever"] for row in assignments],
            "sublever": [row["sublever"] for row in assignments],
            "provenance": ["chatgpt_zip"] * len(assignments),
            "secondary_classifications": {
                key: row["secondary_classifications"]
                for key, row in merged.items()
                if row["secondary_classifications"]
            },
            "nodes": [],
            "taxonomy_fingerprint": taxonomy_fingerprint(job["taxonomy"]),
            "taxonomy": job["taxonomy"],
            "comment_hashes": {
                key: comment_classification_fingerprint(comment)
                for key, comment in zip(frame["_business_key"], frame["Comment"].fillna(""))
            },
            "equivalences": {},
        }
        state = self.taxonomy.state(context)
        state.setdefault("artifacts" if mode == "DISCOVERED" else "llm_artifacts", {})[mode] = sig
        db.execute(
            "INSERT OR REPLACE INTO taxonomy_artifacts VALUES (?, ?, ?, ?)",
            (sig, context_key(context), "DISCOVERED", encode(artifact).decode()),
        )
        db.execute(
            "INSERT OR REPLACE INTO taxonomy_state VALUES (?, ?)",
            (context_key(context), encode(state).decode()),
        )

    def _clear_caches(self) -> None:
        self.taxonomy._state_cache.clear()
        self.taxonomy._cache.clear()
