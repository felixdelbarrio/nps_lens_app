"""Owner-scoped, bounded exchange for evidence-based concept normalization."""

from __future__ import annotations

import io
import uuid
import zipfile
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

from nps_lens.domain.models import UploadContext
from nps_lens.domain.normalization import CATEGORICAL_DIMENSIONS, EquivalenceRegistry, clean_label
from nps_lens.platform.downloads import persist_download
from nps_lens.services.taxonomy_exchange import (
    BATCH_ROWS,
    MAX_EXPANDED_BYTES,
    MAX_MEMBER_BYTES,
    MAX_MEMBERS,
    MAX_ZIP_BYTES,
    TaxonomyExchange,
    digest,
    encode,
    read_response,
    validate_payload,
)
from nps_lens.services.taxonomy_prompts import PROJECT_INSTRUCTIONS


class ConceptGroup(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    canonical: str = Field(min_length=1, max_length=300)
    aliases: list[str] = Field(max_length=1000)

    @field_validator("canonical")
    @classmethod
    def meaningful_name(cls, value: str) -> str:
        if not clean_label(value):
            raise ValueError("El nombre principal no puede estar vacío.")
        return clean_label(value)


class ConceptResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    dimensions: dict[str, list[ConceptGroup]]


class EquivalenceExchange(TaxonomyExchange):
    def inputs(self, context: UploadContext, incidents: pd.DataFrame) -> dict[str, Any]:
        frame = self._frame(context)
        catalogs = {
            mode: self.taxonomy.manual_draft(context, mode) for mode in ("SOURCE", "CURRENT")
        }
        vocabulary: dict[str, list[str]] = {}
        for dimension in sorted(CATEGORICAL_DIMENSIONS):
            domain, column = dimension.split(".", 1)
            source_column = {
                "Canal": "source_channel",
                "Palanca": "source_lever",
                "Subpalanca": "source_sublever",
            }.get(column, column)
            values = (frame if domain == "nps" else incidents).get(
                source_column if domain == "nps" else column,
                (frame if domain == "nps" else incidents).get(column, pd.Series(dtype=str)),
            )
            vocabulary[dimension] = sorted(set(values.fillna("").astype(str)) - {""})
        for catalog in catalogs.values():
            for branch in catalog["taxonomy"]:
                vocabulary["nps.Palanca"].append(branch["lever"])
                vocabulary["nps.Subpalanca"].extend(branch["sublevers"])
        return {
            "owner": context.service_origin,
            "comments": [
                {"id": str(i + 1), "Comment": str(comment)}
                for i, comment in enumerate(frame["Comment"].fillna(""))
            ],
            "vocabulary": {key: sorted(set(values)) for key, values in vocabulary.items()},
            "equivalences": self.taxonomy.registry(context).to_dict(),
        }

    def export_concepts(self, context: UploadContext, incidents: pd.DataFrame) -> dict[str, Any]:
        inputs = self.inputs(context, incidents)
        if not inputs["comments"]:
            raise ValueError("No hay comentarios que exportar para esta compañía.")
        manifest = {
            "schema_version": "nps-lens-normalization/1",
            "stage": "normalizer",
            "job_id": uuid.uuid4().hex,
            "owner": context.service_origin,
            "corpus_sha256": digest(inputs),
        }
        files = {
            "manifest.json": manifest,
            "concepts.json": {key: value for key, value in inputs.items() if key != "comments"},
        }
        for start in range(0, len(inputs["comments"]), BATCH_ROWS):
            files[f"comments/{start // BATCH_ROWS + 1:06d}.json"] = {
                "comments": inputs["comments"][start : start + BATCH_ROWS]
            }
        encoded = {name: encode(value) for name, value in files.items()}
        if (
            len(encoded) >= MAX_MEMBERS
            or sum(map(len, encoded.values())) > MAX_EXPANDED_BYTES
            or any(len(raw) > MAX_MEMBER_BYTES for raw in encoded.values())
        ):
            raise ValueError("El corpus supera los límites del intercambio ZIP.")
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for name, raw in encoded.items():
                archive.writestr(name, raw)
            archive.writestr("INSTRUCCIONES.txt", PROJECT_INSTRUCTIONS["normalizer"])
        if len(buffer.getvalue()) > MAX_ZIP_BYTES:
            raise ValueError("ZIP demasiado grande.")
        path = persist_download(
            buffer.getvalue(), f"nps-lens-normalizer-{manifest['job_id']}.zip", self.downloads
        )
        self._save(context, {"id": manifest["job_id"], "manifest": manifest})
        return {"saved_path": str(path), "total": len(inputs["comments"])}

    def import_concepts(
        self, context: UploadContext, incidents: pd.DataFrame, content: bytes
    ) -> dict[str, Any]:
        manifest, files = read_response(content, "normalizer")
        job = self._load(context, manifest["job_id"])
        inputs = self.inputs(context, incidents)
        if manifest != job["manifest"] or manifest["corpus_sha256"] != digest(inputs):
            raise ValueError(
                "La compañía, los comentarios o los conceptos han cambiado. Exporta de nuevo."
            )
        response = validate_payload(
            ConceptResponse, files["equivalences.json"], "equivalences.json"
        )
        dimensions = inputs["equivalences"]["dimensions"]
        for dimension, groups in response.dimensions.items():
            if dimension not in CATEGORICAL_DIMENSIONS:
                raise ValueError("Dimensión desconocida.")
            known = set(inputs["vocabulary"][dimension])
            for group in dimensions.get(dimension, []):
                known.update([group["canonical"], *group["aliases"]])
            if any(alias not in known for group in groups for alias in group.aliases):
                raise ValueError("La respuesta contiene alias ajenos al catálogo de esta compañía.")
            dimensions[dimension] = [group.model_dump() for group in groups]
        registry = EquivalenceRegistry.from_dict({"dimensions": dimensions})
        self.taxonomy.save_equivalences(context, registry)
        return {
            "owner": context.service_origin,
            "groups": sum(len(groups) for groups in dimensions.values()),
        }
