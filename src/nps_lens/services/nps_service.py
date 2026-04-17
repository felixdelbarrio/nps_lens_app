from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

from nps_lens.domain.models import UploadAttempt, UploadContext
from nps_lens.ingest.base import ValidationIssue
from nps_lens.ingest.nps_thermal import PARSER_VERSION, read_nps_thermal_excel
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.settings import Settings

_FILENAME_SANITIZER_RE = re.compile(r"[^A-Za-z0-9._-]+")


class NpsService:
    def __init__(self, repository: SqliteNpsRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self.logger = logging.getLogger(__name__)

    def ingest_excel(
        self,
        *,
        filename: str,
        payload: bytes,
        context: UploadContext,
    ) -> dict[str, object]:
        upload_id = uuid4().hex
        uploaded_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        file_hash = hashlib.sha256(payload).hexdigest()
        stored_path = self._persist_upload_file(
            upload_id=upload_id, filename=filename, payload=payload
        )

        if self.repository.has_completed_file_hash(file_hash, context):
            issue = ValidationIssue(
                level="WARN",
                code="duplicate_upload",
                message="El fichero ya había sido cargado previamente para este contexto. Se registra el intento, pero no se reinyestan registros.",
                details={"file_hash": file_hash},
            )
            attempt = UploadAttempt(
                upload_id=upload_id,
                filename=Path(filename).name,
                file_hash=file_hash,
                uploaded_at=uploaded_at,
                parser_version=PARSER_VERSION,
                context=context,
                status="duplicate_upload",
                total_rows=0,
                normalized_rows=0,
                inserted_rows=0,
                updated_rows=0,
                duplicate_in_file_rows=0,
                duplicate_historical_rows=0,
                issues=[issue],
            )
            self.repository.persist_upload_attempt(attempt)
            return self._serialize_attempt(attempt)

        result = read_nps_thermal_excel(
            str(stored_path),
            service_origin=context.service_origin,
            service_origin_n1=context.service_origin_n1,
            service_origin_n2=context.service_origin_n2,
        )
        raw_rows = self._meta_int(result.meta, "raw_rows", len(result.df))
        normalized_rows = self._meta_int(result.meta, "normalized_rows", len(result.df))
        duplicate_in_file_rows = self._meta_int(result.meta, "duplicate_rows_in_file", 0)
        extra_columns = self._meta_str_list(result.meta, "extra_columns")
        missing_optional_columns = self._meta_str_list(result.meta, "missing_optional_columns")

        if any(issue.level == "ERROR" for issue in result.issues):
            attempt = UploadAttempt(
                upload_id=upload_id,
                filename=Path(filename).name,
                file_hash=file_hash,
                uploaded_at=uploaded_at,
                parser_version=PARSER_VERSION,
                context=context,
                status="failed",
                total_rows=raw_rows,
                normalized_rows=normalized_rows,
                inserted_rows=0,
                updated_rows=0,
                duplicate_in_file_rows=duplicate_in_file_rows,
                duplicate_historical_rows=0,
                extra_columns=extra_columns,
                missing_optional_columns=missing_optional_columns,
                issues=result.issues,
            )
            self.repository.persist_upload_attempt(attempt)
            self.logger.warning(
                "Upload failed during parsing",
                extra={
                    "upload_id": upload_id,
                    "upload_filename": filename,
                    "file_hash": file_hash,
                },
            )
            return self._serialize_attempt(attempt)

        processing_attempt = UploadAttempt(
            upload_id=upload_id,
            filename=Path(filename).name,
            file_hash=file_hash,
            uploaded_at=uploaded_at,
            parser_version=PARSER_VERSION,
            context=context,
            status="processing",
            total_rows=raw_rows,
            normalized_rows=normalized_rows,
            inserted_rows=0,
            updated_rows=0,
            duplicate_in_file_rows=duplicate_in_file_rows,
            duplicate_historical_rows=0,
            extra_columns=extra_columns,
            missing_optional_columns=missing_optional_columns,
            issues=result.issues,
        )
        self.repository.persist_upload_attempt(processing_attempt)

        inserted_rows, updated_rows, duplicate_historical_rows = self.repository.upsert_records(
            upload_id=upload_id,
            uploaded_at=uploaded_at,
            frame=result.df,
        )

        if updated_rows:
            result.issues.append(
                ValidationIssue(
                    level="WARN",
                    code="historical_records_updated",
                    message=f"Se actualizaron {updated_rows} registros históricos porque la misma clave de negocio reapareció con payload distinto.",
                    details={"rows": updated_rows},
                )
            )

        attempt = UploadAttempt(
            upload_id=upload_id,
            filename=Path(filename).name,
            file_hash=file_hash,
            uploaded_at=uploaded_at,
            parser_version=PARSER_VERSION,
            context=context,
            status="completed",
            total_rows=raw_rows,
            normalized_rows=normalized_rows,
            inserted_rows=inserted_rows,
            updated_rows=updated_rows,
            duplicate_in_file_rows=duplicate_in_file_rows,
            duplicate_historical_rows=duplicate_historical_rows,
            extra_columns=extra_columns,
            missing_optional_columns=missing_optional_columns,
            issues=result.issues,
        )
        self.repository.persist_upload_attempt(attempt)
        self.logger.info(
            "Upload processed",
            extra={
                "upload_id": upload_id,
                "upload_filename": filename,
                "file_hash": file_hash,
                "inserted_rows": inserted_rows,
                "updated_rows": updated_rows,
                "duplicate_historical_rows": duplicate_historical_rows,
                "duplicate_in_file_rows": duplicate_in_file_rows,
            },
        )
        return self._serialize_attempt(attempt)

    def list_uploads(self) -> list[dict[str, object]]:
        return self.repository.list_uploads()

    def summary(self, context: Optional[UploadContext] = None) -> dict[str, object]:
        snapshot = self.repository.build_summary(context)
        return {
            "total_records": snapshot.total_records,
            "date_range": snapshot.date_range,
            "overall_nps": snapshot.overall_nps,
            "promoter_rate": snapshot.promoter_rate,
            "detractor_rate": snapshot.detractor_rate,
            "uploads": snapshot.uploads,
            "duplicates_prevented": snapshot.duplicates_prevented,
            "top_drivers": snapshot.top_drivers,
            "latest_uploads": snapshot.latest_uploads,
        }

    def context_options(self) -> dict[str, object]:
        return {
            "default_service_origin": self.settings.default_service_origin,
            "default_service_origin_n1": self.settings.default_service_origin_n1,
            "service_origins": self.settings.allowed_service_origins,
            "service_origin_n1_map": self.settings.allowed_service_origin_n1,
        }

    def _persist_upload_file(self, *, upload_id: str, filename: str, payload: bytes) -> Path:
        uploads_dir = self.settings.data_dir / "uploads"
        uploads_dir.mkdir(parents=True, exist_ok=True)
        safe_name = _FILENAME_SANITIZER_RE.sub("_", Path(filename).name)
        path = uploads_dir / f"{upload_id}__{safe_name}"
        path.write_bytes(payload)
        return path

    def _serialize_attempt(self, attempt: UploadAttempt) -> dict[str, object]:
        return {
            "upload_id": attempt.upload_id,
            "filename": attempt.filename,
            "file_hash": attempt.file_hash,
            "uploaded_at": attempt.uploaded_at,
            "parser_version": attempt.parser_version,
            "status": attempt.status,
            "service_origin": attempt.context.service_origin,
            "service_origin_n1": attempt.context.service_origin_n1,
            "service_origin_n2": attempt.context.service_origin_n2,
            "total_rows": attempt.total_rows,
            "normalized_rows": attempt.normalized_rows,
            "inserted_rows": attempt.inserted_rows,
            "updated_rows": attempt.updated_rows,
            "duplicate_in_file_rows": attempt.duplicate_in_file_rows,
            "duplicate_historical_rows": attempt.duplicate_historical_rows,
            "extra_columns": attempt.extra_columns,
            "missing_optional_columns": attempt.missing_optional_columns,
            "issues": [issue.to_dict() for issue in attempt.issues],
        }

    @staticmethod
    def _meta_int(meta: dict[str, object], key: str, default: int) -> int:
        value = meta.get(key, default)
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float, str)):
            return int(value)
        return default

    @staticmethod
    def _meta_str_list(meta: dict[str, object], key: str) -> list[str]:
        value = meta.get(key, [])
        if not isinstance(value, list):
            return []
        return [str(item) for item in value]
