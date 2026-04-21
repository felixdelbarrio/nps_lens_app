from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from nps_lens.domain.models import SummarySnapshot, UploadAttempt, UploadContext

CORE_COLUMNS = {
    "ID",
    "Fecha",
    "NPS",
    "NPS Group",
    "Comment",
    "UsuarioDecisión",
    "Canal",
    "Palanca",
    "Subpalanca",
    "Browser",
    "Operating System",
    "service_origin",
    "service_origin_n1",
    "service_origin_n2",
    "_business_key",
    "_record_fingerprint",
    "_source_row_number",
    "_service_origin_n2_key",
    "_text_norm",
}
_SQLITE_IN_BATCH_SIZE = 900


class SqliteNpsRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _init_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS uploads (
                    upload_id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    file_hash TEXT NOT NULL,
                    uploaded_at TEXT NOT NULL,
                    parser_version TEXT NOT NULL,
                    service_origin TEXT NOT NULL,
                    service_origin_n1 TEXT NOT NULL,
                    service_origin_n2 TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_rows INTEGER NOT NULL,
                    normalized_rows INTEGER NOT NULL,
                    inserted_rows INTEGER NOT NULL,
                    updated_rows INTEGER NOT NULL,
                    duplicate_in_file_rows INTEGER NOT NULL,
                    duplicate_historical_rows INTEGER NOT NULL,
                    extra_columns_json TEXT NOT NULL,
                    missing_optional_columns_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_uploads_hash ON uploads (file_hash);
                CREATE INDEX IF NOT EXISTS idx_uploads_context ON uploads (
                    service_origin,
                    service_origin_n1,
                    service_origin_n2,
                    uploaded_at DESC
                );

                CREATE TABLE IF NOT EXISTS upload_issues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    upload_id TEXT NOT NULL REFERENCES uploads(upload_id) ON DELETE CASCADE,
                    level TEXT NOT NULL,
                    code TEXT NOT NULL,
                    message TEXT NOT NULL,
                    column_name TEXT,
                    details_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS records (
                    business_key TEXT PRIMARY KEY,
                    external_id TEXT NOT NULL,
                    response_at TEXT NOT NULL,
                    nps_score REAL NOT NULL,
                    nps_group TEXT NOT NULL,
                    comment_text TEXT NOT NULL,
                    decision_user TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    lever TEXT NOT NULL,
                    sublever TEXT NOT NULL,
                    browser TEXT NOT NULL,
                    operating_system TEXT NOT NULL,
                    service_origin TEXT NOT NULL,
                    service_origin_n1 TEXT NOT NULL,
                    service_origin_n2 TEXT NOT NULL,
                    normalized_text TEXT NOT NULL,
                    record_fingerprint TEXT NOT NULL,
                    extra_payload_json TEXT NOT NULL,
                    first_upload_id TEXT NOT NULL,
                    last_upload_id TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    times_seen INTEGER NOT NULL DEFAULT 1
                );

                CREATE INDEX IF NOT EXISTS idx_records_context ON records (
                    service_origin,
                    service_origin_n1,
                    service_origin_n2
                );
                CREATE INDEX IF NOT EXISTS idx_records_response_at ON records (response_at);

                CREATE TABLE IF NOT EXISTS upload_records (
                    upload_id TEXT NOT NULL REFERENCES uploads(upload_id) ON DELETE CASCADE,
                    business_key TEXT NOT NULL,
                    row_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    record_fingerprint TEXT NOT NULL,
                    PRIMARY KEY (upload_id, row_number, business_key)
                );
                """
            )

    def has_completed_file_hash(self, file_hash: str, context: UploadContext) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM uploads
                WHERE file_hash = ?
                  AND service_origin = ?
                  AND service_origin_n1 = ?
                  AND service_origin_n2 = ?
                  AND status = 'completed'
                LIMIT 1
                """,
                (
                    file_hash,
                    context.service_origin,
                    context.service_origin_n1,
                    context.service_origin_n2,
                ),
            ).fetchone()
        return row is not None

    def persist_upload_attempt(self, attempt: UploadAttempt) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO uploads (
                    upload_id,
                    filename,
                    file_hash,
                    uploaded_at,
                    parser_version,
                    service_origin,
                    service_origin_n1,
                    service_origin_n2,
                    status,
                    total_rows,
                    normalized_rows,
                    inserted_rows,
                    updated_rows,
                    duplicate_in_file_rows,
                    duplicate_historical_rows,
                    extra_columns_json,
                    missing_optional_columns_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt.upload_id,
                    attempt.filename,
                    attempt.file_hash,
                    attempt.uploaded_at,
                    attempt.parser_version,
                    attempt.context.service_origin,
                    attempt.context.service_origin_n1,
                    attempt.context.service_origin_n2,
                    attempt.status,
                    attempt.total_rows,
                    attempt.normalized_rows,
                    attempt.inserted_rows,
                    attempt.updated_rows,
                    attempt.duplicate_in_file_rows,
                    attempt.duplicate_historical_rows,
                    json.dumps(attempt.extra_columns, ensure_ascii=False),
                    json.dumps(attempt.missing_optional_columns, ensure_ascii=False),
                ),
            )
            connection.execute(
                "DELETE FROM upload_issues WHERE upload_id = ?",
                (attempt.upload_id,),
            )
            if attempt.issues:
                connection.executemany(
                    """
                    INSERT INTO upload_issues (
                        upload_id,
                        level,
                        code,
                        message,
                        column_name,
                        details_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            attempt.upload_id,
                            issue.level,
                            issue.code,
                            issue.message,
                            issue.column,
                            json.dumps(issue.details, ensure_ascii=False),
                        )
                        for issue in attempt.issues
                    ],
                )

    def reconcile_processing_uploads(self) -> int:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT upload_id
                FROM uploads
                WHERE status = 'processing'
                """
            ).fetchall()
            if not rows:
                return 0
            upload_ids = [str(row["upload_id"]) for row in rows]
            connection.executemany(
                """
                UPDATE uploads
                SET status = 'failed'
                WHERE upload_id = ?
                """,
                [(upload_id,) for upload_id in upload_ids],
            )
            connection.executemany(
                """
                INSERT INTO upload_issues (
                    upload_id,
                    level,
                    code,
                    message,
                    column_name,
                    details_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        upload_id,
                        "ERROR",
                        "interrupted_upload",
                        "La carga anterior quedó interrumpida antes de completarse. Se marcó como fallida al reabrir la aplicación.",
                        None,
                        json.dumps({}, ensure_ascii=False),
                    )
                    for upload_id in upload_ids
                ],
            )
        return len(upload_ids)

    def upsert_records(
        self,
        *,
        upload_id: str,
        uploaded_at: str,
        frame: pd.DataFrame,
    ) -> tuple[int, int, int]:
        rows = frame.to_dict(orient="records")
        if not rows:
            return 0, 0, 0

        business_keys = [str(row["_business_key"]) for row in rows]
        inserted = 0
        updated = 0
        duplicate_historical = 0

        with self._connect() as connection:
            existing: dict[str, dict[str, Any]] = {}
            for start in range(0, len(business_keys), _SQLITE_IN_BATCH_SIZE):
                chunk = business_keys[start : start + _SQLITE_IN_BATCH_SIZE]
                placeholders = ",".join("?" for _ in chunk)
                existing_rows = connection.execute(
                    f"""
                    SELECT business_key, record_fingerprint, times_seen
                    FROM records
                    WHERE business_key IN ({placeholders})
                    """,
                    chunk,
                ).fetchall()
                existing.update(
                    {
                        str(row["business_key"]): {
                            "record_fingerprint": str(row["record_fingerprint"]),
                            "times_seen": int(row["times_seen"]),
                        }
                        for row in existing_rows
                    }
                )

            upload_events: list[tuple[str, str, int, str, str]] = []
            for row in rows:
                business_key = str(row["_business_key"])
                record_fingerprint = str(row["_record_fingerprint"])
                row_number = int(row["_source_row_number"])
                extra_payload = {key: row.get(key, "") for key in row if key not in CORE_COLUMNS}
                payload = (
                    business_key,
                    str(row.get("ID", "")),
                    pd.Timestamp(row["Fecha"]).isoformat(),
                    float(row["NPS"]),
                    str(row.get("NPS Group", "")),
                    str(row.get("Comment", "")),
                    str(row.get("UsuarioDecisión", "")),
                    str(row.get("Canal", "")),
                    str(row.get("Palanca", "")),
                    str(row.get("Subpalanca", "")),
                    str(row.get("Browser", "")),
                    str(row.get("Operating System", "")),
                    str(row.get("service_origin", "")),
                    str(row.get("service_origin_n1", "")),
                    str(row.get("service_origin_n2", "")),
                    str(row.get("_text_norm", "")),
                    record_fingerprint,
                    json.dumps(extra_payload, ensure_ascii=False),
                    upload_id,
                    upload_id,
                    uploaded_at,
                    uploaded_at,
                    1,
                )

                if business_key not in existing:
                    connection.execute(
                        """
                        INSERT INTO records (
                            business_key,
                            external_id,
                            response_at,
                            nps_score,
                            nps_group,
                            comment_text,
                            decision_user,
                            channel,
                            lever,
                            sublever,
                            browser,
                            operating_system,
                            service_origin,
                            service_origin_n1,
                            service_origin_n2,
                            normalized_text,
                            record_fingerprint,
                            extra_payload_json,
                            first_upload_id,
                            last_upload_id,
                            first_seen_at,
                            last_seen_at,
                            times_seen
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        payload,
                    )
                    existing[business_key] = {
                        "record_fingerprint": record_fingerprint,
                        "times_seen": 1,
                    }
                    inserted += 1
                    upload_events.append(
                        (upload_id, business_key, row_number, "inserted", record_fingerprint)
                    )
                    continue

                if existing[business_key]["record_fingerprint"] == record_fingerprint:
                    connection.execute(
                        """
                        UPDATE records
                        SET last_upload_id = ?,
                            last_seen_at = ?,
                            times_seen = times_seen + 1
                        WHERE business_key = ?
                        """,
                        (upload_id, uploaded_at, business_key),
                    )
                    existing[business_key]["times_seen"] = (
                        int(existing[business_key]["times_seen"]) + 1
                    )
                    duplicate_historical += 1
                    upload_events.append(
                        (
                            upload_id,
                            business_key,
                            row_number,
                            "duplicate_historical",
                            record_fingerprint,
                        )
                    )
                    continue

                connection.execute(
                    """
                    UPDATE records
                    SET external_id = ?,
                        response_at = ?,
                        nps_score = ?,
                        nps_group = ?,
                        comment_text = ?,
                        decision_user = ?,
                        channel = ?,
                        lever = ?,
                        sublever = ?,
                        browser = ?,
                        operating_system = ?,
                        service_origin = ?,
                        service_origin_n1 = ?,
                        service_origin_n2 = ?,
                        normalized_text = ?,
                        record_fingerprint = ?,
                        extra_payload_json = ?,
                        last_upload_id = ?,
                        last_seen_at = ?,
                        times_seen = times_seen + 1
                    WHERE business_key = ?
                    """,
                    (
                        payload[1],
                        payload[2],
                        payload[3],
                        payload[4],
                        payload[5],
                        payload[6],
                        payload[7],
                        payload[8],
                        payload[9],
                        payload[10],
                        payload[11],
                        payload[12],
                        payload[13],
                        payload[14],
                        payload[15],
                        payload[16],
                        payload[17],
                        upload_id,
                        uploaded_at,
                        business_key,
                    ),
                )
                existing[business_key]["record_fingerprint"] = record_fingerprint
                existing[business_key]["times_seen"] = int(existing[business_key]["times_seen"]) + 1
                updated += 1
                upload_events.append(
                    (upload_id, business_key, row_number, "updated", record_fingerprint)
                )

            connection.executemany(
                """
                INSERT OR REPLACE INTO upload_records (
                    upload_id,
                    business_key,
                    row_number,
                    status,
                    record_fingerprint
                ) VALUES (?, ?, ?, ?, ?)
                """,
                upload_events,
            )

        return inserted, updated, duplicate_historical

    def list_uploads(
        self,
        limit: int = 50,
        context: Optional[UploadContext] = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT *
            FROM uploads
        """
        params: list[Any] = []
        if context is not None:
            query += """
                WHERE service_origin = ?
                  AND service_origin_n1 = ?
                  AND service_origin_n2 = ?
            """
            params.extend(
                [
                    context.service_origin,
                    context.service_origin_n1,
                    context.service_origin_n2,
                ]
            )
        query += """
            ORDER BY uploaded_at DESC
            LIMIT ?
        """
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._serialize_upload_row(row) for row in rows]

    def get_upload_issues(self, upload_id: str) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT level, code, message, column_name, details_json
                FROM upload_issues
                WHERE upload_id = ?
                ORDER BY id ASC
                """,
                (upload_id,),
            ).fetchall()
        return [
            {
                "level": str(row["level"]),
                "code": str(row["code"]),
                "message": str(row["message"]),
                "column": row["column_name"],
                "details": json.loads(str(row["details_json"]) or "{}"),
            }
            for row in rows
        ]

    def load_records_df(self, context: Optional[UploadContext] = None) -> pd.DataFrame:
        query = """
            SELECT
                external_id AS ID,
                response_at AS Fecha,
                nps_score AS NPS,
                nps_group AS "NPS Group",
                comment_text AS Comment,
                decision_user AS "UsuarioDecisión",
                channel AS Canal,
                lever AS Palanca,
                sublever AS Subpalanca,
                browser AS Browser,
                operating_system AS "Operating System",
                service_origin,
                service_origin_n1,
                service_origin_n2,
                normalized_text AS _text_norm,
                business_key AS _business_key,
                record_fingerprint AS _record_fingerprint
            FROM records
        """
        params: list[Any] = []
        if context is not None:
            query += """
                WHERE service_origin = ?
                  AND service_origin_n1 = ?
                  AND service_origin_n2 = ?
            """
            params.extend(
                [
                    context.service_origin,
                    context.service_origin_n1,
                    context.service_origin_n2,
                ]
            )
        query += " ORDER BY response_at ASC"

        with self._connect() as connection:
            frame = pd.read_sql_query(query, connection, params=params)
        if frame.empty:
            return frame
        frame["Fecha"] = pd.to_datetime(frame["Fecha"], errors="coerce")
        return frame

    def build_summary(self, context: Optional[UploadContext] = None) -> SummarySnapshot:
        records = self.load_records_df(context)
        uploads = self.list_uploads(limit=10, context=context)
        with self._connect() as connection:
            query = """
                SELECT
                    COALESCE(SUM(duplicate_in_file_rows + duplicate_historical_rows), 0) AS duplicates
                FROM uploads
            """
            params: list[Any] = []
            if context is not None:
                query += """
                    WHERE service_origin = ?
                      AND service_origin_n1 = ?
                      AND service_origin_n2 = ?
                """
                params.extend(
                    [
                        context.service_origin,
                        context.service_origin_n1,
                        context.service_origin_n2,
                    ]
                )
            duplicates_prevented_row = connection.execute(query, params).fetchone()
        duplicates_prevented = (
            int(duplicates_prevented_row["duplicates"]) if duplicates_prevented_row else 0
        )

        if records.empty:
            return SummarySnapshot(
                total_records=0,
                date_range={"min": None, "max": None},
                overall_nps=None,
                promoter_rate=None,
                detractor_rate=None,
                uploads=len(uploads),
                duplicates_prevented=duplicates_prevented,
                top_drivers={},
                latest_uploads=uploads,
            )

        from nps_lens.analytics.drivers import driver_table

        scores = pd.to_numeric(records["NPS"], errors="coerce").dropna()
        promoter_rate = float((scores >= 9).mean()) if not scores.empty else None
        detractor_rate = float((scores <= 6).mean()) if not scores.empty else None
        overall_nps = None
        if promoter_rate is not None and detractor_rate is not None:
            overall_nps = float((promoter_rate - detractor_rate) * 100.0)

        top_drivers: dict[str, list[dict[str, Any]]] = {}
        for dimension in ["Palanca", "Subpalanca", "Canal"]:
            top_drivers[dimension] = [
                stat.__dict__ for stat in driver_table(records, dimension=dimension)[:5]
            ]

        return SummarySnapshot(
            total_records=int(len(records)),
            date_range={
                "min": (
                    records["Fecha"].min().isoformat() if records["Fecha"].notna().any() else None
                ),
                "max": (
                    records["Fecha"].max().isoformat() if records["Fecha"].notna().any() else None
                ),
            },
            overall_nps=overall_nps,
            promoter_rate=promoter_rate,
            detractor_rate=detractor_rate,
            uploads=len(uploads),
            duplicates_prevented=duplicates_prevented,
            top_drivers=top_drivers,
            latest_uploads=uploads,
        )

    def _serialize_upload_row(self, row: sqlite3.Row) -> dict[str, Any]:
        issues = self.get_upload_issues(str(row["upload_id"]))
        return {
            "upload_id": str(row["upload_id"]),
            "filename": str(row["filename"]),
            "file_hash": str(row["file_hash"]),
            "uploaded_at": str(row["uploaded_at"]),
            "parser_version": str(row["parser_version"]),
            "status": str(row["status"]),
            "service_origin": str(row["service_origin"]),
            "service_origin_n1": str(row["service_origin_n1"]),
            "service_origin_n2": str(row["service_origin_n2"]),
            "total_rows": int(row["total_rows"]),
            "normalized_rows": int(row["normalized_rows"]),
            "inserted_rows": int(row["inserted_rows"]),
            "updated_rows": int(row["updated_rows"]),
            "duplicate_in_file_rows": int(row["duplicate_in_file_rows"]),
            "duplicate_historical_rows": int(row["duplicate_historical_rows"]),
            "extra_columns": json.loads(str(row["extra_columns_json"])),
            "missing_optional_columns": json.loads(str(row["missing_optional_columns_json"])),
            "issues": issues,
        }
