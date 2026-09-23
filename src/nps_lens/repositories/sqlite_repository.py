from __future__ import annotations

import json
import sqlite3
from contextlib import nullcontext
from pathlib import Path
from typing import Any, ContextManager, Optional, cast

import pandas as pd

from nps_lens.core.nps_math import classify_nps_scores
from nps_lens.domain.models import SummarySnapshot, UploadAttempt, UploadContext
from nps_lens.domain.record_identity import business_keys

CORE_COLUMNS = {
    "source_channel",
    "source_lever",
    "source_sublever",
    "source_preserved",
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
    "_palanca_key",
    "_subpalanca_key",
    "_canal_key",
}
_SQLITE_IN_BATCH_SIZE = 900


class SqliteNpsRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if self.db_path.exists():
            with sqlite3.connect(self.db_path) as existing:
                columns = {row[1] for row in existing.execute("PRAGMA table_info(records)")}
                backup = self.db_path.with_suffix(".before-taxonomy.sqlite3")
                if "channel" in columns and not backup.exists():
                    with sqlite3.connect(backup) as target:
                        existing.backup(target)
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
                    source_channel TEXT NOT NULL,
                    source_lever TEXT NOT NULL,
                    source_sublever TEXT NOT NULL,
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
                CREATE INDEX IF NOT EXISTS idx_records_context_response_at ON records (
                    service_origin,
                    service_origin_n1,
                    service_origin_n2,
                    response_at
                );

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

        with self._connect() as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(records)")}
            if "source_preserved" not in columns:
                connection.execute(
                    "ALTER TABLE records ADD COLUMN source_preserved INTEGER NOT NULL DEFAULT "
                    + ("0" if "channel" in columns else "1")
                )
            for old, new in [
                ("channel", "source_channel"),
                ("lever", "source_lever"),
                ("sublever", "source_sublever"),
            ]:
                if old in columns:
                    connection.execute(f"ALTER TABLE records RENAME COLUMN {old} TO {new}")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS taxonomy_artifacts (signature TEXT PRIMARY KEY, context TEXT NOT NULL, mode TEXT NOT NULL, payload TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS taxonomy_state (context TEXT PRIMARY KEY, payload TEXT NOT NULL)"
            )

    def migrate_source_identity(
        self, uploads_dir: Path, column_aliases_path: Optional[Path] = None
    ) -> None:
        """Recover originals from retained uploads, then migrate keys once in a transaction."""
        with self._connect() as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version >= 2:
                return
            old = pd.read_sql_query(
                "SELECT business_key, last_upload_id, external_id AS ID, response_at AS Fecha, "
                'nps_score AS NPS, comment_text AS Comment, decision_user AS "UsuarioDecisión", '
                "service_origin, service_origin_n1, service_origin_n2 "
                "FROM records WHERE source_preserved = 0",
                connection,
            )
            if not old.empty:
                from nps_lens.ingest.nps_thermal import read_nps_thermal_excel

                for upload_id, group in old.groupby("last_upload_id", sort=False):
                    paths = list(uploads_dir.glob(str(upload_id) + "__*"))
                    if not paths:
                        continue
                    from nps_lens.analytics.text_mining import preprocess_text

                    first = group.iloc[0]
                    parsed = read_nps_thermal_excel(
                        str(paths[0]),
                        first["service_origin"],
                        first["service_origin_n1"],
                        first["service_origin_n2"],
                        column_aliases_path=column_aliases_path,
                    ).df
                    if parsed.empty:
                        continue
                    group = group.assign(Fecha=pd.to_datetime(group["Fecha"]))
                    group["_recovery_key"] = business_keys(
                        group.assign(Comment=group["Comment"].map(preprocess_text))
                    )
                    parsed["_recovery_key"] = business_keys(
                        parsed.assign(Comment=parsed["Comment"].map(preprocess_text))
                    )
                    joined = group[["business_key", "_recovery_key"]].merge(
                        parsed.drop_duplicates("_recovery_key", keep="last"), on="_recovery_key"
                    )
                    connection.executemany(
                        "UPDATE records SET source_channel = ?, source_lever = ?, source_sublever = ?, comment_text = ?, source_preserved = 1 WHERE business_key = ?",
                        list(
                            joined[
                                [
                                    "source_channel",
                                    "source_lever",
                                    "source_sublever",
                                    "Comment",
                                    "business_key",
                                ]
                            ].itertuples(index=False, name=None)
                        ),
                    )
            frame = pd.read_sql_query(
                'SELECT business_key, external_id AS ID, response_at AS Fecha, nps_score AS NPS, comment_text AS Comment, decision_user AS "UsuarioDecisión", '
                "service_origin, service_origin_n1, service_origin_n2 FROM records ORDER BY last_seen_at",
                connection,
            )
            if not frame.empty:
                frame["Fecha"] = pd.to_datetime(frame["Fecha"])
                frame["new_key"] = business_keys(frame)
                connection.execute(
                    "CREATE TEMP TABLE key_migration (old_key TEXT PRIMARY KEY, new_key TEXT)"
                )
                connection.executemany(
                    "INSERT INTO key_migration VALUES (?, ?)",
                    list(frame[["business_key", "new_key"]].itertuples(index=False, name=None)),
                )
                # Keep the latest observation when old taxonomy-dependent identities collapse.
                redundant = frame.loc[frame.duplicated("new_key", keep="last"), "business_key"]
                connection.executemany(
                    "DELETE FROM records WHERE business_key = ?", [(key,) for key in redundant]
                )
                connection.execute("UPDATE records SET business_key = 'migrating:' || business_key")
                connection.execute(
                    "UPDATE records SET business_key = (SELECT new_key FROM key_migration WHERE old_key = substr(records.business_key, 11))"
                )
                connection.execute(
                    "UPDATE OR REPLACE upload_records SET business_key = COALESCE((SELECT new_key FROM key_migration WHERE old_key = upload_records.business_key), business_key)"
                )
            connection.execute("PRAGMA user_version = 2")

    def find_completed_upload(
        self, file_hash: str, context: UploadContext
    ) -> Optional[dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM uploads
                WHERE file_hash = ?
                  AND service_origin = ?
                  AND service_origin_n1 = ?
                  AND service_origin_n2 = ?
                  AND status = 'completed'
                ORDER BY uploaded_at DESC
                LIMIT 1
                """,
                (
                    file_hash,
                    context.service_origin,
                    context.service_origin_n1,
                    context.service_origin_n2,
                ),
            ).fetchone()
        return self._serialize_upload_row(row) if row is not None else None

    def get_upload(self, upload_id: str) -> Optional[dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
        return self._serialize_upload_row(row) if row is not None else None

    def persist_upload_attempt(self, attempt: UploadAttempt) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO uploads (
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
                ON CONFLICT(upload_id) DO UPDATE SET
                filename = excluded.filename,
                file_hash = excluded.file_hash,
                uploaded_at = excluded.uploaded_at,
                parser_version = excluded.parser_version,
                service_origin = excluded.service_origin,
                service_origin_n1 = excluded.service_origin_n1,
                service_origin_n2 = excluded.service_origin_n2,
                status = excluded.status,
                total_rows = excluded.total_rows,
                normalized_rows = excluded.normalized_rows,
                inserted_rows = excluded.inserted_rows,
                updated_rows = excluded.updated_rows,
                duplicate_in_file_rows = excluded.duplicate_in_file_rows,
                duplicate_historical_rows = excluded.duplicate_historical_rows,
                extra_columns_json = excluded.extra_columns_json,
                missing_optional_columns_json = excluded.missing_optional_columns_json
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
        _connection: Optional[sqlite3.Connection] = None,
    ) -> tuple[int, int, int]:
        rows = frame.to_dict(orient="records")
        if not rows:
            return 0, 0, 0

        business_keys = [str(row["_business_key"]) for row in rows]
        inserted = 0
        updated = 0
        duplicate_historical = 0

        connection_context = cast(
            ContextManager[sqlite3.Connection],
            self._connect() if _connection is None else nullcontext(_connection),
        )
        with connection_context as connection:
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

            inserts: list[tuple[Any, ...]] = []
            duplicates: list[tuple[Any, ...]] = []
            updates: list[tuple[Any, ...]] = []
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
                    str(row.get("source_channel", row.get("Canal", ""))),
                    str(row.get("source_lever", row.get("Palanca", ""))),
                    str(row.get("source_sublever", row.get("Subpalanca", ""))),
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
                    inserts.append(payload)
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
                    duplicates.append((upload_id, uploaded_at, business_key))
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

                updates.append(
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
                    )
                )
                existing[business_key]["record_fingerprint"] = record_fingerprint
                existing[business_key]["times_seen"] = int(existing[business_key]["times_seen"]) + 1
                updated += 1
                upload_events.append(
                    (upload_id, business_key, row_number, "updated", record_fingerprint)
                )

            connection.executemany(
                """
                        INSERT INTO records (
                            business_key,
                            external_id,
                            response_at,
                            nps_score,
                            nps_group,
                            comment_text,
                            decision_user,
                            source_channel,
                            source_lever,
                            source_sublever,
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
                            times_seen, source_preserved
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                        """,
                inserts,
            )
            connection.executemany(
                """
                        UPDATE records
                        SET last_upload_id = ?,
                            last_seen_at = ?,
                            times_seen = times_seen + 1
                        WHERE business_key = ?
                        """,
                duplicates,
            )
            connection.executemany(
                """
                    UPDATE records
                    SET source_preserved = 1, external_id = ?,
                        response_at = ?,
                        nps_score = ?,
                        nps_group = ?,
                        comment_text = ?,
                        decision_user = ?,
                        source_channel = ?,
                        source_lever = ?,
                        source_sublever = ?,
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
                updates,
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

    def replace_upload_records(
        self,
        *,
        replaced_upload_id: str,
        replacement_upload_id: str,
        uploaded_at: str,
        frame: pd.DataFrame,
    ) -> tuple[int, int, int, int]:
        """Replace one upload atomically without deleting records superseded elsewhere."""
        with self._connect() as connection:
            deleted_rows = connection.execute(
                """
                DELETE FROM records
                WHERE last_upload_id = ?
                  AND business_key IN (
                      SELECT business_key
                      FROM upload_records
                      WHERE upload_id = ? AND status = 'inserted'
                  )
                """,
                (replaced_upload_id, replaced_upload_id),
            ).rowcount
            connection.execute(
                "DELETE FROM upload_records WHERE upload_id = ?", (replacement_upload_id,)
            )
            inserted, updated, duplicate_historical = self.upsert_records(
                upload_id=replacement_upload_id,
                uploaded_at=uploaded_at,
                frame=frame,
                _connection=connection,
            )
            connection.execute(
                "UPDATE uploads SET status = 'replaced' WHERE upload_id = ?",
                (replaced_upload_id,),
            )
        return inserted, updated, duplicate_historical, max(0, deleted_rows)

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
            """
            params.append(context.service_origin)
        query += """
            ORDER BY uploaded_at DESC
            LIMIT ?
        """
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._serialize_upload_row(row) for row in rows]

    def delete_upload(self, upload_id: str, owner_support_company: str) -> dict[str, int]:
        """Delete one NPS ingestion and the records whose current version came from it."""
        with self._connect() as connection:
            upload = connection.execute(
                "SELECT service_origin FROM uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
            if upload is None or str(upload["service_origin"]) != owner_support_company:
                raise ValueError("La ingesta NPS no existe para el Owner Support Company activo.")
            removed_records = connection.execute(
                "DELETE FROM records WHERE service_origin = ? AND last_upload_id = ?",
                (owner_support_company, upload_id),
            ).rowcount
            removed_uploads = connection.execute(
                "DELETE FROM uploads WHERE upload_id = ?", (upload_id,)
            ).rowcount
        return {
            "removed_records": max(0, removed_records),
            "removed_uploads": max(0, removed_uploads),
        }

    def rebuild_owner_records(
        self,
        owner_support_company: str,
        uploads: list[tuple[str, str, pd.DataFrame]],
    ) -> int:
        """Replay retained NPS ingestions so deleting one upload restores prior versions."""
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM records WHERE service_origin = ?", (owner_support_company,)
            )
            connection.execute(
                """
                DELETE FROM upload_records
                WHERE upload_id IN (
                    SELECT upload_id FROM uploads WHERE service_origin = ?
                )
                """,
                (owner_support_company,),
            )
            for upload_id, uploaded_at, frame in uploads:
                self.upsert_records(
                    upload_id=upload_id,
                    uploaded_at=uploaded_at,
                    frame=frame,
                    _connection=connection,
                )
        return len(uploads)

    def delete_owner_data(self, owner_support_company: str) -> dict[str, int]:
        """Delete all NPS records and ingestion history for one owner company."""
        with self._connect() as connection:
            removed_records = connection.execute(
                "DELETE FROM records WHERE service_origin = ?", (owner_support_company,)
            ).rowcount
            removed_uploads = connection.execute(
                "DELETE FROM uploads WHERE service_origin = ?", (owner_support_company,)
            ).rowcount
            connection.execute(
                "DELETE FROM taxonomy_artifacts WHERE context LIKE ?",
                (f'{owner_support_company}:%',),
            )
            connection.execute(
                "DELETE FROM taxonomy_state WHERE context LIKE ?",
                (f'{owner_support_company}:%',),
            )
        return {
            "removed_records": max(0, removed_records),
            "removed_uploads": max(0, removed_uploads),
        }

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
                source_channel AS Canal,
                source_lever AS Palanca,
                source_sublever AS Subpalanca,
                source_channel, source_lever, source_sublever, source_preserved,
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
            """
            params.append(context.service_origin)
        query += " ORDER BY response_at ASC"

        with self._connect() as connection:
            frame = pd.read_sql_query(query, connection, params=params)
        if frame.empty:
            return frame
        frame["Fecha"] = pd.to_datetime(frame["Fecha"], errors="coerce")
        frame["NPS Group"] = classify_nps_scores(frame["NPS"])
        # Business dimensions repeat heavily across the corpus. Categoricals preserve
        # their exact labels while avoiding one Python string object per row.
        for column in (
            "NPS Group",
            "UsuarioDecisión",
            "Canal",
            "Palanca",
            "Subpalanca",
            "Browser",
            "Operating System",
            "service_origin",
            "service_origin_n1",
            "service_origin_n2",
        ):
            if column not in frame.columns:
                continue
            unique_values = int(frame[column].nunique(dropna=False))
            if unique_values <= max(64, len(frame) // 10):
                frame[column] = frame[column].astype("category")
        return frame

    def records_profile(self, context: UploadContext) -> dict[str, object]:
        """Return navigation metadata without materializing the customer corpus."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    substr(response_at, 1, 4) AS year,
                    substr(response_at, 6, 2) AS month,
                    source_channel AS channel,
                    COUNT(*) AS row_count
                FROM records
                WHERE service_origin = ?
                GROUP BY year, month, channel
                ORDER BY year, month, channel
                """,
                (context.service_origin,),
            ).fetchall()

        periods: set[tuple[str, str]] = set()
        channels: list[str] = []
        seen_channels: set[str] = set()
        total_rows = 0
        for row in rows:
            year, month = str(row["year"] or ""), str(row["month"] or "")
            if len(year) == 4 and len(month) == 2:
                periods.add((year, month))
            channel = str(row["channel"] or "").strip()
            if channel and channel not in seen_channels:
                seen_channels.add(channel)
                channels.append(channel)
            total_rows += int(row["row_count"] or 0)
        return {
            "rows": total_rows,
            "columns": 17,
            "periods": sorted(periods),
            "score_channels": channels,
        }

    def channels_by_owner(self, owners: list[str]) -> dict[str, list[str]]:
        """Return the source NPS channels observed for each Owner Support Company."""
        result = {owner: [] for owner in owners}
        if not owners:
            return result
        placeholders = ",".join("?" for _ in owners)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT service_origin, source_channel
                FROM records
                WHERE service_origin IN ({placeholders}) AND trim(source_channel) <> ''
                GROUP BY service_origin, source_channel
                ORDER BY service_origin, source_channel
                """,
                owners,
            ).fetchall()
        for row in rows:
            owner = str(row["service_origin"])
            if owner in result:
                result[owner].append(str(row["source_channel"]))
        return result

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
                """
                params.append(context.service_origin)
            duplicates_prevented_row = connection.execute(query, params).fetchone()
        duplicates_prevented = (
            int(duplicates_prevented_row["duplicates"]) if duplicates_prevented_row else 0
        )

        if records.empty:
            return SummarySnapshot(
                total_records=0,
                date_range={"min": None, "max": None},
                classic_nps=None,
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
        classic_nps = None
        if promoter_rate is not None and detractor_rate is not None:
            classic_nps = float((promoter_rate - detractor_rate) * 100.0)

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
            classic_nps=classic_nps,
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
