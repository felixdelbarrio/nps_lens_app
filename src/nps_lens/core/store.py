from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Optional

import pandas as pd

from nps_lens.domain.helix import OWNER_SUPPORT_COMPANY, SOURCE_SERVICE_N1
from nps_lens.ingest.helix_dates import (
    coerce_helix_datetime_series,
    looks_like_helix_datetime_column,
)


def _clear_dir_tree(path: Path) -> None:
    """Best-effort recursive delete for cache directories."""
    if not path.exists():
        return
    for p in path.rglob("*"):
        if p.is_file():
            with contextlib.suppress(Exception):
                p.unlink()
    for p in sorted([p for p in path.rglob("*") if p.is_dir()], reverse=True):
        with contextlib.suppress(Exception):
            p.rmdir()
    with contextlib.suppress(Exception):
        path.rmdir()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class DatasetContext:
    service_origin: str
    service_origin_n1: str
    # Optional third context dimension. Empty string means "not set".
    service_origin_n2: str = ""

    @staticmethod
    def _norm_n2(raw: str) -> str:
        """Normalize N2 as a stable, comparable token-set string.

        - Accepts comma-separated values.
        - Trims whitespace.
        - Sorts tokens.
        - Joins with comma.

        Empty/None-like -> "".
        """
        if raw is None:
            return ""
        s = str(raw).strip()
        if not s:
            return ""
        tokens = [t.strip() for t in s.split(",") if t.strip()]
        if not tokens:
            return ""
        tokens = sorted(set(tokens))
        return ",".join(tokens)

    def key(self) -> str:
        n2 = self._norm_n2(self.service_origin_n2)
        if n2:
            return f"{self.service_origin}__{self.service_origin_n1}__{n2}"
        return f"{self.service_origin}__{self.service_origin_n1}"

    @staticmethod
    def from_key(key: str) -> "DatasetContext":
        parts = key.split("__")
        if len(parts) == 2:
            return DatasetContext(
                service_origin=parts[0], service_origin_n1=parts[1], service_origin_n2=""
            )
        if len(parts) >= 3:
            # N2 itself may contain "__" in theory, but our normalizer does not emit it.
            n2 = "__".join(parts[2:])
            return DatasetContext(
                service_origin=parts[0], service_origin_n1=parts[1], service_origin_n2=n2
            )
        raise ValueError(f"Invalid context key: {key}")


@dataclass(frozen=True)
class StoredDataset:
    context: DatasetContext
    path: Path
    meta_path: Path

    def data_key(self) -> str:
        """Stable cache key based on file metadata + path."""
        stat = self.path.stat()
        raw = f"{self.path.resolve()}|{stat.st_mtime_ns}|{stat.st_size}"
        return sha1(raw.encode("utf-8")).hexdigest()


class HelixIncidentStore:
    """Store for Helix incident exports per context.

    Separate from the NPS dataset store to keep contracts explicit.

    Source of truth: JSONL per context.
    Derived cache: partitioned Parquet dataset for later cross-source linking.
    """

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        # Small in-memory cache for available year/month per context (safe for Streamlit reruns).
        self._avail_year_month_cache: dict[str, tuple[list[str], dict[str, list[str]]]] = {}

    def _paths_for(self, ctx: DatasetContext) -> tuple[Path, Path, Path]:
        data_path = self.base_dir / f"helix_incidents__{ctx.key()}.jsonl"
        meta_path = self.base_dir / f"helix_incidents__{ctx.key()}.meta.json"

        cache_dir = self.base_dir / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        parquet_dir = cache_dir / f"helix_incidents__{ctx.key()}"
        return data_path, meta_path, parquet_dir

    def list_contexts(self) -> list[DatasetContext]:
        out: list[DatasetContext] = []
        for p in sorted(self.base_dir.glob("helix_incidents__*.meta.json")):
            key = p.name.replace("helix_incidents__", "").replace(".meta.json", "")
            try:
                out.append(DatasetContext.from_key(key))
            except ValueError:
                continue
        return out

    def get(self, ctx: DatasetContext) -> Optional[StoredDataset]:
        data_path, meta_path, _ = self._paths_for(ctx)
        if not data_path.exists() or not meta_path.exists():
            return None
        return StoredDataset(context=ctx, path=data_path, meta_path=meta_path)

    def delete(self, ctx: DatasetContext) -> None:
        data_path, meta_path, parquet_dir = self._paths_for(ctx)
        data_path.unlink(missing_ok=True)
        meta_path.unlink(missing_ok=True)
        _clear_dir_tree(parquet_dir)
        self._avail_year_month_cache.pop(ctx.key(), None)

    def available_periods(self, stored: StoredDataset) -> list[tuple[str, str]]:
        """Read temporal coverage without materializing the wide Helix dataset."""
        try:
            metadata = json.loads(stored.meta_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            metadata = {}
        configured = metadata.get("periods", [])
        periods = {
            (str(item[0]), str(item[1]).zfill(2))
            for item in configured
            if isinstance(item, list) and len(item) == 2
        }
        if periods:
            return sorted(periods)
        _data_path, _meta_path, parquet_dir = self._paths_for(stored.context)
        for path in parquet_dir.glob("Fecha_day=*"):
            value = path.name.partition("=")[2]
            if len(value) >= 7:
                periods.add((value[:4], value[5:7]))
        return sorted(periods)

    def save_df(self, ctx: DatasetContext, df: pd.DataFrame, source: str) -> StoredDataset:
        data_path, meta_path, parquet_dir = self._paths_for(ctx)

        df_out = df.copy()

        if "Fecha" in df_out.columns:
            df_out["Fecha"] = pd.to_datetime(df_out["Fecha"], errors="coerce")

        df_out.to_json(
            data_path,
            orient="records",
            lines=True,
            force_ascii=False,
            date_format="iso",
        )

        stat = data_path.stat()

        # Build/refresh parquet cache
        partitioning: list[str] = []
        try:
            partitioning = self._write_parquet_dataset(df_out, parquet_dir)
        except Exception:
            # Keep import resilient: parquet is only a derived cache.
            _clear_dir_tree(parquet_dir)

        meta = {
            "schema_version": "1.0",
            "context": {
                "service_origin": ctx.service_origin,
                "service_origin_n1": ctx.service_origin_n1,
            },
            "rows": int(len(df_out)),
            "cols": int(len(df_out.columns)),
            "source": source,
            "updated_at_utc": _utc_now_iso(),
            "periods": (
                sorted(
                    {
                        (str(value.year), str(value.month).zfill(2))
                        for value in pd.to_datetime(df_out.get("Fecha"), errors="coerce").dropna()
                    }
                )
                if "Fecha" in df_out.columns
                else []
            ),
            "jsonl_mtime_ns": int(stat.st_mtime_ns),
            "jsonl_size": int(stat.st_size),
            "parquet_dataset": {
                "path": str(parquet_dir),
                "rows": int(len(df_out)),
                "cols": int(len(df_out.columns)),
                "partitioning": partitioning,
            },
        }
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return StoredDataset(context=ctx, path=data_path, meta_path=meta_path)

    def load_df(
        self,
        stored: StoredDataset,
        columns: Optional[list[str]] = None,
        date_start: Optional[pd.Timestamp] = None,
        date_end: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """Load Helix incidents for a context.

        JSONL is the single source of truth (written by `save_df`).

        """
        data_path, _, _ = self._paths_for(stored.context)
        if not data_path.exists():
            return pd.DataFrame()

        try:
            df = pd.read_json(data_path, orient="records", lines=True, dtype=False)
        except ValueError:
            return pd.DataFrame()

        if columns:
            keep = [c for c in columns if c in df.columns]
            if keep:
                df = df[keep]

        if "Fecha" in df.columns:
            df["Fecha"] = coerce_helix_datetime_series(df["Fecha"])

        # Best-effort: convert any other date-like columns from epoch/strings to datetime
        for c in list(df.columns):
            if c == "Fecha":
                continue
            if not looks_like_helix_datetime_column(c):
                continue
            try:
                dt = coerce_helix_datetime_series(df[c])
                if len(dt) and float(dt.notna().mean()) >= 0.6:
                    df[c] = dt
            except Exception:
                continue

        # Fallback: if Fecha is missing or poorly parsed, attempt to recover from common timestamp columns.
        if ("Fecha" not in df.columns) or (
            "Fecha" in df.columns and float(df["Fecha"].notna().mean()) < 0.4
        ):
            for c in [
                "Submit Date",
                "SubmitDate",
                "Submitted Date",
                "Last Modified Date",
                "bbva_startdatetime",
                "bbva_closeddate",
            ]:
                if c in df.columns:
                    dt = coerce_helix_datetime_series(df[c])
                    if float(dt.notna().mean()) >= 0.4:
                        df["Fecha"] = dt
                        break

        if "Fecha" in df.columns:
            if date_start is not None:
                df = df[df["Fecha"] >= pd.to_datetime(date_start)]
            if date_end is not None:
                end_ts = pd.to_datetime(date_end)
                # If a pure date (00:00:00), interpret as inclusive end-of-day
                if (
                    end_ts.hour == 0
                    and end_ts.minute == 0
                    and end_ts.second == 0
                    and end_ts.microsecond == 0
                ):
                    end_ts = end_ts + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
                df = df[df["Fecha"] <= end_ts]

        return df

    def _write_parquet_dataset(self, df: pd.DataFrame, parquet_dir: Path) -> list[str]:
        # Ensure clean dir
        if parquet_dir.exists():
            _clear_dir_tree(parquet_dir)
        parquet_dir.mkdir(parents=True, exist_ok=True)

        d = df.copy()
        partition_cols: list[str] = []

        if "Fecha" in d.columns and not d["Fecha"].isna().all():
            d["Fecha_day"] = pd.to_datetime(d["Fecha"], errors="coerce").dt.date.astype("string")
            partition_cols.append("Fecha_day")

        # Company geography comes from Owner Support Company; service origin is not a geo proxy.
        for c in [OWNER_SUPPORT_COMPANY, SOURCE_SERVICE_N1]:
            if c in d.columns:
                nunique = int(d[c].astype("string").nunique(dropna=True))
                if nunique <= 50:
                    d[c] = d[c].astype("string")
                    partition_cols.append(c)

        if not partition_cols:
            d.to_parquet(parquet_dir / "part-0.parquet", index=False)
            return []

        d.to_parquet(parquet_dir, index=False, partition_cols=partition_cols)
        return partition_cols
