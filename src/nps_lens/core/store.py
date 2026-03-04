from __future__ import annotations

import json
from functools import lru_cache
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Optional, Sequence, Tuple

import pandas as pd


# ---------------------------
# In-process dataset cache
# ---------------------------
# Streamlit reruns re-execute the script but keep the Python process. An in-process LRU cache
# prevents reading the exact same (projection + filter) subset multiple times across charts.
# Cache keys include a signature derived from the JSONL metadata to ensure correctness.



def _norm_date(d: Optional[pd.Timestamp]) -> Optional[str]:
    if d is None:
        return None
    return str(pd.to_datetime(d).date())


def _norm_values(values: Optional[Sequence[str]]) -> Tuple[str, ...]:
    if not values:
        return tuple()
    # stable order
    return tuple(sorted({str(v) for v in values if str(v)}))


@lru_cache(maxsize=16)
def _load_parquet_subset_table_cached(
    parquet_dir: str,
    columns: Tuple[str, ...],
    date_start: Optional[str],
    date_end: Optional[str],
    lever_values: Tuple[str, ...],
    signature: str,
):
    """Load a projected & filtered Parquet subset as an Arrow Table.

    This function is cached in-process (LRU). It performs predicate pushdown on
    partition columns and scans in RecordBatches (lower peak memory).
    """
    import pyarrow as pa  # type: ignore
    import pyarrow.dataset as ds  # type: ignore

    dataset = ds.dataset(Path(parquet_dir), format="parquet", partitioning="hive")
    flt = None
    parts = []

    # Predicate pushdown on Fecha_day partition
    if date_start is not None or date_end is not None:
        f = ds.field("Fecha_day")
        if date_start is not None:
            parts.append(f >= date_start)
        if date_end is not None:
            parts.append(f <= date_end)

    # Optional Palanca filter (also pushdown when partitioned by Palanca)
    if lever_values:
        p = ds.field("Palanca")
        if len(lever_values) == 1:
            parts.append(p == lever_values[0])
        else:
            parts.append(p.isin(list(lever_values)))

    if parts:
        flt = parts[0]
        for pexpr in parts[1:]:
            flt = flt & pexpr

    cols = list(columns) if columns else None
    scanner = dataset.scanner(columns=cols, filter=flt, use_threads=True, batch_size=65536)
    batches = list(scanner.to_batches())
    return pa.Table.from_batches(batches) if batches else pa.table({})


@lru_cache(maxsize=16)
def _load_parquet_subset_cached(
    parquet_dir: str,
    columns: Tuple[str, ...],
    date_start: Optional[str],
    date_end: Optional[str],
    lever_values: Tuple[str, ...],
    signature: str,
) -> pd.DataFrame:
    table = _load_parquet_subset_table_cached(
        parquet_dir, columns, date_start, date_end, lever_values, signature
    )
    return table.to_pandas()

def _subset_key(
    base: str,
    columns: Tuple[str, ...],
    date_start: Optional[str],
    date_end: Optional[str],
    lever_values: Tuple[str, ...],
    signature: str,
) -> str:
    payload = {
        "base": base,
        "cols": list(columns),
        "ds": date_start,
        "de": date_end,
        "lv": list(lever_values),
        "sig": signature,
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return sha1(raw.encode("utf-8")).hexdigest()


@lru_cache(maxsize=8)
def _load_jsonl_subset_table_cached(
    jsonl_path: str,
    columns: Tuple[str, ...],
    date_start: Optional[str],
    date_end: Optional[str],
    lever_values: Tuple[str, ...],
    signature: str,
):
    """Load a projected & filtered JSONL subset as an Arrow Table (best-effort).

    JSONL is the source of truth; this path is mostly a fallback when Parquet cache
    is missing/invalid. We still project columns early to reduce RAM.
    """
    import pyarrow as pa  # type: ignore

    df = pd.read_json(Path(jsonl_path), orient="records", lines=True)
    if columns:
        keep = [c for c in columns if c in df.columns]
        df = df[keep].copy()

    if "Fecha" in df.columns and (date_start is not None or date_end is not None):
        day = pd.to_datetime(df["Fecha"], errors="coerce").dt.floor("D")
        df = df.assign(_day=day)
        if date_start is not None:
            df = df.loc[df["_day"] >= pd.to_datetime(date_start)]
        if date_end is not None:
            df = df.loc[df["_day"] <= pd.to_datetime(date_end)]
        df = df.drop(columns=["_day"], errors="ignore")

    if lever_values and "Palanca" in df.columns:
        df = df.loc[df["Palanca"].astype(str).isin(list(lever_values))]

    # Convert to Arrow Table at the end.
    return pa.Table.from_pandas(df, preserve_index=False)


@lru_cache(maxsize=8)
def _load_jsonl_subset_cached(
    jsonl_path: str,
    columns: Tuple[str, ...],
    date_start: Optional[str],
    date_end: Optional[str],
    lever_values: Tuple[str, ...],
    signature: str,
) -> pd.DataFrame:
    table = _load_jsonl_subset_table_cached(
        jsonl_path, columns, date_start, date_end, lever_values, signature
    )
    return table.to_pandas()

@dataclass(frozen=True)
class DatasetContext:
    service_origin: str
    service_origin_n1: str

    def key(self) -> str:
        return f"{self.service_origin}__{self.service_origin_n1}"

    @staticmethod
    def from_key(key: str) -> "DatasetContext":
        parts = key.split("__", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid context key: {key}")
        return DatasetContext(service_origin=parts[0], service_origin_n1=parts[1])


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


class DatasetStore:
    """Single source of truth for NPS datasets per (geo, channel).

    Source of truth: JSON Lines (records) per context.
    Derived caches:
      - Parquet dataset (partitioned) for fast loads and predicate pushdown.
      - Compact index parquet for fast KPI slices (Fecha_day x Palanca x Canal).
    """

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _paths_for(self, ctx: DatasetContext) -> tuple[Path, Path, Path, Path, Path]:
        data_path = self.base_dir / f"nps__{ctx.key()}.jsonl"
        meta_path = self.base_dir / f"nps__{ctx.key()}.meta.json"

        cache_dir = self.base_dir / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        # Parquet *dataset directory* (partitioned)
        parquet_dir = cache_dir / f"nps__{ctx.key()}"

        hot_dir = cache_dir / "hot"
        hot_dir.mkdir(parents=True, exist_ok=True)

        index_dir = self.base_dir / "index"
        index_dir.mkdir(parents=True, exist_ok=True)
        index_path = index_dir / f"nps_index__{ctx.key()}.parquet"
        return data_path, meta_path, parquet_dir, index_path, hot_dir

    def list_contexts(self) -> list[DatasetContext]:
        out: list[DatasetContext] = []
        for p in sorted(self.base_dir.glob("nps__*.meta.json")):
            key = p.name.replace("nps__", "").replace(".meta.json", "")
            try:
                out.append(DatasetContext.from_key(key))
            except ValueError:
                continue
        return out

    def get(self, ctx: DatasetContext) -> Optional[StoredDataset]:
        data_path, meta_path, _, _, _ = self._paths_for(ctx)
        if not data_path.exists() or not meta_path.exists():
            return None
        return StoredDataset(context=ctx, path=data_path, meta_path=meta_path)

    def default_context(self) -> Optional[DatasetContext]:
        ctxs = self.list_contexts()
        return ctxs[0] if ctxs else None

    
    def load_table(
        self,
        stored: StoredDataset,
        columns: Optional[list[str]] = None,
        date_start: Optional[pd.Timestamp] = None,
        date_end: Optional[pd.Timestamp] = None,
        lever_values: Optional[Sequence[str]] = None,
    ):
        """Load a subset as an Arrow Table (preferred for KPI/streaming paths).

        This uses the Parquet dataset cache when valid; otherwise it falls back to
        JSONL. It performs:
          - column projection
          - predicate pushdown on Fecha_day (and Palanca when partitioned)
          - RecordBatch scanning (lower peak memory)
          - in-process LRU caching keyed by projection+filters+dataset signature
        """
        import pyarrow as pa  # type: ignore

        data_path, meta_path, parquet_dir, index_path, hot_dir = self._paths_for(stored.context)
        stat = data_path.stat()

        meta: dict = {}
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}

        meta_mtime = meta.get("jsonl_mtime_ns")
        meta_size = meta.get("jsonl_size")
        cache_ok = (
            parquet_dir.exists()
            and isinstance(meta_mtime, int)
            and isinstance(meta_size, int)
            and meta_mtime == int(stat.st_mtime_ns)
            and meta_size == int(stat.st_size)
        )

        cols_t = tuple(columns) if columns else tuple()
        dsig = f"{int(stat.st_mtime_ns)}|{int(stat.st_size)}"
        subset_hash = _subset_key(
            str(parquet_dir),
            cols_t,
            _norm_date(date_start),
            _norm_date(date_end),
            _norm_values(lever_values),
            dsig,
        )
        hot_path = hot_dir / f"subset__{stored.context.key()}__{subset_hash}.parquet"
        if hot_path.exists():
            try:
                import pyarrow.parquet as pq  # type: ignore
                return pq.read_table(hot_path)
            except Exception:
                pass

        if cache_ok:
            try:
                table = _load_parquet_subset_table_cached(
                    str(parquet_dir),
                    cols_t,
                    _norm_date(date_start),
                    _norm_date(date_end),
                    _norm_values(lever_values),
                    dsig,
                )
                # Persist hot subset on disk (best-effort) to speed future sessions
                try:
                    import pyarrow.parquet as pq  # type: ignore
                    pq.write_table(table, hot_path)
                except Exception:
                    pass
                return table
            except Exception:
                pass

        # Fallback: JSONL
        table = _load_jsonl_subset_table_cached(
            str(data_path),
            cols_t,
            _norm_date(date_start),
            _norm_date(date_end),
            _norm_values(lever_values),
            dsig,
        )
        try:
            import pyarrow.parquet as pq  # type: ignore
            pq.write_table(table, hot_path)
        except Exception:
            pass
        return table

    def load_df(
        self,
        stored: StoredDataset,
        columns: Optional[list[str]] = None,
        date_start: Optional[pd.Timestamp] = None,
        date_end: Optional[pd.Timestamp] = None,
        lever_values: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        """Load dataset for a context.

        JSONL is the single source of truth.
        Parquet dataset is a derived cache for fast startup and lower CPU/RAM.
        """
        data_path, meta_path, parquet_dir, index_path, hot_dir = self._paths_for(stored.context)
        stat = data_path.stat()

        meta: dict = {}
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}

        meta_mtime = meta.get("jsonl_mtime_ns")
        meta_size = meta.get("jsonl_size")
        cache_ok = (
            parquet_dir.exists()
            and isinstance(meta_mtime, int)
            and isinstance(meta_size, int)
            and meta_mtime == int(stat.st_mtime_ns)
            and meta_size == int(stat.st_size)
        )

        
        cols_t = tuple(columns) if columns else tuple()
        dsig = f"{int(stat.st_mtime_ns)}|{int(stat.st_size)}"

        # Hot subset persistence: if the same projection+filter is requested often,
        # reuse a small parquet subset stored on disk (cross-session speedup).
        subset_hash = _subset_key(str(parquet_dir), cols_t, _norm_date(date_start), _norm_date(date_end), _norm_values(lever_values), dsig)
        hot_path = hot_dir / f"subset__{stored.context.key()}__{subset_hash}.parquet"
        if hot_path.exists():
            try:
                return pd.read_parquet(hot_path).copy()
            except Exception:
                pass

        if cache_ok:
            try:
                dfp = _load_parquet_subset_cached(
                    str(parquet_dir),
                    cols_t,
                    _norm_date(date_start),
                    _norm_date(date_end),
                    _norm_values(lever_values),
                    dsig,
                )
                out = dfp.copy()
                # Persist a "hot" subset when it is small enough (best-effort).
                if len(out) <= 300_000 and len(out.columns) <= 25:
                    try:
                        out.to_parquet(hot_path, index=False)
                    except Exception:
                        pass
                return out
            except Exception:
                # Fall back to JSONL
                pass

                # JSONL: source of truth. Use cached subset to avoid repeated IO.
        df = _load_jsonl_subset_cached(
            str(data_path),
            cols_t,
            _norm_date(date_start),
            _norm_date(date_end),
            _norm_values(lever_values),
            dsig,
        ).copy()

        if len(df) <= 300_000 and len(df.columns) <= 25:
            try:
                df.to_parquet(hot_path, index=False)
            except Exception:
                pass

        # Best-effort cache rebuild
        try:
            self._write_parquet_dataset(df, parquet_dir)
            meta["jsonl_mtime_ns"] = int(stat.st_mtime_ns)
            meta["jsonl_size"] = int(stat.st_size)
            meta["parquet_dataset"] = {
                "path": str(parquet_dir),
                "rows": int(len(df)),
                "cols": int(len(df.columns)),
                "partitioning": meta.get("parquet_dataset", {}).get("partitioning", []),
            }
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            try:
                idx_df = self._build_compact_index(df)
                idx_df.to_parquet(index_path, index=False)
                meta["index"] = {"path": str(index_path), "rows": int(len(idx_df))}
                meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass
        except Exception:
            pass

        return df

    def save_df(self, ctx: DatasetContext, df: pd.DataFrame, source: str) -> StoredDataset:
        data_path, meta_path, parquet_dir, index_path, _hot_dir = self._paths_for(ctx)

        # Normalize column order for stable diffs
        df_out = df.copy()
        df_out = df_out.reindex(sorted(df_out.columns), axis=1)

        # Ensure datetimes are serializable
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

        # Build/refresh parquet cache (derived). JSONL remains source of truth.
        partitioning = self._write_parquet_dataset(df_out, parquet_dir)

        # Build compact index (date x palanca x canal) for fast filtering and consistent KPIs.
        try:
            idx_df = self._build_compact_index(df_out)
            idx_df.to_parquet(index_path, index=False)
        except Exception:
            idx_df = pd.DataFrame()

        meta = {
            "schema_version": "1.0",
            "context": {"service_origin": ctx.service_origin, "service_origin_n1": ctx.service_origin_n1},
            "rows": int(len(df_out)),
            "cols": int(len(df_out.columns)),
            "source": source,
            "updated_at_utc": pd.Timestamp.utcnow().isoformat() + "Z",
            "jsonl_mtime_ns": int(stat.st_mtime_ns),
            "jsonl_size": int(stat.st_size),
            "parquet_dataset": {
                "path": str(parquet_dir),
                "rows": int(len(df_out)),
                "cols": int(len(df_out.columns)),
                "partitioning": partitioning,
            },
            "index": {"path": str(index_path), "rows": int(len(idx_df))},
        }
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return StoredDataset(context=ctx, path=data_path, meta_path=meta_path)

    def _write_parquet_dataset(self, df: pd.DataFrame, parquet_dir: Path) -> list[str]:
        """Write a partitioned parquet dataset for predicate pushdown.

        Always partitions by Fecha_day; optionally by Palanca when safe.
        """
        # Ensure a clean directory (atomic-ish update)
        if parquet_dir.exists():
            for p in parquet_dir.rglob("*"):
                if p.is_file():
                    p.unlink()
            for p in sorted([p for p in parquet_dir.rglob("*") if p.is_dir()], reverse=True):
                try:
                    p.rmdir()
                except Exception:
                    pass
        parquet_dir.mkdir(parents=True, exist_ok=True)

        d = df.copy()
        partition_cols: list[str] = []

        if "Fecha" in d.columns:
            d["Fecha_day"] = pd.to_datetime(d["Fecha"], errors="coerce").dt.date.astype("string")
            partition_cols.append("Fecha_day")

        # Optional Palanca partitioning: only if present and cardinality is reasonable.
        if "Palanca" in d.columns:
            nunique = int(d["Palanca"].astype("string").nunique(dropna=True))
            if nunique <= 50:
                d["Palanca"] = d["Palanca"].astype("string")
                partition_cols.append("Palanca")

        # Fallback: if no Fecha, don't partition
        if not partition_cols:
            d.to_parquet(parquet_dir / "part-0.parquet", index=False)
            return []

        # pandas will write a dataset directory when partition_cols is provided (pyarrow backend)
        d.to_parquet(parquet_dir, index=False, partition_cols=partition_cols)
        return partition_cols

    def _build_compact_index(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty or "Fecha" not in df.columns:
            return pd.DataFrame(
                columns=[
                    "Fecha_day",
                    "Palanca",
                    "Canal",
                    "n",
                    "nps_avg",
                    "promoter_rate",
                    "detractor_rate",
                    "nps_classic_pp",
                ]
            )
        d = df.copy()
        d["Fecha_day"] = pd.to_datetime(d["Fecha"], errors="coerce").dt.date.astype("string")
        # Ensure required dims exist
        if "Palanca" not in d.columns:
            d["Palanca"] = "Unknown"
        if "Canal" not in d.columns:
            d["Canal"] = "Unknown"

        scores = pd.to_numeric(d.get("NPS"), errors="coerce")
        d["_score"] = scores
        d["_is_prom"] = d["_score"] >= 9
        d["_is_det"] = d["_score"] <= 6

        g = d.dropna(subset=["Fecha_day"]).groupby(["Fecha_day", "Palanca", "Canal"], dropna=False)
        out = (
            g.agg(
                n=("_score", "count"),
                nps_avg=("_score", "mean"),
                promoter_rate=("_is_prom", "mean"),
                detractor_rate=("_is_det", "mean"),
            )
            .reset_index()
        )
        out["nps_classic_pp"] = (out["promoter_rate"] - out["detractor_rate"]) * 100.0
        return out


class HelixIncidentStore:
    """Store for Helix incident exports per context.

    Separate from the NPS dataset store to keep contracts explicit.

    Source of truth: JSONL per context.
    Derived cache: partitioned Parquet dataset for later cross-source linking.
    """

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

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

    def save_df(self, ctx: DatasetContext, df: pd.DataFrame, source: str) -> StoredDataset:
        data_path, meta_path, parquet_dir = self._paths_for(ctx)

        df_out = df.copy()
        df_out = df_out.reindex(sorted(df_out.columns), axis=1)

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
        partitioning = self._write_parquet_dataset(df_out, parquet_dir)

        meta = {
            "schema_version": "1.0",
            "context": {"service_origin": ctx.service_origin, "service_origin_n1": ctx.service_origin_n1},
            "rows": int(len(df_out)),
            "cols": int(len(df_out.columns)),
            "source": source,
            "updated_at_utc": pd.Timestamp.utcnow().isoformat() + "Z",
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

        This mirrors `DatasetStore.load_df` in a simplified form so the UI can
        treat both stores uniformly.
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
            df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
            if date_start is not None:
                df = df[df["Fecha"] >= pd.to_datetime(date_start)]
            if date_end is not None:
                df = df[df["Fecha"] <= pd.to_datetime(date_end)]

        return df

    def _write_parquet_dataset(self, df: pd.DataFrame, parquet_dir: Path) -> list[str]:
        # Ensure clean dir
        if parquet_dir.exists():
            for p in parquet_dir.rglob("*"):
                if p.is_file():
                    p.unlink()
            for p in sorted([p for p in parquet_dir.rglob("*") if p.is_dir()], reverse=True):
                try:
                    p.rmdir()
                except Exception:
                    pass
        parquet_dir.mkdir(parents=True, exist_ok=True)

        d = df.copy()
        partition_cols: list[str] = []

        if "Fecha" in d.columns and not d["Fecha"].isna().all():
            d["Fecha_day"] = pd.to_datetime(d["Fecha"], errors="coerce").dt.date.astype("string")
            partition_cols.append("Fecha_day")

        # Optional partition by BBVA_SourceServiceCompany/N1 (low cardinality)
        for c in ["BBVA_SourceServiceCompany", "BBVA_SourceServiceN1"]:
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
