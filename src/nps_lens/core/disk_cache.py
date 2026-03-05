from __future__ import annotations

import json
import os
import pickle
import tempfile
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class CacheHit:
    key: str
    path: Path
    meta: dict[str, Any]


class DiskCache:
    """Deterministic on-disk cache for expensive compute artifacts.

    Why this exists (even with Streamlit cache):
    - Streamlit cache keys often depend on hashing large DataFrames (costly).
    - Hosting/redeploys can invalidate Streamlit cache unexpectedly.
    - We want atomic writes to avoid corrupt cache entries.

    The cache key is a SHA1 over: namespace + dataset signature + params.
    """

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def make_key(self, *, namespace: str, dataset_sig: str, params: dict[str, Any]) -> str:
        payload = {
            "ns": str(namespace),
            "ds": str(dataset_sig),
            "params": params,
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return sha1(raw.encode("utf-8")).hexdigest()

    def _paths(self, key: str) -> tuple[Path, Path]:
        sub = key[:2]
        d = self.base_dir / sub
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{key}.pkl", d / f"{key}.meta.json"

    def get(self, key: str) -> Optional[Any]:
        obj_path, meta_path = self._paths(key)
        if not obj_path.exists() or not meta_path.exists():
            return None
        try:
            with obj_path.open("rb") as f:
                return pickle.load(f)
        except Exception:
            # Corrupt cache entry -> delete best-effort.
            try:
                obj_path.unlink(missing_ok=True)
            except Exception:
                pass
            try:
                meta_path.unlink(missing_ok=True)
            except Exception:
                pass
            return None

    def get_with_meta(self, key: str) -> Optional[CacheHit]:
        obj_path, meta_path = self._paths(key)
        if not obj_path.exists() or not meta_path.exists():
            return None
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
        return CacheHit(key=key, path=obj_path, meta=meta)

    def set(self, key: str, value: Any, *, meta: Optional[dict[str, Any]] = None) -> None:
        obj_path, meta_path = self._paths(key)
        tmp_dir = obj_path.parent

        meta = meta or {}
        meta = {
            **meta,
            "key": key,
            "pid": int(os.getpid()),
        }

        # Atomic writes (correct direction): tmp -> fsync -> replace(target)
        # NOTE: pathlib.Path.replace() replaces *the target* with *self*.

        # Object
        with tempfile.NamedTemporaryFile(dir=str(tmp_dir), delete=False) as tf:
            tmp_obj = Path(tf.name)
            pickle.dump(value, tf, protocol=pickle.HIGHEST_PROTOCOL)
            tf.flush()
            os.fsync(tf.fileno())
        # Replace destination with the temp file (works even if destination doesn't exist)
        tmp_obj.replace(obj_path)

        # Metadata
        tmp_meta = tmp_dir / f"{key}.meta.tmp"
        with tmp_meta.open("w", encoding="utf-8") as f:
            f.write(json.dumps(meta, ensure_ascii=False, indent=2, default=str))
            f.flush()
            os.fsync(f.fileno())
        tmp_meta.replace(meta_path)
