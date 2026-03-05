from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from nps_lens.llm.knowledge_cache import stable_signature


@dataclass(frozen=True)
class ArtifactPaths:
    root: Path
    context_dir: Path
    insights_dir: Path
    kpis_path: Path
    manifest_path: Path


def make_context_signature(context: Dict[str, str]) -> str:
    """Stable, filesystem-safe signature for a context dict."""
    # stable_signature returns hex; keep it short for path hygiene
    return stable_signature(context=context, title="context")[:16]


def ensure_artifact_dirs(
    *,
    out_root: Path,
    dataset_id: str,
    pipeline_version: str,
    context: Dict[str, str],
) -> ArtifactPaths:
    """Create a deterministic artifact folder layout.

    Layout:
      artifacts/
        <dataset_id>/
          <pipeline_version>/
            <ctx_sig>/
              insights/
              kpis.json
              manifest.json
    """
    ctx_sig = make_context_signature(context)
    context_dir = out_root / dataset_id / pipeline_version / ctx_sig
    insights_dir = context_dir / "insights"
    insights_dir.mkdir(parents=True, exist_ok=True)
    return ArtifactPaths(
        root=out_root,
        context_dir=context_dir,
        insights_dir=insights_dir,
        kpis_path=context_dir / "kpis.json",
        manifest_path=context_dir / "manifest.json",
    )


def write_json_atomic(path: Path, obj: Any) -> None:
    """Atomic JSON write to avoid partially-written artifacts."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def update_manifest(
    *,
    paths: ArtifactPaths,
    dataset_meta: Dict[str, Any],
    run_params: Dict[str, Any],
    perf: Optional[Dict[str, Any]] = None,
    notes: Optional[str] = None,
) -> None:
    manifest = {
        "dataset": dataset_meta,
        "run_params": run_params,
        "perf": perf or {},
        "notes": notes or "",
    }
    write_json_atomic(paths.manifest_path, manifest)
