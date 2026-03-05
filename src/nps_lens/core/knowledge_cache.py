from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd


@dataclass(frozen=True)
class KnowledgeEntry:
    ts: str  # ISO timestamp
    service_origin: str
    service_origin_n1: str
    service_origin_n2: str
    nps_topic: str
    outcome: str  # confirmed | rejected
    note: str


def _cache_file(knowledge_dir: Path) -> Path:
    return knowledge_dir / "knowledge_cache.jsonl"


def load_entries(knowledge_dir: Path) -> pd.DataFrame:
    path = _cache_file(knowledge_dir)
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "ts",
                "service_origin",
                "service_origin_n1",
                "service_origin_n2",
                "nps_topic",
                "outcome",
                "note",
            ]
        )
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append({k: str(v) for k, v in obj.items()})
            except Exception:
                continue
    df = pd.DataFrame(rows)
    for c in [
        "ts",
        "service_origin",
        "service_origin_n1",
        "service_origin_n2",
        "nps_topic",
        "outcome",
        "note",
    ]:
        if c not in df.columns:
            df[c] = ""
    return df


def add_entry(
    knowledge_dir: Path,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
    nps_topic: str,
    outcome: str,
    note: str = "",
) -> KnowledgeEntry:
    knowledge_dir.mkdir(parents=True, exist_ok=True)
    path = _cache_file(knowledge_dir)
    ts = datetime.now(timezone.utc).isoformat()
    entry = KnowledgeEntry(
        ts=ts,
        service_origin=str(service_origin),
        service_origin_n1=str(service_origin_n1),
        service_origin_n2=str(service_origin_n2 or ""),
        nps_topic=str(nps_topic),
        outcome=str(outcome),
        note=str(note or ""),
    )
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry.__dict__, ensure_ascii=False) + "\n")
    return entry


def score_adjustments(
    entries: pd.DataFrame,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
) -> pd.DataFrame:
    """Compute per-topic adjustment factors from confirmed/rejected learnings.

    Matching is strict on context (service_origin, n1, n2 string equality).
    """
    if entries.empty:
        return pd.DataFrame(columns=["nps_topic", "factor", "confirmed", "rejected"])

    df = entries.copy()
    df = df[
        (df["service_origin"] == str(service_origin))
        & (df["service_origin_n1"] == str(service_origin_n1))
        & (df["service_origin_n2"] == str(service_origin_n2 or ""))
    ].copy()
    if df.empty:
        return pd.DataFrame(columns=["nps_topic", "factor", "confirmed", "rejected"])

    grp = df.groupby(["nps_topic", "outcome"]).size().unstack(fill_value=0)
    confirmed = grp.get("confirmed", pd.Series(dtype=int))
    rejected = grp.get("rejected", pd.Series(dtype=int))
    out = pd.DataFrame(
        {
            "nps_topic": grp.index.astype(str),
            "confirmed": confirmed.reindex(grp.index, fill_value=0).astype(int).values,
            "rejected": rejected.reindex(grp.index, fill_value=0).astype(int).values,
        }
    )

    # Factor: boost confirmed, penalize rejected. Cap to keep scores interpretable.
    def _factor(c: int, r: int) -> float:
        if c == 0 and r == 0:
            return 1.0
        # confirmed: +15% per confirmation up to +50%
        boost = min(0.15 * float(c), 0.50)
        # rejected: -20% per rejection up to -70%
        pen = min(0.20 * float(r), 0.70)
        return max(0.30, min(1.60, (1.0 + boost) * (1.0 - pen)))

    out["factor"] = [
        _factor(int(c), int(r)) for c, r in zip(out["confirmed"].tolist(), out["rejected"].tolist())
    ]
    return out
