from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Optional, cast


def stable_signature(context: dict[str, str], title: str) -> str:
    key = json.dumps({"title": title, "context": context}, sort_keys=True, ensure_ascii=False)
    return sha1(key.encode("utf-8")).hexdigest()


@dataclass
class CacheHit:
    signature: str
    entry: dict[str, Any]


class KnowledgeCache:

    @classmethod
    def for_context(cls, base_dir: Path, service_origin: str, service_origin_n1: str) -> "KnowledgeCache":
        base_dir.mkdir(parents=True, exist_ok=True)
        fname = f"insights__{service_origin}__{service_origin_n1}.json"
        return cls(base_dir / fname)

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            init = {"schema_version": "1.0", "entries": []}
            self.path.write_text(json.dumps(init, ensure_ascii=False, indent=2), encoding="utf-8")

    def load(self) -> dict[str, Any]:
        # json.loads is typed as Any; cast to keep strict mypy happy.
        return cast(dict[str, Any], json.loads(self.path.read_text(encoding="utf-8")))

    def find(self, signature: str) -> Optional[CacheHit]:
        data = self.load()
        for e in data.get("entries", []):
            if e.get("signature") == signature:
                return CacheHit(signature=signature, entry=e)
        return None

    def upsert(self, signature: str, record: dict[str, Any]) -> None:
        data = self.load()
        entries: list[dict[str, Any]] = data.get("entries", [])
        updated = False
        for i, e in enumerate(entries):
            if e.get("signature") == signature:
                entries[i] = record
                updated = True
                break
        if not updated:
            entries.append(record)
        data["entries"] = entries
        payload = json.dumps(data, ensure_ascii=False, indent=2)
        # Atomic write to avoid corruption if the process is interrupted.
        with NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(self.path.parent)) as tmp:
            tmp.write(payload)
            tmp_path = Path(tmp.name)
        tmp_path.replace(self.path)
