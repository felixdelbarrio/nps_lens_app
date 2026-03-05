from __future__ import annotations

from pathlib import Path

from nps_lens.llm.knowledge_cache import KnowledgeCache, stable_signature


def test_cache_upsert_and_find(tmp_path: Path) -> None:
    p = tmp_path / "cache.json"
    kc = KnowledgeCache(p)
    context = {"geo": "MX", "channel": "Senda"}
    sig = stable_signature(context, "foo")
    record = {"signature": sig, "title": "foo", "context": context}
    kc.upsert(sig, record)

    hit = kc.find(sig)
    assert hit is not None
    assert hit.entry["title"] == "foo"
