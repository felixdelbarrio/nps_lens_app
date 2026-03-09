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


def test_cache_for_context_and_upsert_replaces_existing_entry(tmp_path: Path) -> None:
    kc = KnowledgeCache.for_context(tmp_path, "MX", "Senda")
    assert kc.path.name == "insights__MX__Senda.json"

    sig = stable_signature({"geo": "MX"}, "titulo")
    kc.upsert(sig, {"signature": sig, "title": "primero"})
    kc.upsert(sig, {"signature": sig, "title": "segundo"})

    loaded = kc.load()
    assert len(loaded["entries"]) == 1
    assert loaded["entries"][0]["title"] == "segundo"
    assert kc.find("missing") is None
