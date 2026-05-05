from __future__ import annotations


def normalize_helix_base_url(base_url: object) -> str:
    raw = str(base_url or "").strip()
    if not raw:
        return ""
    return raw.rstrip("/") + "/"


def build_helix_url(record_id: object, *, base_url: object) -> str:
    normalized_base = normalize_helix_base_url(base_url)
    record = str(record_id or "").strip()
    if not normalized_base or not record:
        return ""
    return f"{normalized_base}{record}"
