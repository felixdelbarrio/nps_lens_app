from __future__ import annotations

import hashlib
import json
from typing import Any

from nps_lens.domain.causal_methods import get_causal_method_spec


def build_publication_scope(
    *, buug: str, n1: str, n2: str, year: str, month: str, causal_method: str
) -> dict[str, Any]:
    """Return the immutable identity shared by local export, WebApp and newsletter."""

    values = {
        "buug": str(buug).strip(),
        "n1": str(n1).strip(),
        "n2": str(n2).strip(),
        "year": str(year).strip(),
        "month": str(month).strip(),
        "causal_method": str(causal_method).strip(),
    }
    missing = [key for key in ("buug", "n1", "year", "month", "causal_method") if not values[key]]
    if missing or values["year"] == "Todos" or values["month"] == "Todos":
        raise ValueError("Selecciona BUUG, N1, año, mes y método causal antes de preparar la edición web.")
    canonical = json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    scope_key = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    audience_canonical = json.dumps(
        {"buug": values["buug"], "n1": values["n1"]},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    audience_key = hashlib.sha256(audience_canonical.encode("utf-8")).hexdigest()[:20]
    method = get_causal_method_spec(values["causal_method"])
    return {
        **values,
        "key": scope_key,
        "audience_key": audience_key,
        "causal_method_label": method.label,
        "label": " · ".join(
            [values["buug"], values["n1"], values["year"], values["month"], method.label]
        ),
    }

