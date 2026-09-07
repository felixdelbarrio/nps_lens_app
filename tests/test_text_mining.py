from __future__ import annotations

import pandas as pd

from nps_lens.analytics.text_mining import classify_tone, summarize_taxonomy


def test_classify_tone_rules() -> None:
    labels = classify_tone("No puedo entrar, error 500. Urge.")
    assert "frustracion" in labels
    assert "urgencia" in labels


def test_summarize_taxonomy_smoke() -> None:
    s = pd.Series(
        [
            "no puedo entrar error login",
            "error login no deja",
            "transferencia spei rechazada",
            "spei timeout transferencia",
            "me encanta la app muy practica",
            "muy practica y rapida",
        ]
        * 30
    )
    topics = summarize_taxonomy(
        pd.DataFrame(
            {"Comment": s, "Palanca": ["Acceso"] * len(s), "Subpalanca": ["Login"] * len(s)}
        )
    )
    assert topics
    assert topics[0].n > 0
