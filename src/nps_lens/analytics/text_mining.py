from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer

STOPWORDS_ES = {
    "de","la","que","el","en","y","a","los","del","se","las","por","un","para","con",
    "no","una","su","al","lo","como","más","pero","sus","le","ya","o","este","sí",
    "porque","esta","entre","cuando","muy","sin","sobre","también","me","hasta","hay",
    "donde","quien","desde","todo","nos","durante","todos","uno","les","ni","contra",
    "otros","ese","eso","ante","ellos","e","esto","mí","antes","algunos","qué","unos",
    "yo","otro","otras","otra","él","tanto","esa","estos","mucho","quienes","nada",
    "muchos","cual","poco","ella","estar","estas","algunas","algo","nosotros","mi",
}


TONE_LEXICON: dict[str, list[str]] = {
    "frustracion": [
        "no puedo",
        "no deja",
        "error",
        "falla",
        "fallo",
        "bloquea",
        "se queda",
        "lento",
        "intermitente",
        "timeout",
    ],
    "urgencia": ["urge", "urgente", "ya", "hoy", "ahora"],
    "confusion": ["no entiendo", "como", "donde", "por que", "qué"],
    "aprecio": ["excelente", "muy bien", "genial", "gracias", "práctica", "facil", "rápida"],
    "sugerencia": ["podrian", "deberian", "seria bueno", "me gustaria", "falta"],
}


@dataclass(frozen=True)
class TopicCluster:
    cluster_id: int
    n: int
    top_terms: list[str]
    examples: list[str]


def _clean_text(s: str) -> str:
    s2 = s.lower()
    s2 = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in s2)
    s2 = " ".join(s2.split())
    return s2


def extract_topics(
    texts: pd.Series, n_clusters: int = 10, max_features: int = 4000
) -> list[TopicCluster]:
    cleaned = texts.dropna().astype(str).map(_clean_text)
    cleaned = cleaned[cleaned.str.len() >= 5]
    if cleaned.empty:
        return []

    vec = TfidfVectorizer(
        max_features=max_features,
        ngram_range=(1, 2),
        stop_words=sorted(STOPWORDS_ES),
        min_df=2,
    )
    X = vec.fit_transform(cleaned.tolist())
    k = min(n_clusters, max(2, X.shape[0] // 50)) if X.shape[0] >= 100 else min(5, X.shape[0])
    if k < 2:
        return []
    km = KMeans(n_clusters=k, n_init=10, random_state=42)
    labels = km.fit_predict(X)

    terms = np.array(vec.get_feature_names_out())
    clusters: list[TopicCluster] = []
    for cid in range(k):
        idxs = np.where(labels == cid)[0]
        if len(idxs) == 0:
            continue
        center = km.cluster_centers_[cid]
        top = terms[np.argsort(center)[-10:][::-1]].tolist()
        ex = cleaned.iloc[idxs].head(5).tolist()
        clusters.append(TopicCluster(cluster_id=cid, n=int(len(idxs)), top_terms=top, examples=ex))
    clusters.sort(key=lambda c: c.n, reverse=True)
    return clusters


def classify_tone(text: Optional[str]) -> list[str]:
    if not text:
        return []
    t = _clean_text(text)
    labels: list[str] = []
    for label, pats in TONE_LEXICON.items():
        for p in pats:
            if p in t:
                labels.append(label)
                break
    return sorted(set(labels))
