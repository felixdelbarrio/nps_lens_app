"""Deterministic local taxonomy completion. Only Comment enters the feature matrix."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any, Mapping

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
from threadpoolctl import threadpool_limits

from nps_lens.analytics.text_mining import STOPWORDS_ES, preprocess_text

ENGINE_VERSION = "2"
MODES = ("SOURCE", "NORMALIZED", "COMPLETED", "DISCOVERED")
SOURCE_COLUMNS = {
    "Canal": "source_channel",
    "Palanca": "source_lever",
    "Subpalanca": "source_sublever",
}


@dataclass(frozen=True)
class TaxonomyConfig:
    max_features: int = 5000
    certainty_threshold: float = 0.65
    min_f1: float = 0.65
    min_class_size: int = 4
    seed: int = 42

    def __post_init__(self) -> None:
        if not 100 <= self.max_features <= 20000:
            raise ValueError("Configuración fuera de rango: features 100–20000.")
        if (
            not 0.5 <= self.certainty_threshold <= 1
            or not 0 <= self.min_f1 <= 1
            or not 4 <= self.min_class_size <= 100
        ):
            raise ValueError("Umbrales de validación inválidos.")
        if not 0 <= self.seed < 2**32:
            raise ValueError("Semilla inválida.")


def text_series(frame: pd.DataFrame) -> pd.Series:
    return frame.get("Comment", pd.Series("", index=frame.index)).map(preprocess_text)


def labels(frame: pd.DataFrame, column: str) -> pd.Series:
    return frame.get(column, pd.Series("", index=frame.index)).astype("string").fillna("")


def detect_taxonomy(frame: pd.DataFrame) -> dict[str, Any]:
    pal = (
        labels(frame, SOURCE_COLUMNS["Palanca"] if "source_lever" in frame else "Palanca")
        .str.strip()
        .ne("")
    )
    sub = (
        labels(frame, SOURCE_COLUMNS["Subpalanca"] if "source_sublever" in frame else "Subpalanca")
        .str.strip()
        .ne("")
    )
    text = text_series(frame).str.contains(r"\b\w{2,}\b", regex=True)
    complete = int((pal & sub).sum())
    state = (
        "NO_TEXT"
        if not text.any()
        else "COMPLETE" if complete == len(frame) else "PARTIAL" if (pal | sub).any() else "MISSING"
    )
    return {
        "state": state,
        "rows": len(frame),
        "classified": complete,
        "missing": len(frame) - complete,
        "usable_comments": int(text.sum()),
    }


def signature(
    frame: pd.DataFrame,
    mode: str,
    config: TaxonomyConfig | Mapping[str, Any],
    equivalences: str = "",
) -> str:
    # Score, dates, visual filters and Helix equivalences are deliberately excluded.
    columns = ["_business_key", "Comment"]
    if mode == "COMPLETED":
        columns += ["Palanca", "Subpalanca"]
    data = frame.reindex(columns=columns).astype("string").fillna("").sort_values("_business_key")
    digest = sha256(pd.util.hash_pandas_object(data, index=False).values.tobytes())
    digest.update(
        json.dumps(
            [
                ENGINE_VERSION,
                mode,
                asdict(config) if isinstance(config, TaxonomyConfig) else dict(config),
                equivalences if mode == "COMPLETED" else "",
            ],
            sort_keys=True,
        ).encode()
    )
    return digest.hexdigest()


def vectorizer(config: TaxonomyConfig) -> TfidfVectorizer:
    return TfidfVectorizer(
        max_features=config.max_features,
        ngram_range=(1, 2),
        min_df=1,
        stop_words=sorted(STOPWORDS_ES),
        dtype=np.float32,
        sublinear_tf=True,
    )


def complete(frame: pd.DataFrame, config: TaxonomyConfig) -> dict[str, Any]:
    texts = text_series(frame)
    pal, sub = labels(frame, "Palanca").copy(), labels(frame, "Subpalanca").copy()
    provenance = pd.Series("human", index=frame.index)
    assignment_certainty = pd.Series(0.0, index=frame.index)
    quality: list[dict[str, Any]] = []

    def predict(
        target: pd.Series, train_mask: pd.Series, missing: pd.Series, dimension: str
    ) -> None:
        # Hold out unique comments: repeated exports cannot leak text across validation.
        train = pd.DataFrame({"text": texts, "label": target}).loc[
            train_mask & texts.ne("") & target.str.strip().ne("")
        ]
        conflicts = train.groupby("text")["label"].nunique()
        train = train.loc[train["text"].isin(conflicts[conflicts.eq(1)].index)].drop_duplicates(
            "text"
        )
        counts = train["label"].value_counts()
        eligible = counts[counts.ge(config.min_class_size)].index
        train = train.loc[train["label"].isin(eligible)]
        report: dict[str, Any] = {
            "dimension": dimension,
            "training_rows": len(train),
            "classes": len(eligible),
            "macro_f1": None,
            "assigned": 0,
        }
        quality.append(report)
        if len(eligible) < 2:
            report["reason"] = "Etiquetas o ejemplos independientes insuficientes para validar."
            return
        train_idx, test_idx = train_test_split(
            np.arange(len(train)),
            test_size=max(len(eligible), int(np.ceil(len(train) * 0.25))),
            stratify=train["label"],
            random_state=config.seed,
        )
        vec = vectorizer(config)
        try:
            x = vec.fit_transform(train["text"].iloc[train_idx])
        except ValueError:
            report["reason"] = "Vocabulario vacío."
            return
        model = LogisticRegression(
            max_iter=300, class_weight="balanced", random_state=config.seed, C=4
        )
        model.fit(x, train["label"].iloc[train_idx])
        score = float(
            f1_score(
                train["label"].iloc[test_idx],
                model.predict(vec.transform(train["text"].iloc[test_idx])),
                average="macro",
                zero_division=0,
            )
        )
        report["macro_f1"] = score
        if score < config.min_f1:
            report["reason"] = "Calidad insuficiente; se conserva Sin clasificar."
            return
        if not missing.any():
            return
        x = vec.fit_transform(train["text"])
        model.fit(x, train["label"])
        candidates = texts.loc[missing & texts.ne("")]
        if candidates.empty:
            return
        matrix = vec.transform(candidates)
        probabilities = model.predict_proba(matrix)
        best = probabilities.argmax(axis=1)
        certainty = probabilities.max(axis=1)
        accept = (certainty >= config.certainty_threshold) & (matrix.getnnz(axis=1) > 0)
        indices = candidates.index[accept]
        target.loc[indices] = model.classes_[best[accept]]
        assignment_certainty.loc[indices] = certainty[accept]
        provenance.loc[indices] = "completed"
        report["assigned"] = int(accept.sum())

    with threadpool_limits(limits=1):
        # Existing child labels are never paired with a speculative parent.
        predict(
            pal, pal.str.strip().ne(""), pal.str.strip().eq("") & sub.str.strip().eq(""), "Palanca"
        )
        original_pal = labels(frame, "Palanca")
        for parent in sorted(pal[pal.str.strip().ne("")].unique()):
            predict(
                sub,
                original_pal.eq(parent) & labels(frame, "Subpalanca").str.strip().ne(""),
                pal.eq(parent) & sub.str.strip().eq(""),
                f"Subpalanca · {parent}",
            )
    provenance.loc[pal.str.strip().eq("") | sub.str.strip().eq("")] = "unclassified"
    return {
        "lever": pal.tolist(),
        "sublever": sub.tolist(),
        "provenance": provenance.tolist(),
        "assignment_certainty": assignment_certainty.tolist(),
        "quality": quality,
        "nodes": [],
    }
