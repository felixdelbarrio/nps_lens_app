from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd

from nps_lens.core.disk_cache import DiskCache
from nps_lens.core.perf import PerfTracker
from nps_lens.core.profiling import profile_if_enabled


def _df_sig_light(df: pd.DataFrame) -> str:
    """Lightweight signature for compute caching.

    Prefer a stable dataset_id from store meta when available; otherwise,
    use a cheap signature based on shape + columns + a small sample hash.
    """
    cols = ",".join(list(df.columns))
    head = df.head(128).to_json(orient="records", force_ascii=False)
    return f"rows={len(df)}|cols={cols}|head={hash(head)}"


@dataclass
class AppService:
    """Thin application layer: orchestrates heavy compute with caching + timings.

    UI should call this instead of wiring analytics functions directly.
    """

    disk_cache: DiskCache
    perf: PerfTracker

    def cached(
        self,
        *,
        namespace: str,
        dataset_sig: str,
        params: Dict[str, Any],
        compute: Callable[[], Any],
        meta: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Any, bool]:
        key = self.disk_cache.make_key(namespace=namespace, dataset_sig=dataset_sig, params=params)
        hit = self.disk_cache.get(key)
        if hit is not None:
            return hit, True

        # Optional cProfile per expensive stage (guarded by env var NPS_LENS_PROFILE=1)
        from pathlib import Path

        profile_dir = Path("data") / "cache" / "profiles"
        with self.perf.timer(namespace), profile_if_enabled(profile_dir, tag=namespace):
            out = compute()
        self.disk_cache.set(
            key, out, meta={"namespace": namespace, **(meta or {}), "params": params}
        )
        return out, False

    # ----------------------------
    # Use-cases
    # ----------------------------

    def topics(
        self,
        texts: pd.Series,
        *,
        n_clusters: int = 8,
        max_features: int = 4000,
        dataset_sig: Optional[str] = None,
    ) -> Any:
        """Topic extraction (cached)."""
        from nps_lens.analytics.text_mining import extract_topics

        ds_sig = dataset_sig or f"texts:{hash(texts.head(1024).to_list())}|n={len(texts)}"
        params = {"n_clusters": int(n_clusters), "max_features": int(max_features)}

        out, _hit = self.cached(
            namespace="topics",
            dataset_sig=ds_sig,
            params=params,
            compute=lambda: extract_topics(
                texts, n_clusters=int(n_clusters), max_features=int(max_features)
            ),
            meta={"n_texts": int(len(texts))},
        )
        return out

    def routes(
        self,
        df: pd.DataFrame,
        *,
        incidents_df: Optional[pd.DataFrame] = None,
        lever_col: str = "Palanca",
        sublever_col: str = "Subpalanca",
        comment_col: str = "Comment",
        dataset_id: Optional[str] = None,
    ) -> Any:
        from nps_lens.analytics.journey import build_routes

        ds_sig = dataset_id or _df_sig_light(df)
        params = {
            "lever_col": lever_col,
            "sublever_col": sublever_col,
            "comment_col": comment_col,
            "has_incidents": bool(incidents_df is not None and not incidents_df.empty),
        }
        out, _hit = self.cached(
            namespace="routes",
            dataset_sig=ds_sig,
            params=params,
            compute=lambda: build_routes(
                df,
                incidents_df=incidents_df,
                lever_col=lever_col,
                sublever_col=sublever_col,
                comment_col=comment_col,
            ),
            meta={"rows": int(len(df))},
        )
        return out

    def driver_stats(
        self,
        df: pd.DataFrame,
        *,
        dimension: str,
        score_col: str = "NPS",
        dataset_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """Driver table (cached) returned as a DataFrame."""
        from nps_lens.analytics.drivers import driver_table

        ds_sig = dataset_id or _df_sig_light(df)
        params = {"dimension": str(dimension), "score_col": str(score_col)}

        def _compute() -> pd.DataFrame:
            stats = driver_table(df, dimension=str(dimension), score_col=str(score_col))
            return pd.DataFrame([s.__dict__ for s in stats])

        out, _hit = self.cached(
            namespace="drivers",
            dataset_sig=ds_sig,
            params=params,
            compute=_compute,
            meta={"rows": int(len(df))},
        )
        return out
