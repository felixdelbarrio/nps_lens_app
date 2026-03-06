import json
from pathlib import Path
from typing import Any

import pandas as pd

from nps_lens.core.store import DatasetContext, DatasetStore, HelixIncidentStore


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "Palanca": ["A", "B"],
            "Canal": ["WEB", "WEB"],
            "NPS": [9, 2],
            "ID": ["1", "2"],
        }
    )


def test_dataset_store_save_df_tolerates_parquet_failure(tmp_path: Path, monkeypatch: Any) -> None:
    store = DatasetStore(tmp_path / "store")
    ctx = DatasetContext(service_origin="BBVA México", service_origin_n1="ENTERPRISE WEB")

    def _fail_write(self: DatasetStore, df: pd.DataFrame, parquet_dir: Path) -> list[str]:
        parquet_dir.mkdir(parents=True, exist_ok=True)
        (parquet_dir / "partial.parquet").write_text("partial", encoding="utf-8")
        raise RuntimeError("boom")

    monkeypatch.setattr(DatasetStore, "_write_parquet_dataset", _fail_write)

    stored = store.save_df(ctx, _sample_df(), source="excel:test.xlsx")

    assert stored.path.exists()
    meta = json.loads(stored.meta_path.read_text(encoding="utf-8"))
    assert meta["parquet_dataset"]["partitioning"] == []
    assert not Path(meta["parquet_dataset"]["path"]).exists()


def test_helix_store_save_df_tolerates_parquet_failure(tmp_path: Path, monkeypatch: Any) -> None:
    store = HelixIncidentStore(tmp_path / "helix")
    ctx = DatasetContext(service_origin="BBVA México", service_origin_n1="ENTERPRISE WEB")

    def _fail_write(self: HelixIncidentStore, df: pd.DataFrame, parquet_dir: Path) -> list[str]:
        parquet_dir.mkdir(parents=True, exist_ok=True)
        (parquet_dir / "partial.parquet").write_text("partial", encoding="utf-8")
        raise RuntimeError("boom")

    monkeypatch.setattr(HelixIncidentStore, "_write_parquet_dataset", _fail_write)

    stored = store.save_df(ctx, _sample_df(), source="excel:helix.xlsx")

    assert stored.path.exists()
    meta = json.loads(stored.meta_path.read_text(encoding="utf-8"))
    assert meta["parquet_dataset"]["partitioning"] == []
    assert not Path(meta["parquet_dataset"]["path"]).exists()
