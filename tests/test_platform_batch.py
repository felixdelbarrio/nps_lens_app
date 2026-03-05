import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import pandas as pd


def test_load_batch_config(tmp_path: Path) -> None:
    from nps_lens.platform.batch import load_batch_config

    cfg = {
        "runs": [
            {
                "excel_path": "x.xlsx",
                "service_origin": "BBVA México",
                "service_origin_n1": "Senda",
                "service_origin_n2": "SN2X",
                "top_k_packs": 3,
                "min_n": 10,
            }
        ]
    }
    p = tmp_path / "batch.json"
    p.write_text(json.dumps(cfg), encoding="utf-8")
    specs = load_batch_config(p)
    assert len(specs) == 1
    assert specs[0].service_origin_n2 == "SN2X"
    assert specs[0].top_k_packs == 3


def test_run_platform_batch_exports_artifacts(tmp_path: Path, monkeypatch: Any) -> None:
    from nps_lens.platform.batch import BatchRunSpec, run_platform_batch

    # ---- fakes ----
    @dataclass
    class _Res:
        df: pd.DataFrame
        issues: list

    df = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "NPS": [10, 2],
            "Palanca": ["A", "A"],
            "Subpalanca": ["B", "B"],
            "Canal": ["Senda", "Senda"],
            "Comment": ["ok", "mal"],
        }
    )

    def _fake_read_excel(*args: Any, **kwargs: Any) -> _Res:
        return _Res(df=df.copy(), issues=[])

    monkeypatch.setattr("nps_lens.platform.batch.read_nps_thermal_excel", _fake_read_excel)
    monkeypatch.setattr("nps_lens.platform.batch.read_incidents_csv", lambda *a, **k: _Res(df=pd.DataFrame(), issues=[]))
    monkeypatch.setattr("nps_lens.platform.batch.read_reviews_csv", lambda *a, **k: _Res(df=pd.DataFrame(), issues=[]))

    # rank_opportunities is used inside; return one stable opportunity
    @dataclass
    class _Opp:
        dimension: str
        value: str

    monkeypatch.setattr("nps_lens.analytics.opportunities.rank_opportunities", lambda *a, **k: [_Opp("Palanca", "A")])

    # causal best-effort: return a simple object with required fields
    @dataclass
    class _Causal:
        treatment: str
        effect: float
        p_value: float
        n: int
        method: str
        assumptions: list
        warnings: list

    monkeypatch.setattr(
        "nps_lens.analytics.causal.best_effort_ate_logit",
        lambda *a, **k: _Causal("Palanca=A", 0.1, 0.2, 2, "logit", [], []),
    )

    class _Store:
        def save_df(self, ctx: Any, df_in: pd.DataFrame, source: str) -> Any:
            return {"ok": True}

        def read_meta(self, ctx: Any) -> Dict[str, Any]:
            return {"dataset_id": "abc123", "pipeline_version": "test"}

    class _Perf:
        def snapshot(self) -> Dict[str, Any]:
            return {"events": [], "totals": {}}

    class _App:
        def __init__(self) -> None:
            self.perf = _Perf()

        def driver_stats(self, *a: Any, **k: Any) -> pd.DataFrame:
            return pd.DataFrame([{ "dimension": "Palanca", "value": "A", "n": 2 }])

        def routes(self, *a: Any, **k: Any) -> Any:
            return {"routes": []}

    spec = BatchRunSpec(excel_path=Path("x.xlsx"), service_origin="BBVA México", service_origin_n1="Senda")
    out = run_platform_batch(specs=[spec], store=_Store(), app=_App(), out_root=tmp_path)

    assert "runs" in out
    assert len(out["runs"]) == 1
    artifact_dir = Path(out["runs"][0]["artifact_dir"])
    assert (artifact_dir / "kpis.json").exists()
    assert (artifact_dir / "manifest.json").exists()
    assert (artifact_dir / "insights").exists()
