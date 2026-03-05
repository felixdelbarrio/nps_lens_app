from pathlib import Path


def test_artifact_layout_is_deterministic(tmp_path: Path) -> None:
    from nps_lens.platform.artifacts import ensure_artifact_dirs

    ctx = {"service_origin": "BBVA México", "service_origin_n1": "Senda", "service_origin_n2": "SN2X"}
    p1 = ensure_artifact_dirs(out_root=tmp_path, dataset_id="abc123", pipeline_version="1.0.0", context=ctx)
    p2 = ensure_artifact_dirs(out_root=tmp_path, dataset_id="abc123", pipeline_version="1.0.0", context=ctx)

    assert p1.context_dir == p2.context_dir
    assert p1.insights_dir.exists()
    assert p1.kpis_path.name == "kpis.json"
    assert p1.manifest_path.name == "manifest.json"
