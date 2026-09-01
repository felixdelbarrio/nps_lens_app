from __future__ import annotations

import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "local_processes.py"
SPEC = importlib.util.spec_from_file_location("nps_lens_local_processes", MODULE_PATH)
assert SPEC and SPEC.loader
local_processes = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(local_processes)


def test_packaged_nps_lens_listener_is_recognized_outside_repository(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        local_processes,
        "_process_details",
        lambda _pid: ("/Applications/nps-lens.app/Contents/MacOS/nps-lens --internal-server", "/"),
    )

    assert local_processes._belongs_to_project(42, tmp_path)


def test_unrelated_listener_is_not_stopped(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        local_processes,
        "_process_details",
        lambda _pid: ("/usr/local/bin/vite", "/Users/example/another-project"),
    )

    assert not local_processes._belongs_to_project(42, tmp_path)
