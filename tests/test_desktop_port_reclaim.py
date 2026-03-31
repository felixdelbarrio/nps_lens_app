from __future__ import annotations

from pathlib import Path

import pytest

from nps_lens import desktop


def test_reclaim_port_noop_when_port_is_free(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(desktop, "_is_port_open", lambda _port: False)

    called = {"list": False}

    def _list_pids(_port: int) -> list[int]:
        called["list"] = True
        return []

    monkeypatch.setattr(desktop, "_list_listen_pids", _list_pids)
    desktop._reclaim_port_from_previous_instance(8617)

    assert called["list"] is False


def test_reclaim_port_kills_previous_nps_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    state = {"open": True}

    monkeypatch.setattr(desktop, "_is_port_open", lambda _port: state["open"])
    monkeypatch.setattr(desktop, "_list_listen_pids", lambda _port: [4242])
    monkeypatch.setattr(desktop, "_pid_is_nps_lens_instance", lambda _pid: True)

    killed: list[int] = []

    def _kill_pid(pid: int, *, timeout_seconds: float = 3.0) -> None:
        _ = timeout_seconds
        killed.append(pid)
        state["open"] = False

    monkeypatch.setattr(desktop, "_kill_pid", _kill_pid)

    desktop._reclaim_port_from_previous_instance(8617)
    assert killed == [4242]


def test_reclaim_port_raises_when_owner_is_not_nps_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(desktop, "_is_port_open", lambda _port: True)
    monkeypatch.setattr(desktop, "_list_listen_pids", lambda _port: [7777])
    monkeypatch.setattr(desktop, "_pid_is_nps_lens_instance", lambda _pid: False)
    monkeypatch.setattr(desktop, "_kill_pid", lambda _pid, timeout_seconds=3.0: None)

    with pytest.raises(RuntimeError, match="already in use"):
        desktop._reclaim_port_from_previous_instance(8617)


def test_normalize_working_directory_for_frozen_uses_app_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = tmp_path / "runtime-home"
    monkeypatch.setenv("NPS_LENS_APP_HOME", str(target))
    monkeypatch.delenv("NPS_LENS_KEEP_CWD", raising=False)
    monkeypatch.setattr(desktop.sys, "frozen", True, raising=False)

    chdir_calls: list[str] = []
    monkeypatch.setattr(desktop.os, "chdir", lambda p: chdir_calls.append(str(p)))

    desktop._normalize_working_directory_for_frozen()

    assert target.exists()
    assert chdir_calls == [str(target)]


def test_normalize_working_directory_for_frozen_can_be_skipped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("NPS_LENS_APP_HOME", str(tmp_path / "runtime-home"))
    monkeypatch.setenv("NPS_LENS_KEEP_CWD", "1")
    monkeypatch.setattr(desktop.sys, "frozen", True, raising=False)

    called = {"chdir": False}
    monkeypatch.setattr(desktop.os, "chdir", lambda _p: called.__setitem__("chdir", True))

    desktop._normalize_working_directory_for_frozen()

    assert called["chdir"] is False
