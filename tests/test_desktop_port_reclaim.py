from __future__ import annotations

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
