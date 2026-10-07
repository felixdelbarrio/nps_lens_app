import os
import subprocess
from unittest.mock import MagicMock

import plotly.graph_objects as go
import pytest

from nps_lens.reports import chart_renderer, executive_ppt


@pytest.mark.skipif(os.name == "nt", reason="POSIX process group cleanup")
def test_renderer_timeout_kills_process_group_and_reaps(monkeypatch):
    process = MagicMock()
    process.pid = 123
    process.__enter__.return_value = process
    process.communicate.side_effect = [subprocess.TimeoutExpired("kaleido", 15), (b"", None)]
    monkeypatch.setattr(chart_renderer.subprocess, "Popen", lambda *a, **kw: process)
    kill = MagicMock()
    monkeypatch.setattr(chart_renderer.os, "killpg", kill)
    with pytest.raises(subprocess.TimeoutExpired):
        chart_renderer.render_png(go.Figure(go.Bar(x=["A"], y=[1])), 960, 540)
    kill.assert_called_once_with(123, chart_renderer.signal.SIGKILL)
    process.kill.assert_called_once()
    assert process.communicate.call_count == 2


def test_failed_renderer_is_not_retried_and_fallback_is_cached(monkeypatch):
    monkeypatch.setattr(executive_ppt, "_RENDERER_AVAILABLE", True)
    monkeypatch.setattr(executive_ppt, "_FIGURE_PNG_CACHE", executive_ppt.OrderedDict())
    renderer = MagicMock(side_effect=subprocess.TimeoutExpired("kaleido", 15))
    monkeypatch.setattr(executive_ppt, "render_png", renderer)
    first = go.Figure(go.Bar(x=["A"], y=[1]))
    png = executive_ppt._figure_png(first)
    assert png.startswith(b"\x89PNG")
    assert executive_ppt._figure_png(first) is png
    assert executive_ppt._figure_png(go.Figure(go.Bar(x=["B"], y=[2]))).startswith(b"\x89PNG")
    renderer.assert_called_once()
