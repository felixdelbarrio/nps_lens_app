"""Bounded, isolated Plotly rendering; no persistent Chromium process in the app."""

from __future__ import annotations

import base64
import contextlib
import json
import os
import signal
import subprocess
from pathlib import Path

import plotly.graph_objects as go
import plotly.io as pio

RENDER_TIMEOUT_SECONDS = 15


def render_png(fig: go.Figure, width: int, height: int) -> bytes:
    scope = pio.kaleido.scope
    if scope is None:
        raise RuntimeError("El motor de gráficos no está disponible.")
    args = scope._build_proc_args()
    executable = Path(args[0])
    native = executable.parent / "bin" / ("kaleido.exe" if os.name == "nt" else "kaleido")
    if native.is_file():
        args[0] = str(native)
    payload = (
        pio.to_json(
            {"data": fig.to_dict(), "format": "png", "width": width, "height": height, "scale": 1},
            validate=False,
            remove_uids=False,
        ).encode()
        + b"\n"
    )
    with subprocess.Popen(
        args,
        cwd=executable.parent,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=os.name != "nt",
    ) as process:
        try:
            output, _ = process.communicate(payload, timeout=RENDER_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            else:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
            process.kill()
            process.communicate()
            raise
    responses = [json.loads(line) for line in output.splitlines() if line.strip()]
    if process.returncode or len(responses) != 2 or any(row.get("code", 0) for row in responses):
        raise RuntimeError("No se pudo renderizar el gráfico.")
    return base64.b64decode(responses[1]["result"], validate=True)
