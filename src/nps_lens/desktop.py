from __future__ import annotations

import argparse
import importlib
import inspect
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

DEFAULT_PORT = 8617
STARTUP_TIMEOUT_SECONDS = 90
HEALTH_PATH = "/_stcore/health"


def _resource_root() -> Path:
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            return Path(str(meipass))
    return Path(__file__).resolve().parents[2]


def _app_script_path() -> Path:
    return _resource_root() / "app" / "streamlit_app.py"


def _logo_path() -> Optional[Path]:
    candidate = _resource_root() / "assets" / "logo.png"
    return candidate if candidate.exists() else None


def _set_macos_app_icon(icon_path: Optional[Path]) -> None:
    if not icon_path or sys.platform != "darwin":
        return
    try:
        appkit = importlib.import_module("AppKit")
        ns_image = appkit.NSImage
        ns_application = appkit.NSApplication
        image = ns_image.alloc().initWithContentsOfFile_(str(icon_path))
        if image:
            ns_application.sharedApplication().setApplicationIconImage_(image)
    except Exception:
        # Best-effort only: pyobjc/AppKit may be unavailable in some runtimes.
        pass


def _run_streamlit_server(port: int) -> None:
    from streamlit.web import bootstrap

    app_script = _app_script_path()
    if not app_script.exists():
        raise FileNotFoundError(f"Streamlit app script not found: {app_script}")

    flags = {
        "server.port": port,
        "server.address": "127.0.0.1",
        "server.headless": True,
        "server.fileWatcherType": "none",
        "server.runOnSave": False,
        "browser.gatherUsageStats": False,
    }
    bootstrap.run(str(app_script), False, [], flags)


def _server_cmd(port: int) -> list[str]:
    if getattr(sys, "frozen", False):
        return [sys.executable, "--internal-server", "--port", str(port)]
    return [sys.executable, "-m", "nps_lens.desktop", "--internal-server", "--port", str(port)]


def _wait_for_server(port: int, timeout_seconds: int = STARTUP_TIMEOUT_SECONDS) -> None:
    deadline = time.time() + timeout_seconds
    url = f"http://127.0.0.1:{port}{HEALTH_PATH}"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
            time.sleep(0.2)
    raise TimeoutError(f"Timed out waiting for Streamlit server at {url}")


def _terminate_process(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()


def _run_desktop(port: int) -> None:
    import webview

    proc = subprocess.Popen(_server_cmd(port))
    try:
        _wait_for_server(port)
        icon_path = _logo_path()
        _set_macos_app_icon(icon_path)
        window_kwargs: dict[str, Any] = {
            "title": "NPS Lens",
            "url": f"http://127.0.0.1:{port}",
            "width": 1440,
            "height": 920,
            "min_size": (1100, 700),
        }
        supports_icon = "icon" in inspect.signature(webview.create_window).parameters
        if icon_path and supports_icon:
            window_kwargs["icon"] = str(icon_path)
        webview.create_window(**window_kwargs)
        webview.start(debug=False)
    finally:
        _terminate_process(proc)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NPS Lens desktop launcher")
    parser.add_argument("--internal-server", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("NPS_LENS_PORT", DEFAULT_PORT)),
        help=f"Port for the embedded Streamlit server (default: {DEFAULT_PORT})",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.internal_server:
        _run_streamlit_server(args.port)
        return

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    _run_desktop(args.port)


if __name__ == "__main__":
    main()
