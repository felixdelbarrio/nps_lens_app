from __future__ import annotations

import argparse
import importlib
import inspect
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

import uvicorn

DEFAULT_PORT = 8617
STARTUP_TIMEOUT_SECONDS = 90
HEALTH_PATH = "/api/health"


def _runtime_app_home() -> Path:
    app_home_raw = str(os.environ.get("NPS_LENS_APP_HOME", "")).strip()
    if app_home_raw:
        return Path(app_home_raw).expanduser()
    return Path.home() / ".nps-lens"


def _normalize_working_directory_for_frozen() -> None:
    if not getattr(sys, "frozen", False):
        return
    keep_cwd = str(os.environ.get("NPS_LENS_KEEP_CWD", "")).strip().lower()
    if keep_cwd in {"1", "true", "yes"}:
        return
    app_home = _runtime_app_home()
    try:
        app_home.mkdir(parents=True, exist_ok=True)
        os.chdir(str(app_home))
    except Exception:
        pass


def _resource_root() -> Path:
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            return Path(str(meipass))
    return Path(__file__).resolve().parents[2]


def _frontend_dist_path() -> Path:
    env_dist = str(os.environ.get("NPS_LENS_FRONTEND_DIST_DIR", "")).strip()
    if env_dist:
        return Path(env_dist).expanduser().resolve()

    candidates = [
        _resource_root() / "frontend" / "dist",
        _resource_root() / "dist",
        Path.cwd() / "frontend" / "dist",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return candidates[0].resolve()


def _logo_path() -> Optional[Path]:
    env_icon = str(os.environ.get("NPS_LENS_ICON", "")).strip()
    if env_icon:
        env_candidate = Path(env_icon).expanduser()
        if not env_candidate.is_absolute():
            base = _resource_root() if getattr(sys, "frozen", False) else Path.cwd()
            env_candidate = (base / env_candidate).resolve()
        if env_candidate.exists():
            return env_candidate
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
        pass


def _run_api_server(port: int) -> None:
    frontend_dist_dir = _frontend_dist_path()
    if not (frontend_dist_dir / "index.html").exists():
        raise FileNotFoundError(
            f"Frontend dist not found at {frontend_dist_dir}. Run the frontend build first."
        )
    os.environ["NPS_LENS_FRONTEND_DIST_DIR"] = str(frontend_dist_dir)
    uvicorn.run(
        "nps_lens.api.app:create_app",
        factory=True,
        host="127.0.0.1",
        port=port,
        log_level=str(os.environ.get("NPS_LENS_LOG_LEVEL", "info")).lower(),
    )


def _server_cmd(port: int) -> list[str]:
    if getattr(sys, "frozen", False):
        return [sys.executable, "--internal-server", "--port", str(port)]
    return [sys.executable, "-m", "nps_lens.desktop", "--internal-server", "--port", str(port)]


def _wait_for_server_with_process(
    proc: subprocess.Popen[bytes], port: int, timeout_seconds: int = STARTUP_TIMEOUT_SECONDS
) -> None:
    deadline = time.time() + timeout_seconds
    url = f"http://127.0.0.1:{port}{HEALTH_PATH}"
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                "Embedded API server exited before startup completed. "
                f"Exit code: {proc.returncode}."
            )
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
            time.sleep(0.2)
    raise TimeoutError(f"Timed out waiting for API server at {url}")


def _terminate_process(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()


def _is_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.25)
        return sock.connect_ex(("127.0.0.1", int(port))) == 0


def _list_listen_pids(port: int) -> list[int]:
    if os.name == "nt":
        return []
    try:
        completed = subprocess.run(
            ["lsof", f"-tiTCP:{int(port)}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return []

    pids: list[int] = []
    for line in str(completed.stdout or "").splitlines():
        try:
            pids.append(int(line.strip()))
        except Exception:
            continue
    return pids


def _pid_command(pid: int) -> str:
    if os.name == "nt":
        return ""
    try:
        completed = subprocess.run(
            ["ps", "-p", str(int(pid)), "-o", "command="],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return ""
    return str(completed.stdout or "").strip()


def _pid_is_nps_lens_instance(pid: int) -> bool:
    cmd = _pid_command(pid).lower()
    if not cmd:
        return False
    markers = [
        "nps_lens.desktop",
        "--internal-server",
        "nps_lens_app",
        "127.0.0.1:8617",
        "nps_lens.cli serve",
    ]
    return any(marker in cmd for marker in markers)


def _kill_pid(pid: int, *, timeout_seconds: float = 3.0) -> None:
    if pid <= 0:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception:
        return
    deadline = time.time() + float(timeout_seconds)
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
            time.sleep(0.1)
        except Exception:
            return
    try:
        os.kill(pid, signal.SIGKILL)
    except Exception:
        return


def _reclaim_port_from_previous_instance(port: int) -> None:
    if not _is_port_open(port):
        return

    pids = [pid for pid in _list_listen_pids(port) if pid != os.getpid()]
    for pid in pids:
        if _pid_is_nps_lens_instance(pid):
            _kill_pid(pid)

    if _is_port_open(port):
        raise RuntimeError(
            f"Port {port} is already in use by another process. "
            "Close that process or change NPS_LENS_PORT."
        )


def _run_desktop(port: int) -> None:
    import webview

    _reclaim_port_from_previous_instance(port)
    proc = subprocess.Popen(_server_cmd(port))
    try:
        _wait_for_server_with_process(proc, port)
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
        help=f"Port for the embedded API server (default: {DEFAULT_PORT})",
    )
    return parser.parse_args()


def main() -> None:
    _normalize_working_directory_for_frozen()
    args = _parse_args()

    if args.internal_server:
        _run_api_server(args.port)
        return

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    _run_desktop(args.port)


if __name__ == "__main__":
    main()
