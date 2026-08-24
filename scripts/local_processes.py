from __future__ import annotations

import argparse
import os
import signal
import subprocess
import time
from pathlib import Path

DEFAULT_PORTS = (8617, 5173, 8625)


def _listener_pids(port: int) -> set[int]:
    result = subprocess.run(
        ["lsof", "-tiTCP:%d" % port, "-sTCP:LISTEN"],
        capture_output=True,
        check=False,
        text=True,
    )
    return {int(value) for value in result.stdout.split() if value.isdigit()}


def _process_details(pid: int) -> tuple[str, str]:
    command = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        capture_output=True,
        check=False,
        text=True,
    ).stdout.strip()
    cwd_output = subprocess.run(
        ["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"],
        capture_output=True,
        check=False,
        text=True,
    ).stdout.splitlines()
    cwd = next((line[1:] for line in cwd_output if line.startswith("n")), "")
    return command, cwd


def _belongs_to_project(pid: int, root: Path) -> bool:
    command, cwd = _process_details(pid)
    root_text = str(root.resolve())
    safe_markers = ("nps_lens", "nps-lens", "vite", "serve_webapp.py")
    return cwd.startswith(root_text) or (
        any(marker in command for marker in safe_markers) and root_text in command
    )


def stop_project_processes(root: Path, ports: tuple[int, ...]) -> list[int]:
    candidates = {pid for port in ports for pid in _listener_pids(port)}
    targets = sorted(pid for pid in candidates if _belongs_to_project(pid, root))
    for pid in targets:
        os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 3
    pending = set(targets)
    while pending and time.monotonic() < deadline:
        pending = (
            {pid for pid in pending if Path(f"/proc/{pid}").exists()}
            if Path("/proc").exists()
            else {
                pid
                for pid in pending
                if subprocess.run(["kill", "-0", str(pid)], capture_output=True).returncode == 0
            }
        )
        if pending:
            time.sleep(0.1)
    for pid in pending:
        os.kill(pid, signal.SIGKILL)
    return targets


def main() -> None:
    parser = argparse.ArgumentParser(description="Detiene únicamente procesos locales de NPS Lens.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--ports", nargs="*", type=int, default=list(DEFAULT_PORTS))
    args = parser.parse_args()
    stopped = stop_project_processes(args.root, tuple(dict.fromkeys(args.ports)))
    if stopped:
        print("NPS Lens detenido (PID: %s)." % ", ".join(map(str, stopped)))
    else:
        print("No había ninguna instancia de NPS Lens activa.")


if __name__ == "__main__":
    main()
