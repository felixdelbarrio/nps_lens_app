from __future__ import annotations

import os
import socket


def main() -> None:
    preferred = int(os.environ.get("APP_PORT", "8000"))
    with socket.socket() as sock:
        try:
            sock.bind(("127.0.0.1", preferred))
            port = preferred
        except OSError:
            sock.bind(("127.0.0.1", 0))
            port = int(sock.getsockname()[1])
    print(port)


if __name__ == "__main__":
    main()
