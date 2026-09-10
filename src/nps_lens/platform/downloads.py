from __future__ import annotations

import contextlib
import os
import tempfile
from pathlib import Path
from typing import Optional


def persist_download(content: bytes, file_name: str, directory: Path) -> Path:
    """Persist an exported artifact atomically in the configured download directory."""
    safe_name = Path(file_name).name
    if not safe_name or safe_name != file_name:
        raise ValueError("El nombre del fichero de descarga no es válido.")

    target_directory = Path(directory).expanduser().resolve()
    target = target_directory / safe_name
    temporary: Optional[Path] = None
    try:
        target_directory.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=target_directory,
            prefix=f".{safe_name}.",
            suffix=".tmp",
        )
        temporary = Path(temporary_name)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            with contextlib.suppress(OSError):
                os.fsync(handle.fileno())
        temporary.replace(target)
        if not target.is_file() or target.stat().st_size != len(content):
            raise OSError("El fichero guardado está incompleto.")
        return target
    except OSError as exc:
        if temporary is not None:
            with contextlib.suppress(OSError):
                temporary.unlink()
        raise OSError(
            f"No se pudo guardar {safe_name} en la carpeta de descargas "
            f"{target_directory}: {exc}"
        ) from exc
