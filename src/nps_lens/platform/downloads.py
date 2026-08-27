from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import Iterable


def persist_download(content: bytes, file_name: str, directories: Iterable[Path]) -> Path:
    """Persist an exported artifact atomically in the first writable directory."""
    safe_name = Path(file_name).name
    if not safe_name or safe_name != file_name:
        raise ValueError("El nombre del fichero de descarga no es válido.")

    for directory in directories:
        target = directory / safe_name
        temporary = directory / f".{safe_name}.tmp"
        try:
            directory.mkdir(parents=True, exist_ok=True)
            with temporary.open("wb") as handle:
                handle.write(content)
                handle.flush()
                with contextlib.suppress(OSError):
                    os.fsync(handle.fileno())
            temporary.replace(target)
            if target.stat().st_size != len(content):
                raise OSError("La descarga persistida está incompleta.")
            return target
        except OSError:
            with contextlib.suppress(OSError):
                temporary.unlink()

    raise OSError("No se pudo guardar el fichero en ninguna ruta de descarga disponible.")
