from pathlib import Path

import pytest

from nps_lens.platform import downloads


def test_persist_download_writes_atomically_to_the_requested_directory(tmp_path: Path) -> None:
    target = downloads.persist_download(b"PPTX", "report.pptx", tmp_path / "downloads")

    assert target == (tmp_path / "downloads" / "report.pptx").resolve()
    assert target.read_bytes() == b"PPTX"
    assert list(target.parent.glob("*.tmp")) == []


def test_persist_download_does_not_hide_a_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_write(*args: object, **kwargs: object) -> tuple[int, str]:
        raise PermissionError("permission denied")

    monkeypatch.setattr(downloads.tempfile, "mkstemp", fail_write)

    with pytest.raises(OSError, match="carpeta de descargas"):
        downloads.persist_download(b"ZIP", "publication.zip", tmp_path / "downloads")


def test_persist_download_rejects_path_traversal(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="nombre del fichero"):
        downloads.persist_download(b"data", "../report.pptx", tmp_path)
