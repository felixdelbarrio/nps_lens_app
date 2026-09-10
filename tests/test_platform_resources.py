from pathlib import Path

import nps_lens.platform.resources as resources


def test_resource_root_resolves_the_repository_in_source_mode() -> None:
    root = resources.resource_root()

    assert (root / "pyproject.toml").is_file()
    assert (root / "assets").is_dir()


def test_resource_root_uses_pyinstaller_bundle(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(resources.sys, "frozen", True, raising=False)
    monkeypatch.setattr(resources.sys, "_MEIPASS", str(tmp_path), raising=False)

    assert resources.resource_root() == tmp_path.resolve()
