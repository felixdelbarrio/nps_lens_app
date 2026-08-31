from __future__ import annotations

import json
import re
from pathlib import Path
from typing import cast
from zipfile import ZipFile

INCLUDE_RE = re.compile(r"<\?!=\s*include\(['\"]([^'\"]+)['\"]\)\s*\?>")


def latest_publication(search_dirs: list[Path]) -> Path | None:
    candidates = [
        path
        for directory in search_dirs
        if directory.exists()
        for path in directory.glob("nps-lens-publicacion-*.zip")
    ]
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def load_publication(path: Path | None) -> dict[str, object]:
    if path is None:
        return {
            "schema_version": "1.0",
            "generated_at": "",
            "screens": {"dashboard": {}, "linking": {}, "data": {}},
            "manifest": {"status": "La edición local todavía no se ha generado."},
        }
    with ZipFile(path) as archive:
        return cast(dict[str, object], json.loads(archive.read("publication.json")))


def extract_report(publication: Path | None, output: Path, payload: dict[str, object]) -> str:
    output.mkdir(parents=True, exist_ok=True)
    for stale_report in output.glob("*.pptx"):
        stale_report.unlink()
    if publication is None:
        return ""
    manifest = payload.get("manifest", {})
    report_name = str(manifest.get("report", "")) if isinstance(manifest, dict) else ""
    if not report_name or Path(report_name).name != report_name:
        return ""
    with ZipFile(publication) as archive:
        if report_name not in archive.namelist():
            return ""
        (output / report_name).write_bytes(archive.read(report_name))
    return report_name


def build_preview(
    source: Path,
    output: Path,
    payload: dict[str, object],
    *,
    report_url: str = "",
) -> Path:
    index = (source / "Index.html").read_text(encoding="utf-8")
    index = INCLUDE_RE.sub(
        lambda match: (source / f"{match.group(1)}.html").read_text(encoding="utf-8"), index
    )
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    index = index.replace("<?!= publicationJson ?>", encoded)
    viewer = {
        "email": "local@bbva.com",
        "isAdmin": True,
        "local": True,
        "reportUrl": report_url,
        "selectedScopeKey": str(payload.get("scope", {}).get("key", "local"))
        if isinstance(payload.get("scope"), dict)
        else "",
        "publicationCatalog": [
            {
                "scopeKey": str(payload.get("scope", {}).get("key", "local")),
                "label": str(payload.get("scope", {}).get("label", "Edición local")),
            }
        ] if isinstance(payload.get("scope"), dict) else [],
        "administration": {
            "version": "local",
            "generatedAt": str(payload.get("generated_at", "")),
            "access": {"email": "local@bbva.com", "role": "admin"},
        },
    }
    index = index.replace(
        "<?!= viewerJson ?>", json.dumps(viewer, ensure_ascii=False, separators=(",", ":"))
    )
    index = index.replace("<?= adminBodyClass ?>", "is-admin")
    index = index.replace("<?= accessRole ?>", "admin")
    index = index.replace("<?= appVersion ?>", "local")
    output.mkdir(parents=True, exist_ok=True)
    target = output / "index.html"
    target.write_text(index, encoding="utf-8")
    return target
