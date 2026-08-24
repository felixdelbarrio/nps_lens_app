from __future__ import annotations

import argparse
import json
import re
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import cast
from zipfile import ZipFile

INCLUDE_RE = re.compile(r"<\?!=\s*include\(['\"]([^'\"]+)['\"]\)\s*\?>")


def _latest_publication(search_dirs: list[Path]) -> Path | None:
    candidates = [
        path
        for directory in search_dirs
        if directory.exists()
        for pattern in ("nps-lens-publicacion-*.zip", "nps-lens-snapshot-*.zip")
        for path in directory.glob(pattern)
    ]
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def _payload(path: Path | None) -> dict[str, object]:
    if path is None:
        return {
            "schema_version": "1.0",
            "generated_at": "",
            "screens": {"dashboard": {}, "linking": {}, "data": {}},
            "manifest": {"status": "La edición local todavía no se ha generado."},
        }
    with ZipFile(path) as archive:
        member = "publication.json" if "publication.json" in archive.namelist() else "snapshot.json"
        return cast(dict[str, object], json.loads(archive.read(member)))


def build_preview(source: Path, output: Path, payload: dict[str, object]) -> Path:
    index = (source / "Index.html").read_text(encoding="utf-8")
    index = INCLUDE_RE.sub(
        lambda match: (source / f"{match.group(1)}.html").read_text(encoding="utf-8"), index
    )
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    index = index.replace("<?!= publicationJson ?>", encoded)
    index = index.replace(
        "<?!= viewerJson ?>", '{"email":"local@bbva.com","isAdmin":true,"local":true}'
    )
    output.mkdir(parents=True, exist_ok=True)
    target = output / "index.html"
    target.write_text(index, encoding="utf-8")
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description="Previsualiza la WebApp de NPS Lens.")
    parser.add_argument("--port", type=int, default=8625)
    parser.add_argument("--source", type=Path, default=Path("webapp/apps-script"))
    parser.add_argument("--output", type=Path, default=Path("build/webapp-preview"))
    parser.add_argument("--publication", type=Path)
    args = parser.parse_args()
    publication = args.publication or _latest_publication(
        [Path.home() / "Downloads", Path("build")]
    )
    build_preview(args.source, args.output, _payload(publication))
    print(f"WebApp NPS Lens: http://127.0.0.1:{args.port}")
    if publication:
        print(f"Edición cargada: {publication}")
    handler = lambda *values, **kwargs: SimpleHTTPRequestHandler(  # noqa: E731
        *values, directory=str(args.output), **kwargs
    )
    ThreadingHTTPServer(("127.0.0.1", args.port), handler).serve_forever()


if __name__ == "__main__":
    main()
