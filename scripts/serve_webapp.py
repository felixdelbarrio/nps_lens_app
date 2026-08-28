from __future__ import annotations

import argparse
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from nps_lens.platform.webapp_preview import (
    build_preview,
    extract_report,
    latest_publication,
    load_publication,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Previsualiza la WebApp de NPS Lens.")
    parser.add_argument("--port", type=int, default=8625)
    parser.add_argument("--source", type=Path, default=Path("webapp/apps-script"))
    parser.add_argument("--output", type=Path, default=Path("build/webapp-preview"))
    parser.add_argument("--publication", type=Path)
    args = parser.parse_args()
    publication = args.publication or latest_publication([Path.home() / "Downloads", Path("build")])
    payload = load_publication(publication)
    report_url = extract_report(publication, args.output, payload)
    build_preview(args.source, args.output, payload, report_url=report_url)
    print(f"WebApp NPS Lens: http://127.0.0.1:{args.port}")
    if publication:
        print(f"Edición cargada: {publication}")
    handler = lambda *values, **kwargs: SimpleHTTPRequestHandler(  # noqa: E731
        *values, directory=str(args.output), **kwargs
    )
    ThreadingHTTPServer(("127.0.0.1", args.port), handler).serve_forever()


if __name__ == "__main__":
    main()
