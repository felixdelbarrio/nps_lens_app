from __future__ import annotations

import html
import json
from dataclasses import dataclass
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

MAX_PUBLICATION_BYTES = 30 * 1024 * 1024


@dataclass(frozen=True)
class PublicationArtifact:
    file_name: str
    content: bytes
    size_bytes: int
    included_nps_rows: int
    included_helix_rows: int
    saved_path: str = ""


def _newsletter(publication: dict[str, object], report_name: str) -> bytes:
    screens = publication.get("screens", {})
    dashboard = screens.get("dashboard", {}) if isinstance(screens, dict) else {}
    kpis = dashboard.get("kpis", {}) if isinstance(dashboard, dict) else {}
    context = (
        html.escape(str(dashboard.get("context_label", "Periodo actualizado")))
        if isinstance(dashboard, dict)
        else "Periodo actualizado"
    )
    metrics = "".join(
        f'<td style="padding:16px;background:#f7f8f8"><small>{html.escape(label)}</small><div style="font-size:26px;font-weight:700">{html.escape(str(value))}</div></td>'
        for label, value in (
            ("Muestras", kpis.get("samples", "n/d")),
            ("NPS clásico", kpis.get("classic_nps", "n/d")),
            ("Score medio", kpis.get("nps_average", "n/d")),
        )
    )
    report_href = html.escape(report_name, quote=True)
    return f"""<!doctype html><html><body style="margin:0;background:#f7f8f8;font-family:Arial,sans-serif;color:#070e46">
<table role="presentation" width="100%"><tr><td align="center"><table role="presentation" width="680" style="max-width:100%;background:#fff">
<tr><td style="background:#070e46;color:#fff;padding:32px"><h1 style="margin:0">NPS Lens</h1><div style="color:#85c8ff">BBVA Banca de Empresas e Instituciones · {context}</div></td></tr>
<tr><td style="padding:28px"><h2>La señal del cliente, conectada con la operación</h2><p>Consulta la edición actualizada para navegar el análisis completo y accede a la presentación exclusiva.</p>
<table role="presentation" width="100%"><tr>{metrics}</tr></table><p><a href="WEBAPP_URL" style="display:inline-block;background:#001391;color:#fff;text-decoration:none;padding:13px 18px">Abrir NPS Lens</a>
<a href="{report_href}" style="display:inline-block;color:#001391;padding:13px 18px">Abrir presentación</a></p></td></tr></table></td></tr></table></body></html>""".encode(
        "utf-8"
    )


def _archive(publication: dict[str, object], *, report_name: str, report_content: bytes) -> bytes:
    target = BytesIO()
    with ZipFile(target, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
        archive.writestr(
            "publication.json",
            json.dumps(publication, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        )
        archive.writestr("newsletter.html", _newsletter(publication, report_name))
        archive.writestr(report_name, report_content)
    return target.getvalue()


def build_publication_archive(
    publication: dict[str, object],
    *,
    report_name: str,
    report_content: bytes,
    file_name: str,
    max_bytes: int = MAX_PUBLICATION_BYTES,
) -> PublicationArtifact:
    screens = publication.setdefault("screens", {})
    data = screens.setdefault("data", {}) if isinstance(screens, dict) else {}
    nps = data.setdefault("nps", {}) if isinstance(data, dict) else {}
    helix = data.setdefault("helix", {}) if isinstance(data, dict) else {}
    nps_rows = nps.get("rows", []) if isinstance(nps, dict) else []
    helix_rows = helix.get("rows", []) if isinstance(helix, dict) else []
    if not isinstance(nps_rows, list) or not isinstance(helix_rows, list):
        raise ValueError("Las filas de la publicación deben ser listas.")
    original_nps, original_helix = len(nps_rows), len(helix_rows)
    content = _archive(publication, report_name=report_name, report_content=report_content)
    while len(content) > max_bytes and (nps_rows or helix_rows):
        target = nps_rows if len(nps_rows) >= len(helix_rows) else helix_rows
        del target[max(len(target) // 2, 1) :]
        content = _archive(publication, report_name=report_name, report_content=report_content)
    manifest = publication.setdefault("manifest", {})
    if isinstance(manifest, dict):
        manifest.update(
            {
                "size_budget_bytes": int(max_bytes),
                "data_rows": {
                    "nps": {"total": original_nps, "included": len(nps_rows)},
                    "helix": {"total": original_helix, "included": len(helix_rows)},
                },
                "truncated": len(nps_rows) < original_nps or len(helix_rows) < original_helix,
            }
        )
    content = _archive(publication, report_name=report_name, report_content=report_content)
    if len(content) > max_bytes:
        raise ValueError("La publicación supera el límite absoluto de 30 MB.")
    if isinstance(manifest, dict):
        manifest["size_bytes"] = len(content)
        content = _archive(publication, report_name=report_name, report_content=report_content)
    return PublicationArtifact(
        file_name=file_name,
        content=content,
        size_bytes=len(content),
        included_nps_rows=len(nps_rows),
        included_helix_rows=len(helix_rows),
    )
