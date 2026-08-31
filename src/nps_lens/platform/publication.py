from __future__ import annotations

import html
import json
import unicodedata
from dataclasses import dataclass
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

MAX_PUBLICATION_BYTES = 30 * 1024 * 1024
PUBLICATION_SCHEMA_VERSION = "2.0"
DATA_PAGE_SIZE = 500
DATA_VISIBLE_COLUMNS = 14


@dataclass(frozen=True)
class PublicationArtifact:
    file_name: str
    content: bytes
    size_bytes: int
    included_nps_rows: int
    included_helix_rows: int
    saved_path: str = ""


def _filter_key(value: object) -> str:
    key = unicodedata.normalize("NFKD", str(value or "todos").strip().casefold())
    key = "".join(character for character in key if not unicodedata.combining(character))
    return {"detractores": "detractor", "promotores": "promotor", "neutros": "pasivo"}.get(
        key, key or "todos"
    )


def _dataset_page(
    rows: list[dict[str, object]], columns: list[str], *, page_size: int
) -> dict[str, object]:
    visible = columns[:DATA_VISIBLE_COLUMNS]
    return {
        "total_rows": len(rows),
        "rows": [{column: row.get(column) for column in visible} for row in rows[:page_size]],
    }


def build_static_data_snapshot(
    nps: dict[str, object],
    helix: dict[str, object],
    *,
    page_size: int = DATA_PAGE_SIZE,
) -> dict[str, object]:
    """Materializa localmente las páginas inmutables que consumirá la WebApp."""

    def source(value: dict[str, object]) -> tuple[list[str], list[dict[str, object]]]:
        columns = [str(column) for column in value.get("columns", [])]
        rows = [row for row in value.get("rows", []) if isinstance(row, dict)]
        return columns, rows

    nps_columns, nps_rows = source(nps)
    helix_columns, helix_rows = source(helix)
    channels = list(dict.fromkeys(["todos", *(_filter_key(row.get("Canal")) for row in nps_rows)]))
    groups = list(
        dict.fromkeys(["todos", *(_filter_key(row.get("NPS Group")) for row in nps_rows)])
    )
    pages = {
        f"{channel}|{group}": _dataset_page(
            [
                row
                for row in nps_rows
                if (channel == "todos" or _filter_key(row.get("Canal")) == channel)
                and (group == "todos" or _filter_key(row.get("NPS Group")) == group)
            ],
            nps_columns,
            page_size=page_size,
        )
        for channel in channels
        for group in groups
    }
    return {
        "schema_version": PUBLICATION_SCHEMA_VERSION,
        "datasets": {
            "nps": {"columns": nps_columns, "total_rows": len(nps_rows), "pages": pages},
            "helix": {
                "columns": helix_columns,
                "total_rows": len(helix_rows),
                "page": _dataset_page(helix_rows, helix_columns, page_size=page_size),
            },
        },
    }


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
<tr><td style="padding:28px"><h2>La señal del cliente, conectada con la operación</h2><p>Consulta la edición actualizada para navegar el análisis completo y accede a la presentación ejecutiva.</p>
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
    snapshots = publication.get("snapshots", {})
    data = snapshots.get("data", {}) if isinstance(snapshots, dict) else {}
    datasets = data.get("datasets", {}) if isinstance(data, dict) else {}
    if data.get("schema_version") != PUBLICATION_SCHEMA_VERSION or not all(
        isinstance(datasets.get(kind), dict) for kind in ("nps", "helix")
    ):
        raise ValueError("La publicación debe incluir el snapshot estático local de NPS y Helix.")
    nps, helix = datasets["nps"], datasets["helix"]
    nps_total, helix_total = int(nps.get("total_rows", 0)), int(helix.get("total_rows", 0))
    nps_included = len((nps.get("pages", {}).get("todos|todos", {}) or {}).get("rows", []))
    helix_included = len((helix.get("page", {}) or {}).get("rows", []))
    manifest = publication.setdefault("manifest", {})
    if isinstance(manifest, dict):
        manifest.update(
            {
                "size_budget_bytes": int(max_bytes),
                "data_rows": {
                    "nps": {"total": nps_total, "included": nps_included},
                    "helix": {"total": helix_total, "included": helix_included},
                },
                "truncated": nps_included < nps_total or helix_included < helix_total,
            }
        )
    content = _archive(publication, report_name=report_name, report_content=report_content)
    if len(content) > max_bytes:
        raise ValueError("La publicación estática supera el límite absoluto permitido.")
    if isinstance(manifest, dict):
        manifest["size_bytes"] = len(content)
        content = _archive(publication, report_name=report_name, report_content=report_content)
    return PublicationArtifact(
        file_name=file_name,
        content=content,
        size_bytes=len(content),
        included_nps_rows=nps_included,
        included_helix_rows=helix_included,
    )
