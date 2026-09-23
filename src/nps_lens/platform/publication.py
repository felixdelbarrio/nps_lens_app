from __future__ import annotations

import html
import json
import math
from dataclasses import dataclass
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

MAX_PUBLICATION_BYTES = 30 * 1024 * 1024
MAX_PUBLICATION_JSON_BYTES = 20 * 1024 * 1024
PUBLICATION_SCHEMA_VERSION = "5.0"
DATA_PAGE_SIZE = 500
DATA_VISIBLE_COLUMNS = 14
# Keep evidence provenance visible even when raw exports have hundreds of columns.
DATA_PRIORITY_COLUMNS = {
    "nps": (
        "ID",
        "Fecha",
        "NPS",
        "NPS Group",
        "Canal",
        "Palanca",
        "Subpalanca",
        "Comment",
        "match_status",
        "_identity_source",
        "_source_id",
    ),
    "helix": (
        "Incident Number",
        "Record ID",
        "incident_occurred_at",
        "incident_occurred_at_source",
        "incident_registered_at",
        "_source_sheet",
        "Causal Match Eligible",
        "Causal Exclusion Reason",
        "summary",
        "Description",
        "BBVA_SourceServiceN1",
        "BBVA_SourceServiceN2",
    ),
}


def _visible_columns(kind: str, columns: list[str]) -> list[str]:
    priority = [column for column in DATA_PRIORITY_COLUMNS[kind] if column in columns]
    return (priority + [column for column in columns if column not in priority])[
        :DATA_VISIBLE_COLUMNS
    ]


@dataclass(frozen=True)
class PublicationArtifact:
    file_name: str
    content: bytes
    size_bytes: int
    included_nps_rows: int
    included_helix_rows: int
    saved_path: str = ""


def _dataset_page(
    rows: list[dict[str, object]],
    columns: list[str],
    *,
    page_size: int,
    total_rows: int,
) -> dict[str, object]:
    visible = columns[:DATA_VISIBLE_COLUMNS]
    return {
        "total_rows": total_rows,
        "rows": [{column: row.get(column) for column in visible} for row in rows[:page_size]],
    }


def build_static_data_snapshot(
    nps: dict[str, object],
    helix: dict[str, object],
    *,
    page_size: int = DATA_PAGE_SIZE,
) -> dict[str, object]:
    """Materializa localmente las páginas inmutables que consumirá la WebApp."""

    def source(value: dict[str, object]) -> tuple[list[str], list[dict[str, object]], int]:
        columns = [str(column) for column in value.get("columns", [])]
        rows = [row for row in value.get("rows", []) if isinstance(row, dict)]
        total_rows = int(value.get("total_rows", len(rows)) or 0)
        return columns, rows, max(total_rows, len(rows))

    nps_columns, nps_rows, nps_total = source(nps)
    helix_columns, helix_rows, helix_total = source(helix)
    nps_visible_columns = _visible_columns("nps", nps_columns)
    helix_visible_columns = _visible_columns("helix", helix_columns)
    return {
        "schema_version": PUBLICATION_SCHEMA_VERSION,
        "datasets": {
            "nps": {
                "columns": nps_visible_columns,
                "total_rows": nps_total,
                "page": _dataset_page(
                    nps_rows,
                    nps_visible_columns,
                    page_size=page_size,
                    total_rows=nps_total,
                ),
            },
            "helix": {
                "columns": helix_visible_columns,
                "total_rows": helix_total,
                "page": _dataset_page(
                    helix_rows,
                    helix_visible_columns,
                    page_size=page_size,
                    total_rows=helix_total,
                ),
            },
        },
    }


def _newsletter(publication: dict[str, object], report_name: str) -> bytes:
    model = publication.get("newsletter", {})
    if not isinstance(model, dict):
        model = {}

    def esc(value: object) -> str:
        return html.escape(str(value or ""))

    scorecard = [item for item in model.get("scorecard", []) if isinstance(item, dict)]
    quotes = [str(item) for item in model.get("quotes", []) if str(item).strip()]
    signals = [item for item in model.get("signals", []) if isinstance(item, dict)]
    metrics = "".join(
        '<td class="metric-cell" width="20%" valign="top" style="padding:12px 10px;border-top:3px solid #85c8ff;background:#f7f8f8;overflow-wrap:anywhere">'
        f'<div style="font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:#52627a">{esc(item.get("label"))}</div>'
        f'<div style="margin-top:6px;font:700 23px Georgia,serif;color:#001391">{esc(item.get("value"))}</div>'
        f'<div style="margin-top:5px;font-size:10px;color:#52627a">{esc(item.get("delta"))}</div></td>'
        for item in scorecard
    )
    quotes_html = "".join(
        f'<td class="quote-cell" width="50%" valign="top" style="padding:14px;background:#eaf3fa;border-left:3px solid #2dcccd;font:italic 16px Georgia,serif;color:#070e46;overflow-wrap:anywhere">“{esc(quote)}”</td>'
        for quote in quotes[:2]
    )
    signals_html = "".join(
        f'<tr><td style="padding:10px 0;border-bottom:1px solid #d3d8e0"><b style="color:#070e46">{esc(item.get("label"))}</b><br><span style="color:#52627a">{esc(item.get("reason"))}</span></td></tr>'
        for item in signals
    )
    report_href = html.escape(report_name, quote=True)
    return f"""<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body{{margin:0!important;padding:0!important;width:100%!important;background:#f4f6f8}}
td,th,p,h1,h2,div,a{{overflow-wrap:anywhere;word-break:normal}}img{{max-width:100%;height:auto}}
@media only screen and (max-width:600px){{
.email-shell-padding{{padding:0!important}}.email-card{{width:100%!important;max-width:100%!important}}
.email-hero{{padding:24px 20px!important}}.email-body{{padding:24px 18px!important}}
.email-title{{font-size:34px!important;line-height:1.05!important}}.email-headline{{font-size:27px!important;line-height:1.12!important}}
.metric-table,.metric-table tbody,.metric-table tr,.quote-table,.quote-table tbody,.quote-table tr{{display:block!important;width:100%!important;box-sizing:border-box!important}}
.metric-table,.quote-table{{border-spacing:0 8px!important}}.metric-cell,.quote-cell{{display:block!important;width:auto!important;margin:0 0 8px!important}}
.email-actions a{{display:block!important;margin:8px 0!important;text-align:center!important}}.email-body>table{{max-width:100%!important}}
}}
</style></head><body style="margin:0;background:#f4f6f8;font-family:Arial,sans-serif;color:#121f3f">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="width:100%;background:#f4f6f8"><tr>
<td class="email-shell-padding" align="center" style="padding:24px 10px">
<table class="email-card" role="presentation" width="680" cellspacing="0" cellpadding="0" style="width:100%;max-width:680px;background:#fff">
<tr><td class="email-hero" style="background:#071b9c;color:#fff;padding:34px 38px">
<div style="font-size:11px;letter-spacing:1px">{esc(model.get("brand") or "BBVA Banca de Empresas e Instituciones")}</div>
<h1 class="email-title" style="margin:22px 0 5px;font:700 42px Georgia,serif">{esc(model.get("product") or "NPS Lens")}</h1>
<div>{esc(model.get("promise") or "La voz del cliente conectada con la operación")}</div></td></tr>
<tr><td class="email-body" style="padding:32px 38px 20px"><div style="font-size:11px;letter-spacing:.7px;text-transform:uppercase;color:#004481">Lectura de 30 segundos · {esc(model.get("period") or "Periodo actualizado")}</div>
<h2 class="email-headline" style="margin:10px 0 0;font:700 31px Georgia,serif;line-height:1.12;color:#070e46">{esc(model.get("headline") or "La señal del cliente, conectada con la operación")}</h2>
<p style="font-size:16px;line-height:1.55;color:#30375f">{esc(model.get("lead") or "Consulta la edición actualizada y su presentación ejecutiva.")}</p>
<table class="metric-table" role="presentation" width="100%" cellspacing="6" style="margin:14px 0 22px"><tr>{metrics}</tr></table>
<h2 style="font:700 26px Georgia,serif;color:#070e46;margin:30px 0 12px">La voz del cliente</h2><table class="quote-table" role="presentation" width="100%" cellspacing="8"><tr>{quotes_html}</tr></table>
<h2 style="font:700 26px Georgia,serif;color:#070e46;margin:30px 0 4px">Señales a vigilar</h2><table role="presentation" width="100%">{signals_html}</table>
<p class="email-actions" style="margin-top:30px;padding-top:22px;border-top:1px solid #d3d8e0">
<a href="{report_href}" title="Abrir presentación ejecutiva" style="display:inline-block;background:#001391;color:#fff;text-decoration:none;padding:14px 20px;font-weight:700">Ver análisis completo</a>
<a href="WEBAPP_URL" style="display:inline-block;background:#004481;color:#fff;text-decoration:none;padding:14px 20px;font-weight:700">Explorar NPS Lens</a></p></td></tr>
</table></td></tr></table></body></html>""".encode(
        "utf-8"
    )


def _archive(
    publication: dict[str, object],
    *,
    report_name: str,
    report_content: bytes,
    compact_report_name: str,
    compact_report_content: bytes,
) -> bytes:
    target = BytesIO()
    with ZipFile(target, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
        archive.writestr(
            "publication.json",
            json.dumps(
                publication,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ),
        )
        archive.writestr("newsletter.html", _newsletter(publication, report_name))
        archive.writestr(report_name, report_content)
        archive.writestr(compact_report_name, compact_report_content)
    return target.getvalue()


def build_publication_archive(
    publication: dict[str, object],
    *,
    report_name: str,
    report_content: bytes,
    compact_report_name: str,
    compact_report_content: bytes,
    file_name: str,
    max_bytes: int = MAX_PUBLICATION_BYTES,
) -> PublicationArtifact:
    publication = _strict_json_value(publication)
    if not isinstance(publication, dict):
        raise TypeError("La publicación debe ser un objeto JSON.")
    snapshots = publication.get("snapshots", {})
    data = snapshots.get("data", {}) if isinstance(snapshots, dict) else {}
    datasets = data.get("datasets", {}) if isinstance(data, dict) else {}
    if data.get("schema_version") != PUBLICATION_SCHEMA_VERSION or not all(
        isinstance(datasets.get(kind), dict) for kind in ("nps", "helix")
    ):
        raise ValueError("La publicación debe incluir el snapshot estático local de NPS y Helix.")
    publication["snapshots"] = {"data": data}
    nps, helix = datasets["nps"], datasets["helix"]
    nps_total, helix_total = int(nps.get("total_rows", 0)), int(helix.get("total_rows", 0))
    nps_included = len((nps.get("page", {}) or {}).get("rows", []))
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
    publication_json_bytes = len(
        json.dumps(
            publication,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )
    if publication_json_bytes > MAX_PUBLICATION_JSON_BYTES:
        raise ValueError("El snapshot de publicación contiene más datos de los permitidos.")
    if isinstance(manifest, dict):
        manifest["publication_json_bytes"] = publication_json_bytes
    content = _archive(
        publication,
        report_name=report_name,
        report_content=report_content,
        compact_report_name=compact_report_name,
        compact_report_content=compact_report_content,
    )
    if len(content) > max_bytes:
        raise ValueError("La publicación estática supera el límite absoluto permitido.")
    if isinstance(manifest, dict):
        manifest["size_bytes"] = len(content)
        content = _archive(
            publication,
            report_name=report_name,
            report_content=report_content,
            compact_report_name=compact_report_name,
            compact_report_content=compact_report_content,
        )
    return PublicationArtifact(
        file_name=file_name,
        content=content,
        size_bytes=len(content),
        included_nps_rows=nps_included,
        included_helix_rows=helix_included,
    )


def _strict_json_value(value: object) -> object:
    """Replace non-finite floats recursively at the publication boundary."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): _strict_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_strict_json_value(item) for item in value]
    return value
