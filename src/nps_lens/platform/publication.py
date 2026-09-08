from __future__ import annotations

import html
import json
import math
from dataclasses import dataclass
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

MAX_PUBLICATION_BYTES = 30 * 1024 * 1024
MAX_PUBLICATION_JSON_BYTES = 20 * 1024 * 1024
PUBLICATION_SCHEMA_VERSION = "3.0"
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
    nps_visible_columns = nps_columns[:DATA_VISIBLE_COLUMNS]
    helix_visible_columns = helix_columns[:DATA_VISIBLE_COLUMNS]
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
    screens = publication.get("screens", {})
    dashboard = screens.get("dashboard", {}) if isinstance(screens, dict) else {}
    linking = screens.get("linking", {}) if isinstance(screens, dict) else {}
    kpis = dashboard.get("kpis", {}) if isinstance(dashboard, dict) else {}
    context = (
        html.escape(str(dashboard.get("context_label", "Periodo actualizado")))
        if isinstance(dashboard, dict)
        else "Periodo actualizado"
    )
    scenario_block = linking.get("scenarios", {}) if isinstance(linking, dict) else {}
    cards = scenario_block.get("cards", []) if isinstance(scenario_block, dict) else []
    scenario = cards[0] if isinstance(cards, list) and cards and isinstance(cards[0], dict) else {}
    situation = linking.get("situation", {}) if isinstance(linking, dict) else {}
    narrative = situation.get("narrative", {}) if isinstance(situation, dict) else {}
    headline = html.escape(
        str(narrative.get("title") or "La señal del cliente, conectada con la operación")
    )
    lead = html.escape(
        str(
            scenario.get("statement")
            or narrative.get("summary")
            or "Consulta la edición actualizada y su presentación ejecutiva."
        )
    )
    scenario_title = html.escape(str(scenario.get("title") or "Sin evidencia vinculada"))
    metrics = "".join(
        '<td width="33.33%" style="padding:14px 12px;border-top:3px solid #5ac4ff;'
        'background:#fff"><div style="font-size:11px;letter-spacing:.6px;text-transform:'
        f'uppercase;color:#5b638a">{html.escape(label)}</div><div style="margin-top:5px;'
        f'font:700 25px Georgia,serif;color:#071b9c">{html.escape(str(value))}</div></td>'
        for label, value in (
            ("Comentarios", kpis.get("samples", "n/d")),
            ("NPS clásico", kpis.get("classic_nps", "n/d")),
            ("Score medio", kpis.get("nps_average", "n/d")),
        )
    )
    report_href = html.escape(report_name, quote=True)
    return f"""<!doctype html><html><body style="margin:0;background:#f4f5f6;font-family:Arial,sans-serif;color:#071b9c">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr><td align="center" style="padding:24px 10px"><table role="presentation" width="680" cellspacing="0" cellpadding="0" style="max-width:100%;background:#fff">
<tr><td style="background:#071b9c;color:#fff;padding:34px 38px">
<div style="font-size:14px;letter-spacing:.4px">BBVA Banca de Empresas e Instituciones · {context}</div>
<h1 style="margin:48px 0 4px;font:700 44px Georgia,serif;line-height:1.02">Análisis NPS<br>e incidencias</h1></td></tr>
<tr><td style="padding:32px 38px 18px"><h2 style="margin:0;font:700 30px Georgia,serif;line-height:1.12">{headline}</h2><p style="font-size:16px;line-height:1.55;color:#30375f">{lead}</p>
<table role="presentation" width="100%" cellspacing="10" style="margin:12px -10px 22px"><tr>{metrics}</tr></table>
<div style="padding:18px 20px;background:#81c7f5">
<div style="font-size:11px;letter-spacing:.7px;text-transform:uppercase">Evidencia observada</div>
<div style="margin-top:5px;font:700 20px Georgia,serif">{scenario_title}</div></div>
<p style="margin:26px 0 8px"><a href="WEBAPP_URL" style="display:inline-block;background:#071b9c;color:#fff;text-decoration:none;padding:14px 20px;font-weight:700">Abrir NPS Lens</a>
<a href="{report_href}" style="display:inline-block;color:#071b9c;padding:14px 20px;font-weight:700">Abrir presentación ejecutiva</a></p></td></tr>
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
