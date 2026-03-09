from __future__ import annotations

from html import escape
from typing import Callable, Optional
from urllib.parse import quote

import pandas as pd
import streamlit as st


def card(title: str, body_html: str, *, flat: bool = False) -> None:
    klass = "nps-card nps-card--flat" if flat else "nps-card"
    st.markdown(
        f"""
<div class="{klass}">
  <div class="nps-muted"
       style="font-size:12px; font-weight:700; text-transform:uppercase;
              letter-spacing:.08em;">
    {title}
  </div>
  <div style="height:10px"></div>
  {body_html}
</div>
""",
        unsafe_allow_html=True,
    )


def kpi(label: str, value_html: str, *, hint: str = "") -> None:
    hint_html = f"<div class='nps-muted' style='font-size:12px'>{hint}</div>" if hint else ""
    card(
        label,
        f"""
<div class="nps-kpi">{value_html}</div>
{hint_html}
""",
        flat=True,
    )


def section(title: str, subtitle: str = "") -> None:
    st.markdown(
        f"""
<div style="margin: 10px 0 12px 0;">
  <div style="font-size: 22px; font-weight: 800;">{title}</div>
  <div class="nps-muted" style="margin-top:4px;">{subtitle}</div>
</div>
""",
        unsafe_allow_html=True,
    )


def pills(items: list[str]) -> None:
    if not items:
        return
    html = "".join([f"<span class='nps-pill'>{i}</span> " for i in items])
    st.markdown(f"<div class='nps-pill-row'>{html}</div>", unsafe_allow_html=True)


def executive_banner(
    *,
    kicker: str,
    title: str,
    summary: str,
    metrics: list[tuple[str, str]] | None = None,
) -> None:
    metric_html = ""
    if metrics:
        metric_html = "".join(
            [
                (
                    "<div class='nps-hero-metric'>"
                    f"<span>{escape(str(label))}</span>"
                    f"<strong>{escape(str(value))}</strong>"
                    "</div>"
                )
                for label, value in metrics
            ]
        )
        metric_html = f"<div class='nps-hero-metrics'>{metric_html}</div>"

    st.markdown(
        f"""
<section class="nps-hero">
  <div class="nps-hero-kicker">{escape(kicker)}</div>
  <h3>{escape(title)}</h3>
  <p>{escape(summary)}</p>
  {metric_html}
</section>
""",
        unsafe_allow_html=True,
    )


def impact_chain(
    items: list[object],
    *,
    extra_tabs: Optional[list[tuple[str, Callable[[], None]]]] = None,
) -> None:
    if not items:
        return

    def _value(item: object, key: str, default: object = "") -> object:
        if isinstance(item, dict):
            return item.get(key, default)
        return getattr(item, key, default)

    def _fmt_pct(value: object) -> str:
        try:
            f = float(value)
        except Exception:
            return "n/d"
        return "n/d" if f != f else f"{f*100:.0f}%"

    def _fmt_num(value: object, *, signed: bool = False) -> str:
        try:
            f = float(value)
        except Exception:
            return "n/d"
        if f != f:
            return "n/d"
        return f"{f:+.1f}" if signed else f"{f:.2f}"

    def _safe_int(value: object, default: int = 0) -> int:
        try:
            return int(float(value))
        except Exception:
            return int(default)

    def _normalize_examples(value: object) -> list[str]:
        if isinstance(value, list):
            values = value
        elif value in (None, ""):
            values = []
        else:
            values = [value]
        return [str(v).strip() for v in values if str(v).strip()]

    def _widget_key(item: object) -> str:
        raw = str(
            _value(
                item,
                "chain_key",
                f"{_value(item, 'rank', '')}-{_value(item, 'title', _value(item, 'nps_topic', ''))}",
            )
        )
        normalized = "".join(ch.lower() if ch.isalnum() else "-" for ch in raw)
        normalized = "-".join([part for part in normalized.split("-") if part])
        return normalized[:48] or "impact-chain"

    def _normalize_incident_records(item: object) -> list[dict[str, str]]:
        raw = _value(item, "incident_records", [])
        records: list[dict[str, str]] = []
        if isinstance(raw, list):
            for entry in raw:
                if isinstance(entry, dict):
                    incident_id = str(entry.get("incident_id", "") or "").strip()
                    summary = str(entry.get("summary", "") or "").strip()
                    url = str(entry.get("url", "") or "").strip()
                    if incident_id or summary:
                        records.append(
                            {
                                "incident_id": incident_id,
                                "summary": summary,
                                "url": url,
                            }
                        )
        if records:
            return records
        examples = _normalize_examples(_value(item, "incident_examples", []))
        fallback: list[dict[str, str]] = []
        for entry in examples:
            txt = str(entry).strip()
            incident_id = ""
            summary = txt
            if ":" in txt:
                maybe_id, maybe_summary = txt.split(":", 1)
                if maybe_id.strip().upper().startswith("INC"):
                    incident_id = maybe_id.strip()
                    summary = maybe_summary.strip()
            fallback.append(
                {
                    "incident_id": incident_id,
                    "summary": summary,
                    "url": "",
                }
            )
        return fallback

    def _normalize_comment_records(item: object) -> list[dict[str, str]]:
        raw = _value(item, "comment_records", [])
        records: list[dict[str, str]] = []
        if isinstance(raw, list):
            for entry in raw:
                if not isinstance(entry, dict):
                    continue
                comment = str(entry.get("comment", "") or "").strip()
                if not comment:
                    continue
                records.append(
                    {
                        "comment_id": str(entry.get("comment_id", "") or "").strip(),
                        "date": str(entry.get("date", "") or "").strip(),
                        "nps": str(entry.get("nps", "") or "").strip(),
                        "group": str(entry.get("group", "") or "").strip(),
                        "palanca": str(entry.get("palanca", "") or "").strip(),
                        "subpalanca": str(entry.get("subpalanca", "") or "").strip(),
                        "comment": comment,
                    }
                )
        if records:
            return records

        fallback: list[dict[str, str]] = []
        for idx, text in enumerate(
            _normalize_examples(_value(item, "comment_examples", [])), start=1
        ):
            fallback.append(
                {
                    "comment_id": str(idx),
                    "date": "",
                    "nps": "",
                    "group": "",
                    "palanca": "",
                    "subpalanca": "",
                    "comment": text,
                }
            )
        return fallback

    def _is_clickable_url(value: str) -> bool:
        txt = str(value or "").strip()
        return txt.startswith("http://") or txt.startswith("https://") or txt.startswith("file://")

    def _render_voc_record_cards(records: list[dict[str, str]], *, empty_label: str) -> None:
        if not records:
            st.info(empty_label)
            return
        cards_html = "".join(
            [
                (
                    "<article class='nps-evidence-card'>"
                    "<div class='nps-pill-row'>"
                    f"<span class='nps-pill'>ID: {escape(record.get('comment_id') or '-')}</span>"
                    f"<span class='nps-pill'>Fecha: {escape(record.get('date') or '-')}</span>"
                    f"<span class='nps-pill'>NPS: {escape(record.get('nps') or '-')}</span>"
                    f"<span class='nps-pill'>Grupo: {escape(record.get('group') or '-')}</span>"
                    f"<span class='nps-pill'>Palanca: {escape(record.get('palanca') or '-')}</span>"
                    f"<span class='nps-pill'>Subpalanca: {escape(record.get('subpalanca') or '-')}</span>"
                    "</div>"
                    f"<p>{escape(record.get('comment') or '')}</p>"
                    "</article>"
                )
                for record in records
            ]
        )
        st.markdown(
            f"<div class='nps-evidence-grid'>{cards_html}</div>",
            unsafe_allow_html=True,
        )

    def _render_helix_record_cards(records: list[dict[str, str]], *, empty_label: str) -> None:
        if not records:
            st.info(empty_label)
            return
        cards_html = ""
        for record in records:
            incident_id = str(record.get("incident_id", "")).strip()
            summary = str(record.get("summary", "")).strip()
            url = str(record.get("url", "")).strip()
            id_html = escape(incident_id) if incident_id else "INC"
            if incident_id and _is_clickable_url(url):
                href = quote(url, safe=":/?&=%#@+,-._~")
                id_html = f"<a href='{href}' target='_blank'>{escape(incident_id)}</a>"
            cards_html += (
                "<article class='nps-evidence-card'>"
                f"<div class='nps-evidence-card-index'>{id_html}</div>"
                f"<p>{escape(summary)}</p>"
                "</article>"
            )
        st.markdown(
            f"<div class='nps-evidence-grid'>{cards_html}</div>",
            unsafe_allow_html=True,
        )

    def _render_evidence_table(items: list[str], *, column_label: str) -> None:
        if not items:
            st.info(f"Sin registros para {column_label.lower()}.")
            return
        table_df = pd.DataFrame(
            {
                "#": list(range(1, len(items) + 1)),
                column_label: items,
            }
        )
        st.dataframe(
            table_df,
            use_container_width=True,
            hide_index=True,
            height=min(420, 72 + len(items) * 42),
        )

    def _render_evidence_html_table(
        rows: list[dict[str, str]],
        *,
        columns: list[tuple[str, str]],
        empty_label: str,
    ) -> None:
        if not rows:
            st.info(empty_label)
            return
        head_html = "".join([f"<th>{escape(label)}</th>" for _, label in columns])
        body_html = ""
        for row in rows:
            cells = []
            for key, _label in columns:
                value = str(row.get(key, "") or "").strip()
                if key == "incident_id" and _is_clickable_url(
                    str(row.get("url", "") or "").strip()
                ):
                    href = quote(str(row.get("url", "") or "").strip(), safe=":/?&=%#@+,-._~")
                    cell_html = f"<a href='{href}' target='_blank'>{escape(value)}</a>"
                else:
                    cell_html = escape(value)
                cells.append(f"<td>{cell_html}</td>")
            body_html += f"<tr>{''.join(cells)}</tr>"
        st.markdown(
            (
                "<div class='nps-evidence-table-wrap'>"
                "<table class='nps-evidence-table'>"
                f"<thead><tr>{head_html}</tr></thead>"
                f"<tbody>{body_html}</tbody>"
                "</table>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    def _render_single_item(item: object, *, key_suffix: str = "") -> None:
        rank = escape(str(_value(item, "rank", "")))
        title = escape(str(_value(item, "title", _value(item, "nps_topic", ""))))
        touchpoint = escape(str(_value(item, "touchpoint", "")))
        palanca = escape(str(_value(item, "palanca", "")))
        subpalanca = escape(str(_value(item, "subpalanca", "")))
        statement = escape(str(_value(item, "statement", _value(item, "chain_story", ""))))
        focus_probability = _value(
            item,
            "focus_probability",
            _value(item, "detractor_probability", float("nan")),
        )
        nps_delta_expected = _value(item, "nps_delta_expected", float("nan"))
        total_nps_impact = _value(item, "total_nps_impact", 0.0)
        confidence = _value(item, "confidence", 0.0)
        priority = _value(item, "priority", float("nan"))
        nps_points_at_risk = _value(item, "nps_points_at_risk", float("nan"))
        nps_points_recoverable = _value(item, "nps_points_recoverable", float("nan"))
        owner_role = escape(str(_value(item, "owner_role", "n/d") or "n/d"))
        linked_pairs = _safe_int(_value(item, "linked_pairs", 0), default=0)
        linked_incidents = _safe_int(
            _value(item, "linked_incidents", len(_value(item, "incident_examples", []))),
            default=0,
        )
        linked_comments = _safe_int(
            _value(item, "linked_comments", len(_value(item, "comment_examples", []))),
            default=0,
        )
        incident_records = _normalize_incident_records(item)
        incident_examples = [
            str(record.get("summary", "")).strip()
            for record in incident_records
            if str(record.get("summary", "")).strip()
        ]
        comment_records = _normalize_comment_records(item)
        lever_label = " / ".join([v for v in [palanca, subpalanca] if v])
        st.markdown(
            f"""
<article class="nps-spotlight">
  <div class="nps-spotlight-head">
    <div>
      <div class="nps-spotlight-kicker">Cadena causal priorizada</div>
      <h3>{title}</h3>
      <p>{statement}</p>
    </div>
    <div class="nps-spotlight-rank">#{rank}</div>
  </div>
  <div class="nps-spotlight-flow">
    <span class="nps-impact-step">({linked_incidents}) Incidencias Helix</span>
    <span class="nps-impact-arrow">→</span>
    <span class="nps-impact-step">{touchpoint}</span>
    <span class="nps-impact-arrow">→</span>
    <span class="nps-impact-step">{lever_label or "Palanca / subpalanca"}</span>
    <span class="nps-impact-arrow">→</span>
    <span class="nps-impact-step">({linked_comments}) Comentarios VoC</span>
    <span class="nps-impact-arrow">→</span>
    <span class="nps-impact-step">Riesgo NPS</span>
  </div>
  <div class="nps-spotlight-metrics">
    <div class="nps-spotlight-metric"><span>Probabilidad foco</span><strong>{_fmt_pct(focus_probability)}</strong></div>
    <div class="nps-spotlight-metric"><span>Delta NPS</span><strong>{_fmt_num(nps_delta_expected, signed=True)}</strong></div>
    <div class="nps-spotlight-metric"><span>Impacto total</span><strong>{_fmt_num(total_nps_impact)} pts</strong></div>
    <div class="nps-spotlight-metric"><span>Confianza</span><strong>{_fmt_num(confidence)}</strong></div>
    <div class="nps-spotlight-metric"><span>Links validados</span><strong>{linked_pairs}</strong></div>
    <div class="nps-spotlight-metric"><span>Prioridad</span><strong>{_fmt_num(priority)}</strong></div>
    <div class="nps-spotlight-metric"><span>NPS en riesgo</span><strong>{_fmt_num(nps_points_at_risk)} pts</strong></div>
    <div class="nps-spotlight-metric"><span>NPS recuperable</span><strong>{_fmt_num(nps_points_recoverable)} pts</strong></div>
    <div class="nps-spotlight-metric"><span>Owner</span><strong>{owner_role}</strong></div>
  </div>
</article>
""",
            unsafe_allow_html=True,
        )
        evidence_key = f"{_widget_key(item)}{key_suffix}"
        ctl_col, meta_col = st.columns([1.4, 2.6])
        with ctl_col:
            evidence_view = st.radio(
                "Vista de evidencia",
                options=["Cards", "Tabla"],
                horizontal=True,
                key=f"nh_chain_evidence_view_{evidence_key}",
                label_visibility="collapsed",
            )
        with meta_col:
            st.markdown(
                (
                    "<div class='nps-evidence-toolbar-note'>"
                    f"<strong>{len(incident_examples)}</strong> evidencias Helix visibles · "
                    f"<strong>{len(comment_records)}</strong> comentarios VoC visibles"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
        tabs_spec: list[tuple[str, Callable[[], None]]] = []

        def _render_helix_tab() -> None:
            if evidence_view == "Tabla":
                _render_evidence_html_table(
                    incident_records,
                    columns=[("incident_id", "ID"), ("summary", "Evidencia Helix")],
                    empty_label="Sin evidencia Helix visible.",
                )
            else:
                _render_helix_record_cards(
                    incident_records,
                    empty_label="Sin evidencia Helix visible.",
                )

        def _render_voc_tab() -> None:
            if evidence_view == "Tabla":
                _render_evidence_html_table(
                    comment_records,
                    columns=[
                        ("comment_id", "ID"),
                        ("date", "Fecha"),
                        ("nps", "NPS"),
                        ("group", "Grupo"),
                        ("palanca", "Palanca"),
                        ("subpalanca", "Subpalanca"),
                        ("comment", "Comentario"),
                    ],
                    empty_label="Sin evidencia VoC visible.",
                )
            else:
                _render_voc_record_cards(
                    comment_records,
                    empty_label="Sin evidencia VoC visible.",
                )

        tabs_spec.append(
            (
                f"Evidencia Helix ({linked_incidents or len(incident_examples)})",
                _render_helix_tab,
            )
        )
        tabs_spec.append(
            (
                f"Voz del cliente ({linked_comments or len(comment_records)})",
                _render_voc_tab,
            )
        )
        if extra_tabs:
            tabs_spec.extend(extra_tabs)

        tabs = st.tabs([label for label, _ in tabs_spec])
        for tab, (_label, renderer) in zip(tabs, tabs_spec):
            with tab:
                renderer()

    if len(items) == 1:
        _render_single_item(items[0])
        return

    cards: list[str] = []
    summary_rows: list[dict[str, object]] = []
    for item in items:
        rank = escape(str(_value(item, "rank", "")))
        title = escape(str(_value(item, "title", _value(item, "nps_topic", ""))))
        touchpoint = escape(str(_value(item, "touchpoint", "")))
        statement = escape(str(_value(item, "statement", _value(item, "chain_story", ""))))
        focus_probability = _value(
            item,
            "focus_probability",
            _value(item, "detractor_probability", float("nan")),
        )
        nps_delta_expected = _value(item, "nps_delta_expected", float("nan"))
        total_nps_impact = _value(item, "total_nps_impact", 0.0)
        confidence = _value(item, "confidence", 0.0)
        linked_incidents = _safe_int(
            _value(item, "linked_incidents", len(_value(item, "incident_examples", []))),
            default=0,
        )
        linked_comments = _safe_int(
            _value(item, "linked_comments", len(_value(item, "comment_examples", []))),
            default=0,
        )
        incident_records = _normalize_incident_records(item)
        incident_examples = [
            str(record.get("summary", "")).strip()
            for record in incident_records
            if str(record.get("summary", "")).strip()
        ]
        comment_records = _normalize_comment_records(item)
        incident_sample_count = len(incident_examples)
        comment_sample_count = len(comment_records)
        linked_pairs = _safe_int(_value(item, "linked_pairs", 0), default=0)
        cards.append(
            f"""
<article class="nps-impact-card">
  <div class="nps-impact-head">
    <span class="nps-impact-rank">#{rank}</span>
    <span class="nps-impact-kicker">Impact Chain</span>
  </div>
  <h4>{title}</h4>
  <div class="nps-impact-flow">
    <span class="nps-impact-step">({incident_sample_count}) Incidencias</span>
    <span class="nps-impact-arrow">→</span>
    <span class="nps-impact-step">{touchpoint}</span>
    <span class="nps-impact-arrow">→</span>
    <span class="nps-impact-step">({comment_sample_count}) Comentarios VoC</span>
    <span class="nps-impact-arrow">→</span>
    <span class="nps-impact-step">Riesgo NPS</span>
  </div>
  <div class="nps-impact-metrics">
    <span>Prob. foco <strong>{_fmt_pct(focus_probability)}</strong></span>
    <span>Δ NPS <strong>{_fmt_num(nps_delta_expected, signed=True)}</strong></span>
    <span>Impacto <strong>{_fmt_num(total_nps_impact)} pts</strong></span>
    <span>Confianza <strong>{_fmt_num(confidence)}</strong></span>
  </div>
  <p>{statement}</p>
</article>
"""
        )
        summary_rows.append(
            {
                "Rank": _safe_int(_value(item, "rank", 0), default=0),
                "Cadena": str(_value(item, "title", _value(item, "nps_topic", ""))),
                "Touchpoint": str(_value(item, "touchpoint", "")),
                "Incidencias Helix": linked_incidents or incident_sample_count,
                "Comentarios VoC": linked_comments or comment_sample_count,
                "Links validados": linked_pairs,
                "Probabilidad foco": _fmt_pct(focus_probability),
                "Delta NPS": _fmt_num(nps_delta_expected, signed=True),
                "Impacto total": f"{_fmt_num(total_nps_impact)} pts",
                "Confianza": _fmt_num(confidence),
            }
        )

    summary_view = st.radio(
        "Vista de cadenas",
        options=["Cards", "Tabla"],
        horizontal=True,
        key="nh_multi_chain_summary_view",
        label_visibility="collapsed",
    )
    if summary_view == "Tabla":
        st.dataframe(
            pd.DataFrame(summary_rows),
            use_container_width=True,
            hide_index=True,
            height=min(420, 72 + len(summary_rows) * 42),
        )
    else:
        st.markdown(
            f"""
<div class="nps-impact-grid">
  {''.join(cards)}
</div>
""",
            unsafe_allow_html=True,
        )

    if len(items) > 1:
        st.markdown("#### Detalle por cadena")
        detail_tabs = st.tabs(
            [
                f"#{_safe_int(_value(item, 'rank', idx + 1), default=idx + 1)} {str(_value(item, 'title', _value(item, 'nps_topic', '')))}"
                for idx, item in enumerate(items)
            ]
        )
        for idx, (tab, item) in enumerate(zip(detail_tabs, items)):
            with tab:
                _render_single_item(item, key_suffix=f"-detail-{idx}")
