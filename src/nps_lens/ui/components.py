from __future__ import annotations

from html import escape

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
    st.markdown(html, unsafe_allow_html=True)


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


def impact_chain(items: list[object]) -> None:
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

    if len(items) == 1:
        item = items[0]
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
        linked_pairs = _safe_int(_value(item, "linked_pairs", 0), default=0)
        linked_incidents = _safe_int(
            _value(item, "linked_incidents", len(_value(item, "incident_examples", []))),
            default=0,
        )
        linked_comments = _safe_int(
            _value(item, "linked_comments", len(_value(item, "comment_examples", []))),
            default=0,
        )
        incident_examples = _value(item, "incident_examples", [])
        comment_examples = _value(item, "comment_examples", [])
        if not isinstance(incident_examples, list):
            incident_examples = [] if incident_examples in (None, "") else [incident_examples]
        if not isinstance(comment_examples, list):
            comment_examples = [] if comment_examples in (None, "") else [comment_examples]
        incident_examples = [str(v).strip() for v in incident_examples if str(v).strip()]
        comment_examples = [str(v).strip() for v in comment_examples if str(v).strip()]
        incidents_html = "".join([f"<li>{escape(str(v))}</li>" for v in incident_examples])
        comments_html = "".join([f"<li>{escape(str(v))}</li>" for v in comment_examples])
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
  </div>
  <div class="nps-spotlight-evidence">
    <section class="nps-impact-evidence">
      <div class="nps-impact-label">Evidencia Helix ({linked_incidents})</div>
      <ul>{incidents_html or '<li>Sin evidencia Helix visible</li>'}</ul>
    </section>
    <section class="nps-impact-evidence">
      <div class="nps-impact-label">Voz del Cliente ({linked_comments})</div>
      <ul>{comments_html or '<li>Sin evidencia VoC visible</li>'}</ul>
    </section>
  </div>
</article>
""",
            unsafe_allow_html=True,
        )
        return

    cards: list[str] = []
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
        incident_examples = _value(item, "incident_examples", [])
        comment_examples = _value(item, "comment_examples", [])
        if not isinstance(incident_examples, list):
            incident_examples = [] if incident_examples in (None, "") else [incident_examples]
        if not isinstance(comment_examples, list):
            comment_examples = [] if comment_examples in (None, "") else [comment_examples]
        incident_examples = [str(v).strip() for v in incident_examples if str(v).strip()]
        comment_examples = [str(v).strip() for v in comment_examples if str(v).strip()]
        incident_sample_count = len(incident_examples)
        comment_sample_count = len(comment_examples)
        helix_header = (
            f"Helix ({incident_sample_count} de {linked_incidents})"
            if incident_sample_count < linked_incidents
            else f"Helix ({incident_sample_count})"
        )
        voc_header = (
            f"VoC ({comment_sample_count} de {linked_comments})"
            if comment_sample_count < linked_comments
            else f"VoC ({comment_sample_count})"
        )
        incidents_html = ""
        comments_html = ""
        if incident_examples:
            incidents_html = "".join([f"<li>{escape(str(v))}</li>" for v in incident_examples])
            incidents_html = f"<div class='nps-impact-evidence'><div class='nps-impact-label'>{helix_header}</div><ul>{incidents_html}</ul></div>"
        if comment_examples:
            comments_html = "".join([f"<li>{escape(str(v))}</li>" for v in comment_examples])
            comments_html = f"<div class='nps-impact-evidence'><div class='nps-impact-label'>{voc_header}</div><ul>{comments_html}</ul></div>"
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
  {incidents_html}
  {comments_html}
</article>
"""
        )

    st.markdown(
        f"""
<div class="nps-impact-grid">
  {''.join(cards)}
</div>
""",
        unsafe_allow_html=True,
    )
