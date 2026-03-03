from __future__ import annotations

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
