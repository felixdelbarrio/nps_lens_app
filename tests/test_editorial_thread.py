"""Editorial labels connect topic detection to a specific case without changing sources."""

import json
import subprocess
from datetime import date
from pathlib import Path

import pandas as pd

from nps_lens.analytics.text_mining import summarize_taxonomy
from nps_lens.domain.topic_labels import topic_paths
from nps_lens.reports.content_selectors import causal_scenario_title
from nps_lens.reports.executive_newsletter import build_executive_newsletter


def test_topic_paths_preserve_parents_counts_and_source_categories():
    frame = pd.DataFrame(
        {
            "Palanca": ["Acceso", "Acceso", "Pagos", " Uso ", None, "Consulta"],
            "Subpalanca": ["Error", "Error", "Error", "", "Token", "Consulta"],
            "Comment": [
                "No puedo entrar",
                "No puedo entrar",
                "No puedo pagar",
                "Error al usar",
                "Token vacío",
                "Falta información",
            ],
        }
    )
    original = frame.copy(deep=True)
    paths = topic_paths(frame)
    assert paths.tolist() == [
        "Acceso > Error",
        "Acceso > Error",
        "Pagos > Error",
        "Uso",
        "Token",
        "Consulta",
    ]
    topics = summarize_taxonomy(frame)
    counts = {item.top_terms[0]: item.n for item in topics}
    assert counts["Acceso > Error"] == 2 and counts["Pagos > Error"] == 1
    assert sum(counts.values()) == len(frame)
    pd.testing.assert_frame_equal(frame, original)


def test_case_title_keeps_the_anchor_and_the_concrete_task():
    row = {
        "anchor_topic": "Acceso > Token",
        "nps_topic": "Activar token",
        "affected_task": "validar el teléfono",
        "observed_symptom": "la validación no se completa",
    }
    assert (
        causal_scenario_title(row, rank=1, include_topic=True)
        == "Acceso > Token: validar el teléfono → la validación no se completa"
    )
    row["affected_task"] = "Tarea pendiente de validación"
    assert causal_scenario_title(row, rank=1, include_topic=True) == "Acceso > Token"
    assert causal_scenario_title({}, rank=3, include_topic=True) == "Escenario de evidencia 3"


def test_newsletter_never_attributes_another_topics_evidence_to_the_primary_signal():
    frame = pd.DataFrame(
        {
            "Palanca": ["Continuidad"] * 3 + ["Acceso"],
            "NPS": [0, 1, 0, 2],
            "Comment": ["Se cuelga al operar"] * 3 + ["No puedo validar el teléfono"],
        }
    )
    card = {
        "title": "Acceso > Token: validar el teléfono → falla la validación",
        "anchor_topic": "Acceso > Token",
        "affected_task": "validar el teléfono",
        "observed_symptom": "falla la validación",
        "linked_pairs": 2,
        "linked_comments": 1,
        "linked_incidents": 2,
        "causal_engine": "llm",
        "avg_semantic_confidence": 0.9,
        "evidence_reason": "Caso exclusivo de validación de teléfono.",
    }
    kwargs = dict(
        current_df=frame,
        period_kpis={},
        topic_channel="Todos",
        period_start=date(2026, 9, 1),
        period_end=date(2026, 9, 30),
    )
    result = build_executive_newsletter(linking={"scenarios": {"cards": [card]}}, **kwargs)
    assert "continuidad" in result["headline"]
    assert card["evidence_reason"] not in result["lead"]
    assert [item["label"] for item in result["signals"]] == ["Continuidad", "Acceso"]
    card["anchor_topic"] = "Continuidad > Cuelgue"
    card["title"] = "Continuidad > Cuelgue: operar → se interrumpe"
    card["affected_task"] = "operar"
    card["observed_symptom"] = "se interrumpe"
    result = build_executive_newsletter(linking={"scenarios": {"cards": [card]}}, **kwargs)
    assert card["title"] in result["lead"] and card["evidence_reason"] in result["lead"]


def test_webapp_table_respects_published_column_order_even_with_sorted_json_keys():
    source = Path("webapp/apps-script/App.html").read_text()
    function = source[source.index("  function table(") : source.index("  function metric(")]
    columns = [
        "Detractor Comment",
        "Incident ID",
        "Incident Summary",
        "NPS Topic",
        "Confianza semántica",
    ]
    row = {
        "Confianza semántica": "90%",
        "Detractor Comment": "No puedo entrar",
        "Incident ID": "INC1",
        "Incident Summary": "Error acceso",
        "NPS Topic": "Acceso > Token",
    }
    script = (
        "const assert=require('node:assert/strict'); const rows=v=>Array.isArray(v)?v:[]; const esc=v=>String(v??''); const label=v=>v; const format=v=>v; const evidenceText=v=>v;"
        + function
    )
    script += f"const html=table([{json.dumps(row, sort_keys=True)}],100,{json.dumps(columns)});"
    script += "const headers=[...html.matchAll(/<th>(.*?)<\\/th>/g)].map(match=>match[1]);"
    script += f"assert.deepEqual(headers,{json.dumps(columns)});assert.ok(html.includes('90%'));assert.ok(!html.includes('Tasa Foco'));assert.ok(!html.includes('Similitud textual'));"
    subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
