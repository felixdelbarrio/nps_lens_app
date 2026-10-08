from datetime import date
from io import BytesIO

import pandas as pd
import pytest
from pptx import Presentation

from nps_lens.analytics.causal_evidence import engine_quality, link_confidence_label
from nps_lens.reports.executive_newsletter import build_executive_newsletter
from nps_lens.reports.executive_ppt import generate_business_review_ppt
from nps_lens.reports.narrative import comment_groups, experience_topics, ordered_scenarios
from nps_lens.services.dashboard_service import DashboardService


def opinions():
    return pd.DataFrame(
        {
            "ID": range(8),
            "Palanca": ["Operativa"] * 3 + ["Continuidad"] * 3 + ["Acceso"] * 2,
            "Subpalanca": ["Fallo"] * 8,
            "NPS": [0, 3, 3, 2, 4, 6, None, None],
            "Comment": ["No puedo completar mi operación"] * 8,
            "Canal": ["Web"] * 8,
            "Fecha": ["2026-08-01"] * 8,
        }
    )


def cases():
    rows = []
    for topic, value, identity, volume, text in [
        ("Continuidad", 0, "c", 1, "La aplicación no funciona"),
        ("Operativa", 2.5, "b", 2, "No puedo consultar movimientos"),
        ("Operativa", 2, "a", 2, "No pude pagar sueldos"),
        ("Operativa", None, "d", 100, "No puedo hacer una operación"),
        ("Acceso", 0, "e", 1, "No puedo iniciar sesión"),
    ]:
        rows.append(
            {
                "scenario_id": identity,
                "anchor_topic": topic + " > Fallo",
                "nps_topic": topic + " > Fallo",
                "avg_nps": value,
                "linked_comments": volume,
                "linked_incidents": 1,
                "linked_pairs": volume,
                "causal_engine": "llm",
                "avg_semantic_confidence": 0.9,
                "affected_task": "operar",
                "observed_symptom": "la operación falla",
                "comment_records": [{"comment_id": identity, "nps": value, "comment": text}],
                "incident_records": [
                    {
                        "incident_id": "INC" + identity,
                        "summary": "La operación falla",
                        "url": "https://helix.example/" + identity,
                    }
                ],
            }
        )
    return pd.DataFrame(rows)


def test_order_uses_topic_population_then_linked_score_and_preserves_every_case():
    topics = experience_topics(opinions(), channel="Web")
    assert topics.value.tolist() == ["Operativa", "Continuidad", "Acceso"]
    assert topics.score.iloc[:2].tolist() == [2, 4]
    assert pd.isna(topics.score.iloc[2])
    expected = ["a", "b", "d", "c", "e"]
    assert ordered_scenarios(cases(), topics).scenario_id.tolist() == expected
    assert (
        ordered_scenarios(cases().sample(frac=1, random_state=1), topics).scenario_id.tolist()
        == expected
    )
    assert len(ordered_scenarios(cases(), topics)) == len(cases())
    assert experience_topics(opinions(), channel="App").empty


def test_ties_use_volume_then_normalized_name_and_stable_identity():
    frame = opinions()
    frame.loc[:, "NPS"] = 2
    topics = experience_topics(frame)
    assert topics.value.tolist() == ["Continuidad", "Operativa", "Acceso"]
    rows = cases().iloc[:3].copy()
    rows.loc[:, "anchor_topic"] = "Operativa > Fallo"
    rows.loc[:, "avg_nps"] = 0
    assert ordered_scenarios(rows, topics).scenario_id.tolist() == ["a", "b", "c"]


def test_invalid_scenario_average_sorts_after_valid_zero():
    rows = cases().iloc[:3].copy()
    rows.loc[:, "anchor_topic"] = "Operativa > Fallo"
    rows.loc[:, "avg_nps"] = [99, 0, -1]
    assert ordered_scenarios(rows, experience_topics(opinions())).scenario_id.tolist() == [
        "b",
        "a",
        "c",
    ]


def test_groups_count_unique_comments_sort_scores_and_preserve_zero_and_missing():
    records = [
        {"comment_id": "a", "nps": 3, "comment": "No puedo operar"},
        {"comment_id": "b", "nps": 0, "comment": "Error"},
        {"comment_id": "c", "nps": 0, "comment": "Error"},
        {"comment_id": "d", "nps": None, "comment": "Sin nota"},
    ]
    groups = comment_groups(records + [records[1]])
    assert [(g["score"], g["count"]) for g in groups] == [(0, 2), (3, 1), (None, 1)]
    assert [g["label"] for g in groups] == [
        "Score 0 / 2 Comentarios",
        "Score 3 / 1 Comentario",
        "Sin score / 1 Comentario",
    ]
    assert sum(g["count"] for g in groups) == 4


@pytest.mark.parametrize(
    "engine,column,label",
    [
        ("llm", "avg_semantic_confidence", "CONFIANZA SEMÁNTICA"),
        ("rules", "avg_text_similarity", "SIMILITUD TEXTUAL"),
    ],
)
def test_quality_uses_own_engine_scale_and_distinguishes_zero(engine, column, label):
    assert link_confidence_label(engine) == label
    assert engine_quality({"causal_engine": engine, column: 0}) == 0
    assert engine_quality({"causal_engine": engine, column: 0.9}) == 0.9
    assert engine_quality({"causal_engine": engine, column: 90}) is None
    assert engine_quality({"causal_engine": engine}) is None


def test_newsletter_takes_one_traceable_quote_per_first_two_topics():
    frame = opinions()
    cards = object.__new__(DashboardService)._build_linking_scenario_cards(
        ordered_scenarios(cases(), experience_topics(frame))
    )
    result = build_executive_newsletter(
        current_df=frame,
        period_kpis={},
        linking={"scenarios": {"cards": cards}},
        topic_channel="Todos",
        period_start=date(2026, 8, 1),
        period_end=date(2026, 8, 31),
    )
    assert result["quotes"] == ["No pude pagar sueldos", "La aplicación no funciona"]
    assert [r["label"] for r in result["signals"]] == ["Operativa", "Continuidad", "Acceso"]
    assert [r["scenario_id"] for r in cards] == ["a", "b", "d", "c", "e"]
    single = {"scenarios": {"cards": cards[:1]}}
    result = build_executive_newsletter(
        current_df=frame,
        period_kpis={},
        linking=single,
        topic_channel="Todos",
        period_start=date(2026, 8, 1),
        period_end=date(2026, 8, 31),
    )
    assert result["quotes"] == ["No pude pagar sueldos"]


def test_ppt_interleaves_topics_and_cases_in_the_same_sequence():
    frame = opinions()
    out = generate_business_review_ppt(
        service_origin="Fixture",
        service_origin_n1="",
        service_origin_n2="",
        period_start=date(2026, 8, 1),
        period_end=date(2026, 8, 31),
        focus_name="Todos",
        selected_nps_df=frame,
        comparison_nps_df=frame,
        attribution_df=cases(),
    )
    deck = Presentation(BytesIO(out.compact_content))
    assert len(deck.slides) == 4 + 3 + 5
    sequence = []
    for slide in list(deck.slides)[4:]:
        separator = next(
            (shape.text for shape in slide.shapes if shape.name == "Topic separator"), None
        )
        if separator:
            sequence.append(separator)
        else:
            values = [shape.text for shape in slide.shapes if shape.name == "Scenario metric value"]
            sequence.append(values[0])
    assert sequence == [
        "VoC : Operativa",
        "2,00",
        "2,50",
        "n/d",
        "VoC : Continuidad",
        "0,00",
        "VoC : Acceso",
        "0,00",
    ]


def test_webapp_navigation_uses_the_published_sequence_and_resets_on_new_publication():
    import json
    import subprocess
    from pathlib import Path

    source = Path("webapp/apps-script/App.html").read_text()
    workspace = source[
        source.index("  function scenarioWorkspace(") : source.index(
            "  function linkingDiagnostics("
        )
    ]
    apply_start = source.index("  function applyPublication(")
    assert (
        "state.scenarioIndex=0;"
        in source[apply_start : source.index("  function ", apply_start + 10)]
    )
    rows = ordered_scenarios(cases(), experience_topics(opinions())).to_dict("records")
    ids = [row["scenario_id"] for row in rows]
    script = (
        """
const assert=require('node:assert/strict');
const state={scenarioIndex:0};
const rows=value=>value;
const esc=value=>String(value);
const scenarioCard=card=>card.scenario_id;
"""
        + workspace
        + "\nconst cards="
        + json.dumps(rows)
        + ";\nconst expected="
        + json.dumps(ids)
        + ";\n"
        + """
for(let i=0;i<cards.length;i++){
  state.scenarioIndex=i;
  assert.ok(scenarioWorkspace(cards).endsWith(expected[i]));
}
state.scenarioIndex=500;
scenarioWorkspace(cards);
assert.ok(state.scenarioIndex>=0&&state.scenarioIndex<cards.length);
"""
    )
    subprocess.run(["node"], input=script, check=True, capture_output=True, text=True)


def test_quotes_do_not_silently_replace_a_duplicate_second_topic_with_a_third():
    rows = cases()
    rows.at[0, "comment_records"] = [
        {"comment_id": "c", "nps": 0, "comment": "No pude pagar sueldos"}
    ]
    result = build_executive_newsletter(
        current_df=opinions(),
        period_kpis={},
        linking={"scenarios": {"cards": rows.to_dict("records")}},
        topic_channel="Todos",
        period_start=date(2026, 8, 1),
        period_end=date(2026, 8, 31),
    )
    assert result["quotes"] == ["No pude pagar sueldos"]


def test_null_comment_records_and_unknown_topic_do_not_become_zero():
    rows = pd.DataFrame(
        [
            {
                "nps_topic": "Acceso > Fallo",
                "anchor_topic": float("nan"),
                "palanca": "Acceso",
                "avg_nps": None,
                "comment_records": float("nan"),
            }
        ]
    )
    result = ordered_scenarios(rows, experience_topics(opinions()))
    assert result.narrative_topic.tolist() == ["Acceso"]
    assert result.avg_nps.isna().all()
    assert comment_groups([{"comment_id": "x", "nps": 2.5, "comment": "Error"}])[0]["score"] is None
