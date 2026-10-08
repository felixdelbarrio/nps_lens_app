"""One score distribution and frequency-weighted mean for all output channels."""

import json
import subprocess
from pathlib import Path

import pandas as pd

from nps_lens.analytics.causal_evidence import linked_comment_metrics
from nps_lens.analytics.incident_attribution import build_incident_attribution_chains
from nps_lens.services.dashboard_service import DashboardService


def test_weighted_scores_count_responses_once_not_distinct_scores_or_links():
    links = pd.DataFrame(
        {
            "nps_id": ["a", "b", "c", "d", "a", "d"],
            "nps_score": [0, 0, 0, 1, 0, 1],
        }
    )
    metrics = linked_comment_metrics(links)
    assert metrics["avg_score"] == 0.25
    assert metrics["linked_comments"] == 4
    assert metrics["score_distribution"] == [
        {"score": 0, "count": 3, "label": "Score 0 / 3 Comentarios"},
        {"score": 1, "count": 1, "label": "Score 1 / 1 Comentario"},
    ]
    assert linked_comment_metrics(links.iloc[::-1]) == metrics


def test_invalid_scores_are_not_zeros_and_have_no_weight_in_the_mean():
    metrics = linked_comment_metrics(
        pd.DataFrame(
            {
                "nps_id": list("abcdefgh"),
                "nps_score": [0, "10", None, "", "bad", -1, 11, 2.5],
            }
        )
    )
    assert metrics["avg_score"] == 5
    assert metrics["detractor_rate"] == 0.5
    assert metrics["score_distribution"][-1] == {
        "score": None,
        "count": 6,
        "label": "Sin score / 6 Comentarios",
    }
    empty = linked_comment_metrics(pd.DataFrame(columns=["nps_id", "nps_score"]))
    assert empty["avg_score"] is None
    assert empty["score_distribution"] == []
    assert (
        linked_comment_metrics(pd.DataFrame({"nps_id": ["a"], "nps_score": [None]}))["avg_score"]
        is None
    )


def scenario_card():
    nps = pd.DataFrame(
        {
            "ID": list("abcd"),
            "NPS": [0, 0, 0, 1],
            "Fecha": pd.Timestamp("2026-09-03"),
            "Comment": "No puedo acceder",
            "Palanca": "Acceso",
            "Subpalanca": "Login",
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["i1", "i2"],
            "Submit Date": pd.Timestamp("2026-09-01"),
            "Detailed Description": "Error al acceder",
        }
    )
    links = pd.DataFrame(
        {
            "nps_id": ["a", "b", "c", "d", "a", "d"],
            "incident_id": ["i1", "i1", "i1", "i1", "i2", "i2"],
            "text_similarity": 0.9,
            "nps_topic": "Acceso > Login",
        }
    )
    chains = build_incident_attribution_chains(links, nps, helix, top_k=0, max_comment_examples=1)
    assert len(chains) == 1
    row = chains.iloc[0]
    assert row.avg_nps == row.avg_score == 0.25
    assert row.linked_comments == 4
    assert len(row.comment_records) == 1
    capped = chains
    card = object.__new__(DashboardService)._build_linking_scenario_cards(capped)[0]
    assert card["avg_nps"] == 0.25
    assert sum(bucket["count"] for bucket in card["score_distribution"]) == 4
    assert card["spotlight_metrics"][0]["value"] == "0,25"
    return card


def test_full_score_distribution_survives_evidence_sample_limits():
    scenario_card()


def test_webapp_renders_the_same_precomputed_score_distribution_safely():
    card = scenario_card()
    card["score_distribution"][0]["label"] += " <script>unsafe</script>"
    source = Path("webapp/apps-script/App.html").read_text()
    functions = source[
        source.index("  function evidenceText(") : source.index("  function scenarioWorkspace(")
    ]
    script = (
        """
const assert = require('node:assert/strict');
const state = {evidenceView:'table',scenarioDetail:'helix'};
const rows = value => Array.isArray(value) ? value : [];
const esc = value => String(value??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
const format = () => "REFORMATTED";
const table = () => '<table>Detail</table>';
const contentTabs = () => '';
"""
        + functions
        + "\nconst card = "
        + json.dumps(card, ensure_ascii=False)
        + """;
const original = JSON.stringify(card);
const html = scenarioCard(card,0,1);
assert.ok(html.includes('Score 0 / 3 Comentarios'));
assert.ok(html.includes('Score 1 / 1 Comentario'));
assert.ok(html.includes('0,25'));
assert.ok(!html.includes('<script>unsafe</script>'));
const overview = html.split('4 comentarios enlazados')[1].split('</article>')[0];
assert.ok(!overview.includes('comment_id'));
assert.equal(JSON.stringify(card),original);
"""
    )
    subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
