import pandas as pd
import pytest

from nps_lens.domain.analysis_labels import negative_gap_headline, problem_scope_labels


@pytest.mark.parametrize("group", ["Todos", "Detractores", "Pasivos", "Promotores"])
def test_problem_labels_explain_group_granularity_and_limit(group):
    labels = problem_scope_labels(group)
    assert ("todos los grupos NPS" if group == "Todos" else group.lower()) in labels["title"]
    assert "tópico > problema" in labels["subtitle"]
    assert "hasta 10 problemas" in labels["subtitle"]


def test_negative_gap_headline_recognizes_ties_independently_of_volume_order():
    frame = pd.DataFrame(
        {"value": ["A", "B", "C", "D"], "gap_vs_base": [-111.61, -111.61, -106.28, -111.61]}
    )
    assert negative_gap_headline(frame).startswith("3 tópicos comparten")
    assert negative_gap_headline(frame.iloc[::-1]).startswith("3 tópicos comparten")
    assert negative_gap_headline(frame.iloc[[2]]).startswith("C presenta")
    assert negative_gap_headline(frame.assign(gap_vs_base=0)).startswith("Sin brechas")
