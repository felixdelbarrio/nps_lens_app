from nps_lens.ui.population import POP_ALL, month_format_es


def test_month_format_es_all():
    assert month_format_es(POP_ALL) == POP_ALL


def test_month_format_es_known_month():
    # UI requirement: show month label without "(MM)" suffix.
    assert month_format_es("03") == "Marzo"


def test_month_format_es_unknown_passthrough():
    assert month_format_es("13") == "13"
