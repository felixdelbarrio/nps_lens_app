from nps_lens.domain.publication_scope import build_publication_scope


def test_publication_scope_is_stable_and_separates_audience_from_edition() -> None:
    march = build_publication_scope(
        owner_support_company="BBVA México",
        year="2026",
        month="03",
        causal_method="executive_journeys",
    )
    april = build_publication_scope(
        owner_support_company="BBVA México",
        year="2026",
        month="04",
        causal_method="executive_journeys",
    )
    assert march["key"] != april["key"]
    assert march["audience_key"] == april["audience_key"]
    assert march["owner_support_company"] == "BBVA México"
    assert "n1" not in march
    assert march["causal_method_label"] == "Journeys de detracción"


def test_publication_scope_requires_an_exact_period() -> None:
    try:
        build_publication_scope(
            owner_support_company="BBVA México",
            year="2026",
            month="Todos",
            causal_method="executive_journeys",
        )
    except ValueError as error:
        assert "año, mes" in str(error)
    else:
        raise AssertionError("An exact publication period is required")
