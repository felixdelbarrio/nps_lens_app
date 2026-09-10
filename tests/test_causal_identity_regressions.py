"""Regression contracts for one causal identity across catalog, links and time series."""

import numpy as np
import pandas as pd
import pytest

from nps_lens.analytics.incident_attribution import (
    _select_topic_entities,
    build_broken_journey_catalog,
    build_broken_journey_topic_map,
    build_causal_topic_map,
    build_incident_attribution_chains,
    remap_links_to_causal_entities,
    remap_topic_timeseries_to_causal_entities,
)
from nps_lens.analytics.nps_helix_link import build_nps_topic
from nps_lens.domain.normalization import EquivalenceRegistry


def frames():
    nps = pd.DataFrame(
        {
            "ID": ["n1", "n2"],
            "Fecha": pd.to_datetime(["2026-02-01"] * 2),
            "NPS": [1, 5],
            "Palanca": ["Operativa"] * 2,
            "Subpalanca": ["Transferencias"] * 2,
            "Comment": ["No puedo enviar dinero", "La firma de transferencias falla"],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["i1", "i2", "i3"],
            "Fecha": pd.to_datetime(["2026-02-01"] * 3),
            "Detailed Description": ["Error de firma", "Timeout al enviar dinero", "Error SPEI"],
            "BBVA_SourceServiceN2": ["Firma", "SPEI", "Firma"],
        }
    )
    links = pd.DataFrame(
        {
            "incident_id": ["i1", "i2", "i3"],
            "nps_id": ["n1", "n1", "n2"],
            "similarity": [0.9, 0.8, 0.7],
            "nps_topic": ["Operativa > Transferencias"] * 3,
        }
    )
    return links, nps, helix


def test_one_route_is_atomic_and_score_counts_each_response_once():
    links, nps, helix = frames()
    catalog, evidence = build_broken_journey_catalog(links, nps, helix)
    assert len(catalog) == 1
    assert catalog.iloc[0].linked_pairs == 3
    assert catalog.iloc[0].linked_comments == 2
    assert catalog.iloc[0].avg_nps == 3
    assert evidence.journey_id.nunique() == 1
    assert len(build_broken_journey_topic_map(evidence)) == 1


def test_identity_survives_order_duplicates_and_unrelated_routes():
    links, nps, helix = frames()
    original, _ = build_broken_journey_catalog(links, nps, helix)
    nps.loc[len(nps)] = ["n3", pd.Timestamp("2026-02-01"), 0, "Acceso", "Login", "No puedo entrar"]
    links.loc[len(links)] = ["i1", "n3", 0.99, "Acceso > Login"]
    catalog, evidence = build_broken_journey_catalog(
        pd.concat([links, links]).sample(frac=1, random_state=9), nps.iloc[::-1], helix.iloc[::-1]
    )
    assert len(catalog) == 2
    assert (
        catalog.set_index("journey_label").loc[original.iloc[0].journey_label].journey_id
        == original.iloc[0].journey_id
    )
    assert len(evidence) == len(links)


def test_explicit_equivalences_apply_before_route_identity_without_touching_comments():
    links, nps, helix = frames()
    nps.loc[1, "Subpalanca"] = "Envíos"
    registry = EquivalenceRegistry.from_dict(
        {"dimensions": {"nps.Subpalanca": [{"canonical": "Transferencias", "aliases": ["Envíos"]}]}}
    )
    normalized = registry.apply("nps", nps)
    links["nps_topic"] = links.nps_id.map(normalized.set_index("ID").pipe(build_nps_topic))
    catalog, _ = build_broken_journey_catalog(links, normalized, helix)
    assert len(catalog) == 1
    pd.testing.assert_series_equal(nps.Comment, normalized.Comment)
    source_links = links.copy()
    source_links["nps_topic"] = source_links.nps_id.map(nps.set_index("ID").pipe(build_nps_topic))
    assert len(build_broken_journey_catalog(source_links, nps, helix)[0]) == 2


def test_missing_references_cannot_produce_phantom_evidence():
    links, nps, helix = frames()
    links.loc[0, "nps_id"] = "missing"
    nps.loc[1, "Comment"] = np.nan
    catalog, evidence = build_broken_journey_catalog(links, nps, helix)
    assert len(evidence) == 1
    assert catalog.iloc[0].linked_pairs == 1
    assert build_nps_topic(
        pd.DataFrame({"Palanca": [None, "Acceso"], "Subpalanca": ["Login", None]})
    ).tolist() == ["Login", "Acceso"]


def test_duplicate_reference_rows_do_not_multiply_links_and_conflicts_fail():
    links, nps, helix = frames()
    assert len(build_broken_journey_catalog(links, pd.concat([nps, nps]), helix)[1]) == 3
    conflict = nps.iloc[:1].copy()
    conflict["Subpalanca"] = "Login"
    with pytest.raises(pd.errors.MergeError):
        build_broken_journey_catalog(links, pd.concat([nps, conflict]), helix)


def test_entity_vote_is_not_fragmented_by_secondary_metadata():
    candidates = pd.DataFrame(
        {
            "source_nps_topic": ["Operativa > Transferencias"] * 3,
            "incident_id": ["i1", "i2", "i3"],
            "nps_id": ["n1", "n2", "n3"],
            "entity_id": ["A", "A", "B"],
            "entity_label": ["A", "A", "B"],
            "helix_source_service_n2": ["Firma", "SPEI", "Login"],
            "similarity": [0.8, 0.9, 0.99],
        }
    )
    mapping = _select_topic_entities(
        candidates, "entity_id", "entity_label", ["helix_source_service_n2"]
    )
    assert mapping.iloc[0].entity_id == "A"


def test_remapping_preserves_counts_and_does_not_impute_unknown_scores_as_zero():
    mapping = pd.DataFrame(
        {
            "source_nps_topic": ["a", "b"],
            "entity_id": ["e", "e"],
            "entity_label": ["Journey", "Journey"],
        }
    )
    weekly = pd.DataFrame(
        {
            "week": pd.to_datetime(["2026-02-01"] * 2),
            "nps_topic": ["a", "b"],
            "responses": [10, 20],
            "focus_count": [2, 3],
            "nps_mean": [8, np.nan],
            "incidents": [1, 2],
        }
    )
    result = remap_topic_timeseries_to_causal_entities(weekly, mapping)
    assert result.iloc[0].responses == 30
    assert result.iloc[0].focus_count == 5
    assert result.iloc[0].incidents == 3
    assert result.iloc[0].nps_mean == 8
    with pytest.raises(pd.errors.MergeError):
        remap_topic_timeseries_to_causal_entities(weekly, pd.concat([mapping, mapping]))


def test_catalog_detail_and_remapping_share_assignment():
    links, nps, helix = frames()
    catalog, evidence = build_broken_journey_catalog(links, nps, helix)
    mapping = build_causal_topic_map(
        links, nps, helix, touchpoint_source="broken_journeys", journey_links_df=evidence
    )
    remapped = remap_links_to_causal_entities(links, mapping)
    chains = build_incident_attribution_chains(
        remapped,
        nps,
        helix,
        touchpoint_source="broken_journeys",
        journey_catalog_df=catalog,
        journey_links_df=evidence,
        top_k=0,
    )
    assert len(chains) == len(catalog) == 1
    assert chains.iloc[0].linked_pairs == catalog.iloc[0].linked_pairs
    assert chains.iloc[0].avg_nps == catalog.iloc[0].avg_nps == 3
    assert set(remapped.entity_id) == set(evidence.journey_id)


def test_equivalence_suggestions_find_single_missing_variant_and_are_deterministic():
    registry = EquivalenceRegistry.default()
    expected = [{"canonical": "Funcionamiento continuo", "variants": ["Funcionamiento Continuo"]}]
    assert registry.collision_report("nps.Palanca", ["Funcionamiento Continuo"]) == expected
    values = ["PAGOS/ TRANSFERENCIAS", "pagos y transferencias"]
    assert registry.collision_report("nps.Palanca", values) == registry.collision_report(
        "nps.Palanca", values[::-1]
    )
    assert registry.normalize("nps.Palanca", "Funcionamiento Continuo") == "Funcionamiento Continuo"


def test_helix_product_equivalences_are_scoped_to_the_categorical_field():
    registry = EquivalenceRegistry.from_dict(
        {
            "dimensions": {
                "helix.Product Categorization Tier 1": [
                    {"canonical": "Pagos", "aliases": ["Transfer"]}
                ]
            }
        }
    )
    frame = pd.DataFrame(
        {"Product Categorization Tier 1": ["Transfer"], "Description": ["Transfer"]}
    )
    result = registry.apply("helix", frame)
    assert result.iloc[0]["Product Categorization Tier 1"] == "Pagos"
    assert result.iloc[0].Description == "Transfer"


def test_short_labels_do_not_drop_valid_links_when_vectorizer_has_no_vocabulary():
    links, nps, helix = frames()
    nps["Palanca"], nps["Subpalanca"], nps["Comment"] = "A", "B", "x"
    helix["Detailed Description"], helix["BBVA_SourceServiceN2"] = "c", ""
    links["nps_topic"] = "A > B"
    catalog, evidence = build_broken_journey_catalog(links, nps, helix)
    assert len(catalog) == 1
    assert len(evidence) == 3
    assert catalog.iloc[0].journey_keywords == ""


def test_executive_detail_reuses_topic_owner_instead_of_reclassifying_each_pair():
    links, nps, helix = frames()
    catalog = [
        {
            "id": "chosen",
            "title": "Operativa elegida",
            "touchpoint": "Firma",
            "palanca": "Operativa",
            "subpalanca": "Firma",
            "route": "Firma -> NPS",
            "keywords": ["inexistente"],
        }
    ]
    links["entity_id"] = "chosen"
    links["entity_label"] = "Operativa elegida"
    links["source_nps_topic"] = links["nps_topic"]
    links["nps_topic"] = "Operativa elegida"
    chains = build_incident_attribution_chains(
        links,
        nps,
        helix,
        touchpoint_source="executive_journeys",
        executive_journey_catalog=catalog,
        top_k=0,
    )
    assert len(chains) == 1
    assert chains.iloc[0].linked_pairs == 3
    assert chains.iloc[0].nps_topic == "Operativa elegida"


def test_null_ids_never_link_to_each_other():
    links, nps, helix = frames()
    links.loc[0, "nps_id"] = np.nan
    nps.loc[0, "ID"] = np.nan
    catalog, evidence = build_broken_journey_catalog(links, nps, helix)
    assert len(evidence) == 1
    assert catalog.iloc[0].linked_comments == 1
