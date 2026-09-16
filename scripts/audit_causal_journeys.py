"""Audit local causal identities and conservation without exporting customer text.

Run: .venv/bin/python scripts/audit_causal_journeys.py --output /tmp/causal-audit.json
Uses the configured default context unless explicit context options are supplied.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional, cast

import pandas as pd

from nps_lens.analytics.incident_attribution import build_incident_attribution_chains
from nps_lens.analytics.linking_policy import LINK_MAX_DAYS_APART, LINK_MIN_SIMILARITY
from nps_lens.domain.causal_methods import (
    TOUCHPOINT_SOURCE_BBVA_SOURCE_N2,
    TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
    TOUCHPOINT_SOURCE_DOMAIN,
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
    TOUCHPOINT_SOURCE_PALANCA,
)
from nps_lens.domain.models import UploadContext
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.dashboard_service import DashboardService
from nps_lens.settings import Settings
from nps_lens.ui.population import POP_ALL


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service-origin")
    parser.add_argument("--service-origin-n1")
    parser.add_argument("--service-origin-n2", default="")
    parser.add_argument("--year", default=POP_ALL)
    parser.add_argument("--month", default=POP_ALL)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    settings = Settings.from_env()
    service = DashboardService(SqliteNpsRepository(settings.database_path), settings)
    context = UploadContext(
        service_origin=args.service_origin or settings.default_service_origin,
        service_origin_n1=args.service_origin_n1 or settings.default_service_origin_n1,
        service_origin_n2=args.service_origin_n2,
    )
    bundle = service._causal_analysis_bundle(
        context=context,
        pop_year=args.year,
        pop_month=args.month,
        min_similarity=LINK_MIN_SIMILARITY,
        max_days_apart=LINK_MAX_DAYS_APART,
        touchpoint_source=TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
    )
    if not bundle["ready"]:
        raise SystemExit(
            "No hay datos suficientes para auditar el contexto y periodo seleccionados."
        )
    mode_payload = cast(dict[str, pd.DataFrame], bundle["mode_payload"])
    core = cast(dict[str, object], bundle["core"])
    focus_df = cast(pd.DataFrame, bundle["focus_df"])
    helix_slice = cast(pd.DataFrame, bundle["helix_slice"])
    executive_journey_catalog = cast(
        Optional[list[dict[str, object]]], bundle["executive_journey_catalog"]
    )
    catalog = mode_payload["broken_journeys_df"]
    evidence = mode_payload["broken_journey_links_df"]
    checks = {
        "unique_journey_ids": bool(catalog.journey_id.is_unique),
        "unique_journey_labels": bool(catalog.journey_label.is_unique),
        "unique_evidence_pairs": not bool(evidence.duplicated(["incident_id", "nps_id"]).any()),
        "atomic_source_topics": bool(
            (evidence.groupby("source_nps_topic").journey_id.nunique() <= 1).all()
        ),
        "catalog_conserves_links": int(catalog.linked_pairs.sum()) == len(evidence),
    }
    modes = {}
    for mode in (
        TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
        TOUCHPOINT_SOURCE_DOMAIN,
        TOUCHPOINT_SOURCE_PALANCA,
        TOUCHPOINT_SOURCE_BBVA_SOURCE_N2,
        TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
    ):
        payload = service._build_touchpoint_mode_payload(
            touchpoint_source=mode,
            links_df=cast(pd.DataFrame, core["links_df"]),
            focus_df=focus_df,
            helix_df=helix_slice,
            by_topic_weekly=cast(pd.DataFrame, core["by_topic_weekly"]),
            executive_journey_catalog=executive_journey_catalog,
        )
        mapping = payload["causal_topic_map_df"]
        mapped = payload["links_mode_df"]
        chains = build_incident_attribution_chains(
            mapped,
            focus_df,
            helix_slice,
            touchpoint_source=mode,
            top_k=0,
            journey_catalog_df=payload["broken_journeys_df"],
            journey_links_df=payload["broken_journey_links_df"],
            executive_journey_catalog=executive_journey_catalog,
        )
        original_series = cast(pd.DataFrame, core["by_topic_weekly"])
        eligible = original_series.loc[original_series.nps_topic.isin(mapping.source_nps_topic)]
        remapped_series = payload["by_topic_weekly_mode"]
        valid = {
            "unique_topic_owner": bool(mapping.source_nps_topic.is_unique),
            "detail_conserves_links": int(chains.linked_pairs.sum()) == len(mapped),
            "same_entities_in_detail": set(chains.nps_topic) == set(mapped.get("entity_label", [])),
            "series_conserve_responses": int(eligible.responses.sum())
            == int(remapped_series.responses.sum()),
            "series_conserve_incidents": int(eligible.incidents.sum())
            == int(remapped_series.incidents.sum()),
        }
        modes[mode] = {"entities": len(chains), "mapped_links": len(mapped), "checks": valid}
        checks.update({mode + ":" + name: value for name, value in valid.items()})
    report = {
        "context": vars(context),
        "year": args.year,
        "month": args.month,
        "passed": all(checks.values()),
        "checks": checks,
        "modes": modes,
        "journeys": catalog[
            ["journey_id", "journey_label", "linked_pairs", "linked_comments"]
        ].to_dict("records"),
    }
    output = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
    print(output)
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
