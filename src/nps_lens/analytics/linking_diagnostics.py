"""Population accounting shared by analysis, API and report consumers."""

from __future__ import annotations

import pandas as pd

from nps_lens.analytics.linking_policy import evaluation_diagnostic
from nps_lens.analytics.nps_helix_link import nps_matchable_mask
from nps_lens.domain.helix import SOURCE_SERVICE_N1, SOURCE_SERVICE_N2
from nps_lens.domain.normalization import semantic_series


def linking_diagnostics(
    *,
    nps: pd.DataFrame,
    focus: pd.DataFrame,
    helix: pd.DataFrame,
    scoped: pd.DataFrame,
    period: pd.DataFrame,
    eligible: pd.DataFrame,
    links: pd.DataFrame,
    requested_scope: list[str],
) -> dict[str, object]:
    def values(frame: pd.DataFrame, column: str) -> list[str]:
        text = semantic_series(frame.get(column, pd.Series("", index=frame.index)))
        return sorted(set(text.str.split(",").explode().str.strip().dropna()) - {""})

    matchable = int(nps_matchable_mask(focus).sum())
    linked_incidents = int(links["incident_id"].nunique()) if not links.empty else 0
    linked_comments = int(links["nps_id"].nunique()) if not links.empty else 0
    exclusions = {
        "nps_outside_focus": len(nps) - len(focus),
        "non_matchable": len(focus) - matchable,
        "helix_outside_scope": len(helix) - len(scoped),
        "helix_outside_period_or_missing_date": len(scoped) - len(period),
        "helix_quality": len(period) - len(eligible),
        "helix_without_evidence": len(eligible) - linked_incidents,
    }
    evaluation = evaluation_diagnostic(
        eligible=len(eligible),
        candidate_count=int(links.attrs.get("candidate_count", 0)),
        with_candidates=int(links.attrs.get("incidents_with_candidates", 0)),
        matches=len(links),
        reason=str(links.attrs.get("evaluation_reason", "")) if links.empty else "",
    )
    reuse = (
        links.groupby("nps_id", sort=False)["incident_id"].nunique()
        if not links.empty
        else pd.Series(dtype="int64")
    )
    percentiles = (
        reuse.quantile([0.5, 0.9, 0.95])
        if not reuse.empty
        else pd.Series(0.0, index=[0.5, 0.9, 0.95])
    )
    return {
        **evaluation,
        "incidents_per_comment": {
            "p50": float(percentiles.loc[0.5]),
            "p90": float(percentiles.loc[0.9]),
            "p95": float(percentiles.loc[0.95]),
            "max": int(reuse.max()) if not reuse.empty else 0,
            "comments_gt1": int((reuse > 1).sum()),
            "comments_gt5": int((reuse > 5).sum()),
            "comments_gt10": int((reuse > 10).sum()),
        },
        "nps_total": len(nps),
        "nps_focus": len(focus),
        "nps_matchable": matchable,
        "nps_non_matchable": len(focus) - matchable,
        "helix_total": len(helix),
        "helix_after_scope": len(scoped),
        "helix_after_period": len(period),
        "helix_quality_eligible": len(eligible),
        "linked_incidents": linked_incidents,
        "linked_nps_comments": linked_comments,
        "evidence_pairs": len(links),
        "nps_matchable_pct": 100 * matchable / len(focus) if len(focus) else 0.0,
        "nps_coverage_pct": 100 * linked_comments / len(nps) if len(nps) else 0.0,
        "matchable_coverage_pct": 100 * linked_comments / matchable if matchable else 0.0,
        "helix_coverage_pct": 100 * linked_incidents / len(eligible) if len(eligible) else 0.0,
        "scope_requested_n1_n2": requested_scope,
        "scope_found_n1": values(scoped, SOURCE_SERVICE_N1),
        "scope_found_n2": values(scoped, SOURCE_SERVICE_N2),
        "scope_available_n1": values(helix, SOURCE_SERVICE_N1),
        "scope_available_n2": values(helix, SOURCE_SERVICE_N2),
        "exclusions": exclusions,
        "principal_exclusion_reason": (
            max(exclusions, key=exclusions.get) if any(exclusions.values()) else ""
        ),
    }
