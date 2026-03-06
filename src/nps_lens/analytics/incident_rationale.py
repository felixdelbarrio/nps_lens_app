from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class IncidentRationaleSummary:
    topics_analyzed: int
    nps_points_at_risk: float
    nps_points_recoverable: float
    top3_incident_share: float
    confidence_mean: float
    median_lag_weeks: float


RATIONALE_COLUMNS = [
    "nps_topic",
    "weeks",
    "responses",
    "incidents",
    "incident_rate_per_100_responses",
    "focus_rate_base",
    "focus_rate_high_inc",
    "delta_focus_rate_pp",
    "attributable_focus_cases",
    "nps_points_at_risk",
    "nps_points_recoverable",
    "confidence",
    "priority",
    "best_lag_weeks",
    "corr",
    "max_cp_stability",
    "incidents_lead_changepoint_share",
    "action_lane",
    "owner_role",
    "eta_weeks",
]


def _empty_rationale_df() -> pd.DataFrame:
    return pd.DataFrame(columns=RATIONALE_COLUMNS)


def _clip01(value: Any) -> float:
    try:
        f = float(value)
    except Exception:
        return 0.0
    if not np.isfinite(f):
        return 0.0
    return float(max(0.0, min(1.0, f)))


def _safe_num(value: Any, default: float = 0.0) -> float:
    try:
        f = float(value)
    except Exception:
        return float(default)
    if not np.isfinite(f):
        return float(default)
    return float(f)


def _focus_group_norm(focus_group: str) -> str:
    g = str(focus_group or "detractor").strip().lower()
    if g == "promoter":
        return "promoter"
    if g == "passive":
        return "passive"
    return "detractor"


def _risk_delta(delta_focus_rate: float, focus_group: str) -> float:
    # "focus_rate" means bad when focus is detractor/passive and good when focus is promoter.
    if _focus_group_norm(focus_group) == "promoter":
        return max(0.0, -float(delta_focus_rate))
    return max(0.0, float(delta_focus_rate))


def _norm_by_max(values: pd.Series) -> pd.Series:
    v = pd.to_numeric(values, errors="coerce").fillna(0.0).astype(float)
    vmax = float(v.max()) if len(v) else 0.0
    if vmax <= 0:
        return pd.Series([0.0] * len(v), index=v.index)
    return (v / vmax).clip(0.0, 1.0)


def _action_plan(priority: float, lag_weeks: float, stability: float) -> tuple[str, str, int]:
    p = _clip01(priority)
    lag = max(0.0, _safe_num(lag_weeks, default=0.0))
    stab = _clip01(stability)
    if p >= 0.72 or (p >= 0.62 and stab >= 0.60):
        return "Fix estructural", "Producto + Tecnologia", 6
    if p >= 0.48 and lag <= 2.0:
        return "Quick win operativo", "Canal + Operaciones", 2
    return "Instrumentacion + validacion", "VoC + Analitica", 3


def _rank_lookup(rank_df: Optional[pd.DataFrame]) -> dict[str, dict[str, float]]:
    if rank_df is None or rank_df.empty or "nps_topic" not in rank_df.columns:
        return {}
    out: dict[str, dict[str, float]] = {}
    for _, r in rank_df.iterrows():
        topic = str(r.get("nps_topic", "")).strip()
        if not topic:
            continue
        out[topic] = {
            "score": _safe_num(r.get("score", np.nan), default=0.0),
            "corr": _safe_num(r.get("corr", np.nan), default=0.0),
            "best_lag_weeks": _safe_num(r.get("best_lag_weeks", np.nan), default=np.nan),
            "max_cp_stability": _safe_num(r.get("max_cp_stability", np.nan), default=0.0),
            "lead_share": _safe_num(
                r.get("incidents_lead_changepoint_share", np.nan),
                default=0.0,
            ),
        }
    return out


def build_incident_nps_rationale(
    by_topic_weekly: pd.DataFrame,
    *,
    focus_group: str = "detractor",
    rank_df: Optional[pd.DataFrame] = None,
    min_topic_responses: int = 80,
    recovery_factor: float = 0.65,
) -> pd.DataFrame:
    """Compute a business-priority table: incidents -> NPS risk -> recovery levers.

    Input is expected from `weekly_aggregates(...)[1]` with columns:
      - week, nps_topic, responses, focus_rate, incidents
    Optional `rank_df` enriches confidence/temporal signals with:
      - score, corr, best_lag_weeks, max_cp_stability, incidents_lead_changepoint_share
    """

    required = {"nps_topic", "responses", "focus_rate", "incidents"}
    if by_topic_weekly.empty or not required.issubset(set(by_topic_weekly.columns)):
        return _empty_rationale_df()

    df = by_topic_weekly.copy()
    if "week" not in df.columns:
        df["week"] = pd.NaT
    df["week"] = pd.to_datetime(df["week"], errors="coerce")
    df["nps_topic"] = df["nps_topic"].astype(str).str.strip()
    df["responses"] = pd.to_numeric(df["responses"], errors="coerce").fillna(0.0).clip(lower=0.0)
    df["focus_rate"] = pd.to_numeric(df["focus_rate"], errors="coerce").clip(lower=0.0, upper=1.0)
    df["incidents"] = pd.to_numeric(df["incidents"], errors="coerce").fillna(0.0).clip(lower=0.0)
    df = df[(df["nps_topic"] != "") & (df["responses"] > 0)].copy()
    if df.empty:
        return _empty_rationale_df()

    rank_map = _rank_lookup(rank_df)
    total_responses = float(df["responses"].sum())
    if total_responses <= 0.0:
        return _empty_rationale_df()

    rows: list[dict[str, Any]] = []
    fg = _focus_group_norm(focus_group)
    for topic, g in df.groupby("nps_topic", dropna=False, observed=True):
        g = g.sort_values("week").copy()
        responses = int(g["responses"].sum())
        if responses < int(min_topic_responses):
            continue
        weeks = int(g["week"].nunique()) if g["week"].notna().any() else int(len(g))
        if weeks < 3:
            continue

        focus = g["focus_rate"].astype(float)
        incidents = g["incidents"].astype(float)
        if int(focus.notna().sum()) < 3:
            continue

        q30 = float(np.nanquantile(incidents, 0.30))
        q70 = float(np.nanquantile(incidents, 0.70))
        low_mask = incidents <= q30
        high_mask = incidents >= q70
        if int(low_mask.sum()) < 2 or int(high_mask.sum()) < 2:
            med = float(np.nanmedian(incidents))
            low_mask = incidents <= med
            high_mask = incidents > med
        if int(low_mask.sum()) < 1 or int(high_mask.sum()) < 1:
            continue

        focus_base = _safe_num(focus.loc[low_mask].mean(), default=np.nan)
        focus_high = _safe_num(focus.loc[high_mask].mean(), default=np.nan)
        if not (np.isfinite(focus_base) and np.isfinite(focus_high)):
            continue

        delta_focus = float(focus_high - focus_base)
        risk_delta = _risk_delta(delta_focus, fg)
        high_responses = float(g.loc[high_mask, "responses"].sum())
        attributable_cases = float(risk_delta * high_responses)

        incidents_total = float(incidents.sum())
        incident_rate = float((incidents_total / max(float(responses), 1.0)) * 100.0)
        response_share = float(responses / total_responses)
        nps_risk = float(risk_delta * 100.0 * response_share)

        r = rank_map.get(str(topic), {})
        rank_score = _clip01(r.get("score", 0.0))
        corr = abs(_safe_num(r.get("corr", 0.0), default=0.0))
        cp_stability = _clip01(r.get("max_cp_stability", 0.0))
        lag_weeks = _safe_num(r.get("best_lag_weeks", np.nan), default=np.nan)
        lead_share = _clip01(r.get("lead_share", 0.0))
        if lead_share > 1.0:
            # Some UI paths show this metric in percentage points.
            lead_share = _clip01(lead_share / 100.0)

        support_signal = _clip01(np.log1p(responses) / 8.0)
        incident_signal = _clip01(np.log1p(incidents_total) / 6.0)
        effect_signal = _clip01(risk_delta / 0.12)
        temporal_signal = _clip01(corr)
        confidence = (
            0.22 * support_signal
            + 0.12 * incident_signal
            + 0.26 * effect_signal
            + 0.16 * temporal_signal
            + 0.12 * cp_stability
            + 0.07 * lead_share
            + 0.05 * rank_score
        )
        confidence = _clip01(confidence)

        lag_signal = 0.55
        if np.isfinite(lag_weeks):
            lag_signal = 1.0 - _clip01(float(lag_weeks) / 6.0)
        recoverable = float(nps_risk * float(recovery_factor) * confidence)

        rows.append(
            {
                "nps_topic": str(topic),
                "weeks": int(weeks),
                "responses": int(responses),
                "incidents": int(round(incidents_total)),
                "incident_rate_per_100_responses": incident_rate,
                "focus_rate_base": float(focus_base),
                "focus_rate_high_inc": float(focus_high),
                "delta_focus_rate_pp": float(delta_focus * 100.0),
                "attributable_focus_cases": attributable_cases,
                "nps_points_at_risk": nps_risk,
                "nps_points_recoverable": recoverable,
                "confidence": confidence,
                "_lag_signal": lag_signal,
                "best_lag_weeks": lag_weeks if np.isfinite(lag_weeks) else np.nan,
                "corr": float(corr),
                "max_cp_stability": cp_stability,
                "incidents_lead_changepoint_share": lead_share,
            }
        )

    if not rows:
        return _empty_rationale_df()

    out = pd.DataFrame(rows)
    out["impact_norm"] = _norm_by_max(out["nps_points_at_risk"])
    out["priority"] = (
        0.60 * out["impact_norm"]
        + 0.30 * out["confidence"].astype(float)
        + 0.10 * out["_lag_signal"].astype(float)
    ).clip(0.0, 1.0)
    out.loc[out["nps_points_at_risk"] <= 0.0, "priority"] = 0.0

    actions = [
        _action_plan(p, lag, stab)
        for p, lag, stab in zip(
            out["priority"].astype(float).tolist(),
            out["best_lag_weeks"].tolist(),
            out["max_cp_stability"].astype(float).tolist(),
        )
    ]
    out["action_lane"] = [a[0] for a in actions]
    out["owner_role"] = [a[1] for a in actions]
    out["eta_weeks"] = [int(a[2]) for a in actions]

    out = out.sort_values(
        ["priority", "nps_points_at_risk", "incidents", "responses"],
        ascending=False,
    ).reset_index(drop=True)
    return out[RATIONALE_COLUMNS].copy()


def summarize_incident_nps_rationale(
    rationale_df: pd.DataFrame,
) -> IncidentRationaleSummary:
    if rationale_df is None or rationale_df.empty:
        return IncidentRationaleSummary(
            topics_analyzed=0,
            nps_points_at_risk=0.0,
            nps_points_recoverable=0.0,
            top3_incident_share=0.0,
            confidence_mean=0.0,
            median_lag_weeks=float("nan"),
        )

    df = rationale_df.copy()
    incidents = pd.to_numeric(df.get("incidents"), errors="coerce").fillna(0.0)
    total_inc = float(incidents.sum())
    top3_inc = float(incidents.head(3).sum())
    top3_share = (top3_inc / total_inc) if total_inc > 0 else 0.0

    lags = pd.to_numeric(df.get("best_lag_weeks"), errors="coerce").dropna()
    lag_med = float(lags.median()) if not lags.empty else float("nan")

    conf = pd.to_numeric(df.get("confidence"), errors="coerce").fillna(0.0)
    risk = pd.to_numeric(df.get("nps_points_at_risk"), errors="coerce").fillna(0.0)
    rec = pd.to_numeric(df.get("nps_points_recoverable"), errors="coerce").fillna(0.0)

    return IncidentRationaleSummary(
        topics_analyzed=int(len(df)),
        nps_points_at_risk=float(risk.sum()),
        nps_points_recoverable=float(rec.sum()),
        top3_incident_share=float(max(0.0, min(1.0, top3_share))),
        confidence_mean=float(conf.mean()),
        median_lag_weeks=lag_med,
    )
