from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import statsmodels.api as sm


@dataclass(frozen=True)
class CausalHypothesis:
    treatment: str
    outcome: str
    controls: list[str]
    effect: float
    p_value: float
    n: int
    method: str
    assumptions: list[str]
    warnings: list[str]


def _prepare_binary_treatment(df: pd.DataFrame, col: str, value: str) -> pd.Series:
    return (df[col].astype(str) == value).astype(int)


def best_effort_ate_logit(
    df: pd.DataFrame,
    treatment_col: str,
    treatment_value: str,
    outcome_col: str = "is_detractor",
    control_cols: Optional[list[str]] = None,
) -> Optional[CausalHypothesis]:
    """Pragmatic causal estimate (best-effort).

    Estimates association with controls using logistic regression:
      P(outcome=1) = logit( treat + controls )

    Limitations:
      - Not a true causal identification without strong assumptions.
      - Controls only observable confounders.
    """
    if control_cols is None:
        control_cols = []

    data = df.copy()
    if outcome_col not in data.columns:
        # create detractor outcome if NPS present
        if "NPS" not in data.columns:
            return None
        data[outcome_col] = (pd.to_numeric(data["NPS"], errors="coerce") <= 6).astype(int)

    if treatment_col not in data.columns:
        return None

    y = pd.to_numeric(data[outcome_col], errors="coerce")
    t = _prepare_binary_treatment(data, treatment_col, treatment_value)
    X_parts: list[pd.Series] = [t.rename("treat")]
    warnings: list[str] = []

    for c in control_cols:
        if c not in data.columns:
            warnings.append(f"Control column missing: {c}")
            continue
        # one-hot for categoricals (limit cardinality)
        if data[c].dtype == "object" or str(data[c].dtype).startswith("category"):
            top = data[c].astype(str).value_counts().head(20).index.tolist()
            limited = data[c].astype(str).where(data[c].astype(str).isin(top), "__OTHER__")
            dummies = pd.get_dummies(limited, prefix=c)
            for colname in dummies.columns:
                X_parts.append(dummies[colname])
        else:
            X_parts.append(pd.to_numeric(data[c], errors="coerce").rename(c))

    X = pd.concat(X_parts, axis=1)
    X = sm.add_constant(X, has_constant="add")
    mask = y.notna() & X.notna().all(axis=1)
    y2 = y.loc[mask].astype(int)
    X2 = X.loc[mask].astype(float)

    if len(y2) < 500:
        warnings.append("Low sample size for stable estimates (<500).")

    try:
        model = sm.Logit(y2, X2).fit(disp=False, maxiter=200)
        coef = float(model.params["treat"])
        pval = float(model.pvalues["treat"])
        # convert log-odds to approx risk diff at mean baseline
        baseline = float(y2.mean())
        # marginal effect approx: baseline*(1-baseline)*coef
        effect = float(baseline * (1.0 - baseline) * coef)
        return CausalHypothesis(
            treatment=f"{treatment_col} == {treatment_value}",
            outcome=outcome_col,
            controls=control_cols,
            effect=effect,
            p_value=pval,
            n=int(len(y2)),
            method="logit+marginal_effect",
            assumptions=[
                "No unobserved confounding (given controls).",
                "Correct model specification (logit).",
                "SUTVA / no interference.",
            ],
            warnings=warnings,
        )
    except Exception as e:
        warnings.append(f"Model failed: {e}")
        return CausalHypothesis(
            treatment=f"{treatment_col} == {treatment_value}",
            outcome=outcome_col,
            controls=control_cols,
            effect=float("nan"),
            p_value=float("nan"),
            n=int(len(y2)),
            method="logit_failed",
            assumptions=["Best-effort only."],
            warnings=warnings,
        )
