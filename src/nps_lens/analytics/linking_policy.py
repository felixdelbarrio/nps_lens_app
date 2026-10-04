from __future__ import annotations

from typing import Any

# Association policy: no coverage target may relax the evidence requirements.
LINK_MIN_SHARED_TERMS = 2
# A product or channel match alone does not establish a shared failure.
LINK_CONTEXT_TERMS = frozenset(
    {
        "acceso",
        "seguridad",
        "login",
        "cuenta",
        "cuentas",
        "tarjeta",
        "tarjetas",
        "credito",
        "debito",
        "pago",
        "pagos",
        "transferencia",
        "transferencias",
        "saldo",
        "saldos",
        "movimientos",
        "consulta",
        "consultas",
        "comprobante",
        "comprobantes",
        "producto",
        "productos",
        "net",
        "cash",
        "operativa",
    }
)
LINK_MIN_SIMILARITY = 0.15
LINK_TOP_K_PER_INCIDENT = 5
HOTSPOT_MIN_TERM_OCCURRENCES = 3
LINK_MAX_DAYS_APART = 90
# Evidence samples are bounded; totals and organization metrics use all accepted pairs.
LINK_MAX_VISIBLE_INCIDENTS = 50
LINK_MAX_VISIBLE_COMMENTS = 50

LINK_MAX_FEATURES = 50000
LINK_EVIDENCE_CHUNK_SIZE = 128


def temporal_mask(
    comment_dates: Any, incident_dates: Any, max_days_apart: int = LINK_MAX_DAYS_APART
) -> Any:
    """Business association window: inclusive calendar days, in either direction."""
    import numpy as np
    import pandas as pd

    def days(values):
        dates = pd.to_datetime(values, errors="coerce", utc=True)
        if isinstance(dates, pd.Series):
            return dates.dt.normalize()
        if isinstance(dates, pd.DatetimeIndex):
            return dates.normalize()
        return pd.DatetimeIndex([dates]).normalize()[0]

    delta = (days(comment_dates) - days(incident_dates)) / pd.Timedelta(days=1)
    return np.isfinite(delta) & (abs(delta) <= max(0, int(max_days_apart)))


def temporal_scope_mask(
    comment_dates: Any, incident_dates: Any, max_days_apart: int = LINK_MAX_DAYS_APART
) -> Any:
    """Cheap envelope prefilter; individual pairs still pass temporal_mask."""
    import pandas as pd

    comments = pd.to_datetime(comment_dates, errors="coerce", utc=True).dropna()
    incidents = pd.to_datetime(incident_dates, errors="coerce", utc=True)
    if comments.empty:
        return incidents.notna() & False
    return (
        incidents.between(comments.min(), comments.max())
        | temporal_mask(comments.min(), incidents, max_days_apart)
        | temporal_mask(comments.max(), incidents, max_days_apart)
    )


def evaluation_diagnostic(
    *,
    eligible: int = 0,
    candidate_count: int = 0,
    with_candidates: int = 0,
    matches: int = 0,
    reason: str = "",
) -> dict[str, Any]:
    state = (
        "MATCHED"
        if matches
        else "NOT_EVALUATED" if reason or not candidate_count else "EVALUATED_NO_MATCH"
    )
    reason = reason or ("no_candidates" if state == "NOT_EVALUATED" else "")
    messages = {
        "MATCHED": "Evidencia Helix ↔ VoC",
        "EVALUATED_NO_MATCH": "Candidatos evaluados sin vínculos aceptados",
        "NOT_EVALUATED": "Vínculos no evaluados: "
        + {
            "classification_pending": "clasificación de la lente activa pendiente",
            "evaluation_pending": "evaluación LLM pendiente",
            "no_candidates": "sin candidatos elegibles en la ventana",
        }.get(reason, reason),
    }
    return dict(
        evaluation_state=state,
        evaluation_reason=reason,
        evaluation_message=messages[state],
        eligible_incidents=eligible,
        incidents_with_candidates=with_candidates,
        incidents_without_candidates=max(0, eligible - with_candidates),
        candidate_count=candidate_count,
    )
