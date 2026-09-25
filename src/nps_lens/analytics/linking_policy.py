from __future__ import annotations

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
