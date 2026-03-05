from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from nps_lens.analytics.causal import CausalHypothesis
from nps_lens.llm.knowledge_cache import stable_signature
from nps_lens.llm.schemas import InsightPackV1


def _as_int(value: Any, default: int = 0) -> int:
    """Best-effort conversion to int with strict typing.

    This is used for values coming from JSON-like dicts where the type is `object`.
    """

    if isinstance(value, bool):
        # avoid treating True/False as 1/0 accidentally
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except ValueError:
            return default
    return default


def render_pack_markdown(pack: InsightPackV1) -> str:
    ctx = "\n".join([f"- **{k}**: {v}" for k, v in pack.context.items()])
    hyps_lines = []
    for h in pack.hypotheses:
        title = h.get("title", "Hipótesis")
        conf = h.get("confidence", "?")
        why = h.get("why", "")
        hyps_lines.append(f"- **{title}** (conf={conf}) — {why}")
    hyps = "\n".join(hyps_lines)
    questions = "\n".join([f"- {q}" for q in pack.suggested_questions])
    actions = "\n".join([f"- {a}" for a in pack.suggested_actions])

    n_val = _as_int(pack.quantitative_evidence.get("n"), default=0)

    return f"""# LLM Deep-Dive Pack — {pack.title}

## Contexto
{ctx}

## Métricas clave
- **NPS**: {pack.metrics.get('nps', float('nan')):.1f}
- **% Detractores**: {pack.metrics.get('detractor_rate', float('nan')):.3f}
- **N (respuestas)**: {n_val}

## Evidencia cuantitativa
```json
{json.dumps(pack.quantitative_evidence, ensure_ascii=False, indent=2)}
```

## Evidencia cualitativa (muestras)
```json
{json.dumps(pack.qualitative_evidence, ensure_ascii=False, indent=2)}
```

## Hipótesis causales (ranked)
{hyps}

## Preguntas sugeridas para el deep-dive
{questions}

## Acciones sugeridas (fixes / experimentos / instrumentación)
{actions}

## Trazabilidad técnica
```json
{json.dumps(pack.technical_trace, ensure_ascii=False, indent=2)}
```
"""


def build_insight_pack(
    title: str,
    context: dict[str, str],
    nps_slice: pd.DataFrame,
    driver: dict[str, str],
    causal: Optional[CausalHypothesis] = None,
    examples: int = 10,
) -> InsightPackV1:
    scores = pd.to_numeric(nps_slice["NPS"], errors="coerce").dropna()
    promoters = float((scores >= 9).mean()) if not scores.empty else float("nan")
    detractors = float((scores <= 6).mean()) if not scores.empty else float("nan")
    nps = float((promoters - detractors) * 100.0) if not scores.empty else float("nan")

    # evidence: aggregated
    quantitative = {
        "n": int(len(nps_slice)),
        "driver": driver,
        "nps": nps,
        "promoter_rate": promoters,
        "detractor_rate": detractors,
    }

    # qualitative evidence: sample verbatims
    verb = nps_slice.get("Comment")
    samples: list[str] = []
    if verb is not None:
        samples = verb.dropna().astype(str).head(examples).tolist()
    qualitative = {"verbatims": samples}

    hypotheses: list[dict[str, Any]] = []
    if causal is not None:
        conf = 0.2
        if causal.p_value == causal.p_value:  # not NaN
            conf = float(1.0 - min(1.0, causal.p_value * 5.0))
        hypotheses.append(
            {
                "title": "Tratamiento asociado a detractores (best-effort)",
                "treatment": causal.treatment,
                "effect": causal.effect,
                "p_value": causal.p_value,
                "n": causal.n,
                "method": causal.method,
                "confidence": conf,
                "why": (
                    "Estimación con controles observables (segmento/canal/periodo/geo). "
                    "Ver supuestos."
                ),
                "assumptions": causal.assumptions,
                "warnings": causal.warnings,
            }
        )

    suggested_questions = [
        (
            "¿Qué cambios (release, incidents, campañas) ocurrieron en la ventana temporal "
            "donde se deteriora el driver?"
        ),
        "¿Qué segmentos/usuarios de decisión concentran el problema? ¿Hay cohortes por canal?",
        "¿Qué verbatims se repiten y qué 'tema' agrupan? ¿Hay errores o fricciones específicas?",
        "¿Qué métricas operativas (tiempos, errores, conversiones) confirman la hipótesis?",
        (
            "¿Qué experimento de producto o fix técnico podría validar/invalidar "
            "la causa raíz rápidamente?"
        ),
    ]
    suggested_actions = [
        "Abrir ticket con evidencia multi-fuente y owner claro (producto/tech/ops).",
        "Instrumentar evento en el touchpoint afectado (journey_step) para medir fricción.",
        "Diseñar experimento A/B (o rollout controlado) sobre la palanca/subpalanca priorizada.",
        "Crear alerta semanal para degradaciones del driver (change-point + umbral).",
    ]

    technical_trace = {
        "pack_version": "1.0",
        "filters": context,
        "created_at_utc": datetime.utcnow().isoformat() + "Z",
    }

    insight_id = stable_signature(context=context, title=title)[:12]
    return InsightPackV1(
        insight_id=insight_id,
        title=title,
        context=context,
        metrics={"nps": nps, "detractor_rate": detractors},
        quantitative_evidence=quantitative,
        qualitative_evidence=qualitative,
        hypotheses=hypotheses,
        suggested_questions=suggested_questions,
        suggested_actions=suggested_actions,
        technical_trace=technical_trace,
    )


def export_pack(pack: InsightPackV1, out_dir: Path) -> dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"{pack.insight_id}__pack.md"
    json_path = out_dir / f"{pack.insight_id}__pack.json"
    md_path.write_text(render_pack_markdown(pack), encoding="utf-8")
    # Pydantic v2: prefer model_dump_json; keep backward compatibility.
    if hasattr(pack, "model_dump_json"):
        json_str = pack.model_dump_json(indent=2)
    else:  # pragma: no cover
        json_str = pack.json(ensure_ascii=False, indent=2)
    json_path.write_text(json_str, encoding="utf-8")
    return {"md": md_path, "json": json_path}
