"""Population and metric labels shared by analytical presentation surfaces."""

import math

import pandas as pd

TOPIC_METRIC_BASIS = (
    "Por tópico; opiniones de todos los grupos NPS: detractores, pasivos y promotores. "
    "Score medio: media de puntuaciones de 0 a 10."
)
TOPIC_METRIC_CAPTION = (
    "Por tópico · Todos los grupos NPS · Score medio (0–10) · % detractores del tópico. "
)
SIGNALS_TITLE = "Señales por tópico · todos los grupos NPS"
GAP_METRIC_BASIS = (
    "Brecha = NPS clásico del tópico en el periodo actual − NPS clásico global de la base "
    "histórica, en puntos. Ambos usan todos los grupos NPS. El canal selecciona los tópicos observados."
)


def problem_scope_labels(group: str, limit: int = 10) -> dict[str, str]:
    population = "todos los grupos NPS" if group == "Todos" else group.lower()
    return {
        "title": f"Top {limit} problemas · {population}",
        "subtitle": (
            "Agrupación: tópico > problema. Recuento del periodo y canal seleccionados. "
            f"Se muestran hasta {limit} problemas; los restantes no aparecen en este ranking."
        ),
        "count_label": f"Comentarios · {population}",
    }


def gap_metric_label(base_label: str) -> str:
    return f"Brecha NPS vs base global [{base_label}] (puntos)"


def negative_gap_headline(rows: pd.DataFrame) -> str:
    if rows.empty or not rows["gap_vs_base"].lt(0).any():
        return "Sin brechas NPS negativas frente a la base histórica global"
    worst = rows.loc[rows["gap_vs_base"].idxmin()]
    ties = sum(
        math.isclose(float(value), float(worst["gap_vs_base"]), abs_tol=1e-9)
        for value in rows["gap_vs_base"]
    )
    if ties > 1:
        return f"{ties} tópicos comparten la mayor brecha NPS negativa frente a la base global"
    return f"{worst['value']} presenta la mayor brecha NPS negativa frente a la base global"
