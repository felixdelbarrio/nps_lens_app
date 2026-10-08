"""One label for the unique-comment distribution across all output channels."""


def score_group_label(score: float | None, count: int) -> str:
    value = f"Score {score:g}" if score is not None else "Sin score"
    return f"{value} / {count} {'Comentario' if count == 1 else 'Comentarios'}"
