"""Shared geometry for topic summaries, separators and single-slide evidence."""

from dataclasses import dataclass
from functools import lru_cache

from PIL import ImageFont

from nps_lens.platform.resources import resource_root


@dataclass(frozen=True)
class EvidenceLayout:
    margin: float = 0.38
    body_top: float = 1.35
    body_bottom: float = 4.69
    metric_width: float = 2.60
    content_left: float = 3.55
    content_width: float = 5.83
    gap: float = 0.10
    padding: float = 0.12
    body_font: float = 11
    title_font: float = 20
    conclusion_top: float = 4.91
    conclusion_height: float = 0.39
    max_summary_topics: int = 4
    label_font: float = 10
    kpi_font: float = 30


EVIDENCE_LAYOUT = EvidenceLayout()


@lru_cache(maxsize=2)
def _evidence_font(bold: bool) -> ImageFont.FreeTypeFont:
    name = "Bold" if bold else "Book"
    return ImageFont.truetype(
        str(resource_root() / "assets" / "ppt" / "bbva" / "fonts" / f"BentonSansBBVA-{name}.ttf"),
        round(EVIDENCE_LAYOUT.body_font * 4),
    )


def wrap_evidence_text(value: str) -> list[str]:
    """Measure bold glyphs conservatively, including highlighted phrases."""
    font = _evidence_font(True)
    width = (EVIDENCE_LAYOUT.content_width - 2 * EVIDENCE_LAYOUT.padding) * 72 * 4 * 0.97
    lines: list[str] = []
    line = ""
    for word in value.split():
        candidate = f"{line} {word}" if line else word
        if line and font.getlength(candidate) > width:
            lines.append(line)
            line = ""
        while font.getlength(word) > width:
            low, high = 1, len(word)
            while low < high:
                middle = (low + high + 1) // 2
                if font.getlength(word[:middle]) <= width:
                    low = middle
                else:
                    high = middle - 1
            lines.append(word[:low])
            word = word[low:]
        line = f"{line} {word}" if line else word
    return lines + [line] if line or not lines else lines
