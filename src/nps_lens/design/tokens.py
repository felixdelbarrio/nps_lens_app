"""Design tokens.

This app follows BBVA Experience token naming (e.g. "color.primary.*").

Important:
- We do NOT ship design-spec PDFs, icon packs, or font assets in this repository.
- Tokens are centralized here as a curated subset used by the UI.
- If you have access to the official token package, swap the values here only.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True)
class DesignTokens:
    """Curated subset of tokens used by the app UI."""

    colors_light: dict[str, str]
    colors_dark: dict[str, str]

    @staticmethod
    def default() -> "DesignTokens":
        # Light palette (subset)
        light: dict[str, str] = {
            # Accents
            "color.primary.accent.value-01.default": "#85c8ff",
            "color.primary.accent.value-01.pressed": "#53a9ef",
            "color.primary.accent.value-05.default": "#ffe761",
            "color.primary.accent.value-07.default": "#ff5252",
            # Backgrounds / surfaces
            "color.primary.bg.alternative.default": "#ffffff",
            # App semantic surfaces (centralized; avoid hardcoding in pages)
            "color.app.surface.default": "#ffffff",
            "color.app.surface.raised": "#f7f8fa",
            "color.primary.bg.action.default": "#001391",
            "color.primary.bg.action.active": "#070e46",
            "color.primary.bg.selection.default": "#85c8ff",
            "color.primary.bg.bar": "#cad1d8",
            # Text
            "color.primary.text.primary": "#070e46",
            "color.primary.text.disabled": "#adb8c2",
            "color.primary.text.main-inverse.default": "#ffffff",
            "color.primary.text.action.default": "#001391",
            # Text on accent backgrounds
            "color.app.text.on-accent": "#070e46",
            # Status
            "color.primary.bg.success": "#42a64c",
            "color.primary.bg.warning": "#ffe761",
            "color.primary.bg.alert": "#c30a0a",
        }

        # Dark palette (subset). Values curated from the dark-mode spec.
        dark: dict[str, str] = {
            "color.primary.accent.value-01.default": "#85c8ff",
            "color.primary.accent.value-01.pressed": "#53a9ef",
            "color.primary.accent.value-05.default": "#ffe761",
            "color.primary.accent.value-07.default": "#ff5252",
            "color.primary.bg.alternative.default": "#11192d",
            "color.app.surface.default": "#11192d",
            "color.app.surface.raised": "#16203a",
            "color.primary.bg.action.default": "#001391",
            "color.primary.bg.action.active": "#070e46",
            "color.primary.bg.selection.default": "#334056",
            "color.primary.bg.bar": "#334056",
            "color.primary.text.primary": "#ffffff",
            "color.primary.text.disabled": "#46536d",
            "color.primary.text.main-inverse.default": "#ffffff",
            "color.primary.text.action.default": "#85c8ff",
            "color.app.text.on-accent": "#070e46",
            "color.primary.bg.success": "#42a64c",
            "color.primary.bg.warning": "#ffe761",
            "color.primary.bg.alert": "#c30a0a",
        }

        return DesignTokens(colors_light=light, colors_dark=dark)


@dataclass(frozen=True)
class TypographyTokens:
    display: str
    heading: str
    body: str
    medium: str


def bbva_typography_tokens() -> TypographyTokens:
    return TypographyTokens(
        display="Tiempos Headline",
        heading="BentonSansBBVA Bold",
        body="BentonSansBBVA Book",
        medium="BentonSansBBVA Medium",
    )


def palette(tokens: DesignTokens, mode: str) -> dict[str, str]:
    return tokens.colors_dark if mode == "dark" else tokens.colors_light


def primary_accent(tokens: DesignTokens, mode: str) -> str:
    return palette(tokens, mode)["color.primary.accent.value-01.default"]


def _hex_to_rgb(color: str) -> tuple[int, int, int]:
    code = str(color or "").strip().lstrip("#")
    if len(code) == 3:
        code = "".join(ch * 2 for ch in code)
    if len(code) != 6:
        return (0, 0, 0)
    return (int(code[0:2], 16), int(code[2:4], 16), int(code[4:6], 16))


def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
    r, g, b = [max(0, min(255, int(v))) for v in rgb]
    return f"#{r:02x}{g:02x}{b:02x}"


def mix_hex_colors(a: str, b: str, ratio_to_b: float) -> str:
    """Mix color ``a`` towards ``b``.

    ``ratio_to_b=0`` keeps ``a`` and ``ratio_to_b=1`` returns ``b``.
    """

    t = max(0.0, min(1.0, float(ratio_to_b)))
    ar, ag, ab = _hex_to_rgb(a)
    br, bg, bb = _hex_to_rgb(b)
    return _rgb_to_hex(
        (
            round(ar + (br - ar) * t),
            round(ag + (bg - ag) * t),
            round(ab + (bb - ab) * t),
        )
    )


def _coerce_float(value: object) -> float | None:
    try:
        score = float(str(value))
    except Exception:
        return None
    return score if isfinite(score) else None


def nps_score_band(value: object) -> str:
    """Map a raw NPS score (0-10) to its semantic band."""

    score = _coerce_float(value)
    if score is None:
        return "unknown"
    if score <= 6.0:
        return "detractor"
    if score >= 9.0:
        return "promoter"
    return "passive"


def nps_group_band(group_value: object, score_value: object | None = None) -> str:
    """Normalize explicit group labels to the same detractor/passive/promoter bands."""

    txt = str(group_value or "").strip().lower()
    if txt:
        if any(key in txt for key in ("detrac", "detractor")):
            return "detractor"
        if any(key in txt for key in ("neutro", "neutral", "passive", "pasivo", "passiv")):
            return "passive"
        if any(key in txt for key in ("promot", "promoter")):
            return "promoter"
    return nps_score_band(score_value)


def nps_semantic_palette(tokens: DesignTokens, mode: str) -> dict[str, str]:
    """Absolute NPS semantics for the 0-10 score domain.

    Critical/detractor stay in the red family, passives in warning,
    promoters in green. Never normalize these colors to local chart ranges.
    """

    p = palette(tokens, mode)
    return {
        "critical": p["color.primary.bg.alert"],
        "detractor": p["color.primary.accent.value-07.default"],
        "passive": p["color.primary.bg.warning"],
        "promoter": p["color.primary.bg.success"],
        "neutral": p["color.primary.bg.bar"],
        "line": p["color.primary.accent.value-01.default"],
        "text": p["color.primary.text.primary"],
        "surface": p.get("color.app.surface.default", p["color.primary.bg.alternative.default"]),
    }


def nps_score_color(tokens: DesignTokens, mode: str, value: object) -> str:
    """Absolute semantic color for a raw NPS score.

    0-2 uses the strongest alert red, 3-6 stays in the red family,
    7-8 uses warning, and 9-10 uses success.
    """

    sem = nps_semantic_palette(tokens, mode)
    score = _coerce_float(value)
    if score is None:
        return sem["neutral"]
    if score <= 2.0:
        return sem["critical"]
    if score <= 6.0:
        return sem["detractor"]
    if score <= 8.0:
        return sem["passive"]
    return sem["promoter"]


def nps_group_color(
    tokens: DesignTokens, mode: str, group_value: object, score_value: object = None
) -> str:
    """Semantic color for a detractor/passive/promoter label."""

    sem = nps_semantic_palette(tokens, mode)
    band = nps_group_band(group_value, score_value)
    if band == "detractor":
        return sem["detractor"]
    if band == "passive":
        return sem["passive"]
    if band == "promoter":
        return sem["promoter"]
    return sem["neutral"]


def nps_semantic_surface(tokens: DesignTokens, mode: str, band: str) -> str:
    """Soft background color for NPS-labeled UI surfaces."""

    sem = nps_semantic_palette(tokens, mode)
    base = sem.get(str(band or "").strip().lower(), sem["neutral"])
    return mix_hex_colors(base, sem["surface"], 0.82)


def plotly_nps_score_scale(tokens: DesignTokens, mode: str) -> list[list[object]]:
    """Absolute Plotly colorscale for 0-10 NPS scores.

    The scale encodes the business rule directly:
    - 0..2: critical red
    - 3..6: detractor red
    - 7..8: warning
    - 9..10: promoter green
    """

    sem = nps_semantic_palette(tokens, mode)
    return [
        [0.0, sem["critical"]],
        [0.2, sem["critical"]],
        [0.200001, sem["detractor"]],
        [0.6, sem["detractor"]],
        [0.600001, sem["passive"]],
        [0.8, sem["passive"]],
        [0.800001, sem["promoter"]],
        [1.0, sem["promoter"]],
    ]


def plotly_discrete_sequence(tokens: DesignTokens, mode: str) -> list[str]:
    """Discrete color sequence aligned to the design tokens.

    Use for categorical series (lines/bars). Order matters.
    """

    p = palette(tokens, mode)
    return [
        p["color.primary.accent.value-01.default"],
        p["color.primary.accent.value-05.default"],
        p["color.primary.accent.value-07.default"],
        p["color.primary.bg.success"],
        p["color.primary.bg.action.default"],
    ]


def plotly_continuous_scale(tokens: DesignTokens, mode: str) -> list[str]:
    """Sequential continuous scale (low→high) aligned to tokens.

    Intended for intensity/heatmaps. Uses existing token values only.
    """

    p = palette(tokens, mode)
    return [
        p["color.primary.bg.alternative.default"],
        p["color.primary.bg.bar"],
        p["color.primary.accent.value-01.default"],
        p["color.primary.bg.action.default"],
    ]


def plotly_risk_scale(tokens: DesignTokens, mode: str) -> list[list[object]]:
    """Risk/intensity continuous scale aligned to status tokens.

    Low values are neutral/background; high values move to warning and alert.
    Use for heatmaps / confidence / severity where higher == more risk/priority.

    Plotly accepts either a list of colors or a list of (stop, color) pairs.
    We use explicit stops to make the perceptual jump clear.
    """

    p = palette(tokens, mode)
    return [
        [0.0, p["color.primary.bg.alternative.default"]],
        [0.25, p["color.primary.bg.bar"]],
        [0.6, p["color.primary.bg.warning"]],
        [1.0, p["color.primary.bg.alert"]],
    ]


def cp_level_color(tokens: DesignTokens, mode: str, level: str) -> str:
    """Color for changepoint significance labels."""

    p = palette(tokens, mode)
    lv = (level or "").strip().lower()
    if lv == "high":
        return p["color.primary.bg.alert"]
    if lv == "medium":
        return p["color.primary.bg.warning"]
    return p["color.primary.bg.bar"]


def executive_report_palette(tokens: DesignTokens, mode: str = "light") -> dict[str, str]:
    """Presentation palette derived from the centralized token set."""

    p = palette(tokens, mode)
    return {
        "bg_dark": p["color.primary.bg.action.default"].lstrip("#").upper(),
        "bg_light": "F7F8F8",
        "line": p["color.primary.bg.bar"].lstrip("#").upper(),
        "ink": p["color.primary.text.primary"].lstrip("#").upper(),
        "muted": "42526E",
        "white": "FFFFFF",
        "blue": p["color.primary.bg.action.default"].lstrip("#").upper(),
        "sky": p["color.primary.accent.value-01.default"].lstrip("#").upper(),
        "green": "88E783",
        "amber": "D97706",
        "yellow": p["color.primary.bg.warning"].lstrip("#").upper(),
        "orange": "FFB56B",
        "red": p["color.primary.bg.alert"].lstrip("#").upper(),
        "sand": "F7F8F8",
        "navy": "000519",
    }
