"""Design tokens.

This app follows BBVA Experience token naming (e.g. "color.primary.*").

Important:
- We do NOT ship design-spec PDFs, icon packs, or font assets in this repository.
- Tokens are centralized here as a curated subset used by the UI.
- If you have access to the official token package, swap the values here only.
"""

from __future__ import annotations

from dataclasses import dataclass


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


def palette(tokens: DesignTokens, mode: str) -> dict[str, str]:
    return tokens.colors_dark if mode == "dark" else tokens.colors_light


def primary_accent(tokens: DesignTokens, mode: str) -> str:
    return palette(tokens, mode)["color.primary.accent.value-01.default"]


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
        "bg_dark": "061B4E",
        "bg_light": "F4F7FB",
        "line": p["color.primary.bg.bar"].lstrip("#").upper(),
        "ink": p["color.primary.text.primary"].lstrip("#").upper(),
        "muted": "42526E",
        "white": "FFFFFF",
        "blue": p["color.primary.bg.action.default"].lstrip("#").upper(),
        "sky": p["color.primary.accent.value-01.default"].lstrip("#").upper(),
        "green": p["color.primary.bg.success"].lstrip("#").upper(),
        "amber": "D97706",
        "yellow": p["color.primary.bg.warning"].lstrip("#").upper(),
        "orange": "FB923C",
        "red": p["color.primary.bg.alert"].lstrip("#").upper(),
    }
