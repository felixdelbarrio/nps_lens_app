from __future__ import annotations

from dataclasses import dataclass

from nps_lens.design.tokens import DesignTokens, palette


@dataclass(frozen=True)
class Theme:
    mode: str  # "light" | "dark"
    bg: str
    surface: str
    surface_2: str
    text: str
    muted: str
    border: str
    chart_paper: str
    chart_plot: str
    chart_grid: str
    chart_zero_line: str
    table_bg: str
    table_bg_alt: str
    table_bg_hover: str
    table_header_bg: str
    table_header_text: str
    table_border: str
    control_bg: str
    control_bg_hover: str
    control_border: str
    control_text: str
    control_placeholder: str
    control_icon: str
    control_menu_bg: str
    control_menu_item_hover: str
    control_menu_item_selected: str
    accent: str
    on_accent: str
    brand: str
    on_brand: str
    danger: str
    danger_soft: str
    warning: str
    success: str


def get_theme(mode: str) -> Theme:
    toks = DesignTokens.default()
    p = palette(toks, mode)

    # Map tokens to app-level semantics (no hardcoded colors outside tokens).
    bg = p["color.primary.bg.alternative.default"]
    surface = p.get("color.app.surface.default", bg)
    surface_2 = p.get("color.app.surface.raised", surface)

    text = p["color.primary.text.primary"]
    muted = p["color.primary.text.disabled"]
    border = p["color.primary.bg.bar"]
    chart_paper = p.get("color.app.chart.paper", surface)
    chart_plot = p.get("color.app.chart.plot", surface_2)
    chart_grid = p.get("color.app.chart.grid", border)
    chart_zero_line = p.get("color.app.chart.zero-line", chart_grid)
    table_bg = p.get("color.app.table.bg", surface)
    table_bg_alt = p.get("color.app.table.bg.alt", surface_2)
    table_bg_hover = p.get("color.app.table.bg.hover", table_bg_alt)
    table_header_bg = p.get("color.app.table.header.bg", surface_2)
    table_header_text = p.get("color.app.table.header.text", muted)
    table_border = p.get("color.app.table.border", border)
    accent = p["color.primary.accent.value-01.default"]
    control_bg = p.get("color.app.control.bg", surface_2)
    control_bg_hover = p.get("color.app.control.bg.hover", control_bg)
    control_border = p.get("color.app.control.border", border)
    control_text = p.get("color.app.control.text", text)
    control_placeholder = p.get("color.app.control.placeholder", muted)
    control_icon = p.get(
        "color.app.control.icon", p.get("color.primary.text.action.default", accent)
    )
    control_menu_bg = p.get("color.app.control.menu.bg", surface_2)
    control_menu_item_hover = p.get("color.app.control.menu.item.hover", control_bg_hover)
    control_menu_item_selected = p.get(
        "color.app.control.menu.item.selected", control_menu_item_hover
    )
    on_accent = p.get(
        "color.app.text.on-accent", p.get("color.primary.text.main-inverse.default", text)
    )
    brand = p["color.primary.bg.action.default"]
    on_brand = p.get("color.primary.text.main-inverse.default", text)
    danger = p["color.primary.bg.alert"]
    danger_soft = p["color.primary.accent.value-07.default"]
    warning = p["color.primary.bg.warning"]
    success = p["color.primary.bg.success"]
    return Theme(
        mode=mode,
        bg=bg,
        surface=surface,
        surface_2=surface_2,
        text=text,
        muted=muted,
        border=border,
        chart_paper=chart_paper,
        chart_plot=chart_plot,
        chart_grid=chart_grid,
        chart_zero_line=chart_zero_line,
        table_bg=table_bg,
        table_bg_alt=table_bg_alt,
        table_bg_hover=table_bg_hover,
        table_header_bg=table_header_bg,
        table_header_text=table_header_text,
        table_border=table_border,
        control_bg=control_bg,
        control_bg_hover=control_bg_hover,
        control_border=control_border,
        control_text=control_text,
        control_placeholder=control_placeholder,
        control_icon=control_icon,
        control_menu_bg=control_menu_bg,
        control_menu_item_hover=control_menu_item_hover,
        control_menu_item_selected=control_menu_item_selected,
        accent=accent,
        on_accent=on_accent,
        brand=brand,
        on_brand=on_brand,
        danger=danger,
        danger_soft=danger_soft,
        warning=warning,
        success=success,
    )
