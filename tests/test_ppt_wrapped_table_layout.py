from __future__ import annotations

from nps_lens.reports.executive_ppt import (
    _build_wrapped_table_layout,
    _wrap_text_to_width,
)


def test_wrapped_table_layout_never_uses_ellipsis_and_increases_height() -> None:
    long_text = (
        "Pagos Transferencias Mostrar movimientos actualizados con descripción completa "
        "del tópico ancla y detalle funcional de extremo a extremo"
    )
    wrapped = _wrap_text_to_width(long_text + " …", column_width_in=1.15, font_size_pt=9.0)
    assert "…" not in wrapped
    assert "\n" in wrapped

    layout = _build_wrapped_table_layout(
        [[long_text, "Transferencias pagos firma", "Error funcional timeout"]],
        column_widths=[1.15, 1.10, 1.05],
        font_size_pt=9.0,
        min_row_height=0.35,
    )
    assert all("…" not in cell for row in layout.rows for cell in row)
    assert layout.row_heights[0] > 0.35
    assert layout.total_height == layout.row_heights[0]
