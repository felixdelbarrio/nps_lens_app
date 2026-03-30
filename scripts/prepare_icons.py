#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image

ICO_SIZES = [(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
OUTPUT_SIZE = 1024
CONTENT_SCALE = 0.92

try:
    RESAMPLE = Image.Resampling.LANCZOS
except AttributeError:  # Pillow < 10
    RESAMPLE = Image.LANCZOS


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate app icons for packaging.")
    parser.add_argument("--input", required=True, help="Input PNG image path")
    parser.add_argument("--out-dir", required=True, help="Output directory for generated icons")
    return parser.parse_args()


def _square_crop_alpha(img: Image.Image) -> Image.Image:
    """Trim transparent margins and return a square icon source."""
    alpha = img.split()[-1]
    bbox = alpha.getbbox()
    if bbox:
        img = img.crop(bbox)

    w, h = img.size
    side = max(w, h)
    square = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    square.paste(img, ((side - w) // 2, (side - h) // 2), img)
    return square


def _build_canvas(img: Image.Image) -> Image.Image:
    """Normalize icon visual weight so macOS does not look undersized."""
    source = _square_crop_alpha(img)
    target_content = max(32, int(round(OUTPUT_SIZE * CONTENT_SCALE)))
    source = source.resize((target_content, target_content), RESAMPLE)

    canvas = Image.new("RGBA", (OUTPUT_SIZE, OUTPUT_SIZE), (0, 0, 0, 0))
    offset = ((OUTPUT_SIZE - target_content) // 2, (OUTPUT_SIZE - target_content) // 2)
    canvas.paste(source, offset, source)
    return canvas


def main() -> None:
    args = _parse_args()
    input_path = Path(args.input).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Input image not found: {input_path}")

    img = Image.open(input_path).convert("RGBA")
    canvas = _build_canvas(img)
    canvas.save(out_dir / "app.png")
    canvas.save(out_dir / "app.ico", sizes=ICO_SIZES)
    canvas.save(out_dir / "app.icns")

    print(f"Generated icons in {out_dir}")


if __name__ == "__main__":
    main()
