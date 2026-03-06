#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image

ICO_SIZES = [(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate app icons for packaging.")
    parser.add_argument("--input", required=True, help="Input PNG image path")
    parser.add_argument("--out-dir", required=True, help="Output directory for generated icons")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    input_path = Path(args.input).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Input image not found: {input_path}")

    img = Image.open(input_path).convert("RGBA")
    img.save(out_dir / "app.png")
    img.save(out_dir / "app.ico", sizes=ICO_SIZES)
    img.save(out_dir / "app.icns")

    print(f"Generated icons in {out_dir}")


if __name__ == "__main__":
    main()
