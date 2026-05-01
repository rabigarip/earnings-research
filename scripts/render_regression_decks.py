#!/usr/bin/env python3
"""
Render all 10 regression decks to /tmp/regression_decks/ for visual review.

Reads the same TICKER_CONFIGS used by `tests/test_regression_ten_tickers.py`
so the rendered PPTXs reflect the same fixture data the unit tests assert
against. Useful for hand-eyeballing the decks before tagging a release.

Usage:
    python -m scripts.render_regression_decks                  # all 10
    python -m scripts.render_regression_decks 2020.SR 1120.SR  # subset
    python -m scripts.render_regression_decks --pdf            # also export PDFs via Keynote (macOS)

Outputs:
    /tmp/regression_decks/<TICKER>_regression.pptx
    /tmp/regression_decks/<TICKER>_regression.pdf   (if --pdf)
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

# Allow `python scripts/render_regression_decks.py` from anywhere by adding
# the project root to sys.path.
_THIS = Path(__file__).resolve()
_ROOT = _THIS.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.test_regression_ten_tickers import TICKER_CONFIGS, _make_payload  # noqa: E402
from src.services.generate_report import _write_preview_pptx_portrait  # noqa: E402


# A short thesis for each ticker — keeps decks visually complete without
# needing a Gemini call. Real runs would source this from the LLM step.
_THESIS_TEXT = {
    "default": (
        "The company heads into its upcoming print rated by sell-side. The setup "
        "hinges on whether topline strength translates into earnings delivery; "
        "recent quarters have shown surprise variance. Operating drivers, capital "
        "allocation, and guidance commentary will frame the next leg. The key "
        "uncertainty is whether margins can sustain as comparisons get tougher."
    ),
}


def _render_one(ticker: str, out_dir: Path, also_pdf: bool) -> Path:
    cfg = TICKER_CONFIGS[ticker]
    payload = _make_payload(**cfg)
    out_pptx = out_dir / f"{ticker}_regression.pptx"
    iv = _THESIS_TEXT.get(ticker, _THESIS_TEXT["default"])
    _write_preview_pptx_portrait(payload, out_pptx, {}, iv, [], None)
    print(f"  ✓ {ticker:12s}  {out_pptx.stat().st_size:>7,} bytes  → {out_pptx}")

    if also_pdf and sys.platform == "darwin":
        out_pdf = out_dir / f"{ticker}_regression.pdf"
        # Quote paths defensively for the AppleScript heredoc.
        applescript = (
            'tell application "Keynote"\n'
            f'  set theDoc to open POSIX file "{out_pptx}"\n'
            f'  export theDoc to POSIX file "{out_pdf}" as PDF\n'
            '  close theDoc saving no\n'
            'end tell'
        )
        try:
            subprocess.run(
                ["osascript", "-e", applescript],
                check=True, capture_output=True, timeout=60,
            )
            print(f"               → {out_pdf} ({out_pdf.stat().st_size:,} bytes)")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            print(f"               PDF export failed: {exc}")

    return out_pptx


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument(
        "tickers", nargs="*",
        help="Specific tickers to render (default: all 10)",
    )
    ap.add_argument(
        "--pdf", action="store_true",
        help="Also export each PPTX to PDF via Keynote (macOS only)",
    )
    ap.add_argument(
        "--out", default="/tmp/regression_decks",
        help="Output directory (default: /tmp/regression_decks)",
    )
    ap.add_argument(
        "--clean", action="store_true",
        help="Wipe the output directory before rendering",
    )
    args = ap.parse_args()

    out_dir = Path(args.out)
    if args.clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    targets = args.tickers or list(TICKER_CONFIGS.keys())
    unknown = [t for t in targets if t not in TICKER_CONFIGS]
    if unknown:
        print(f"Unknown tickers: {unknown}", file=sys.stderr)
        print(f"Available: {list(TICKER_CONFIGS.keys())}", file=sys.stderr)
        return 2

    print(f"Rendering {len(targets)} regression deck(s) → {out_dir}/")
    for t in targets:
        _render_one(t, out_dir, args.pdf)

    print(f"\nDone. {len(targets)} deck(s) in {out_dir}/")
    if args.pdf:
        print("Use `open /tmp/regression_decks/` to inspect.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
