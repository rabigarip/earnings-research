"""
Render slide 1 (the cover) of the earnings preview deck.

Consumes ONLY a `CoverData` plus the small set of pptx primitives the
rest of the deck shares (`tx`, `rect`, the colour palette, slide width).
This is the first slide migrated to the ReportContext contract; it is
deliberately small and copy-faithful to the legacy portrait cover so the
visual diff is zero on the regression suite.

What's in scope here:
- Pull every value from `cover` (no `payload` / `memo_data` access).
- Sign-aware UPSIDE colour (green ≥ 0, red < 0, muted None).
- Currency-prefixed market cap and target price.
- "—" sentinel for any missing field; never invent.

What's NOT in scope:
- Sector taxonomy (still uses whatever the seed provides).
- FX conversion (per design, deck is in local currency end-to-end).
- The "Recent Headlines" sidebar — that lives on slide 2.
"""

from __future__ import annotations

from datetime import datetime

from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches

from src.models.report_context import CoverData


# Colour palette — matches `_write_preview_pptx_portrait`. Re-declared here
# (not imported) because slide modules should be standalone; sharing happens
# through a `pptx_helpers` module in a later phase.
DARK = RGBColor(0x0D, 0x11, 0x17)
GOLD = RGBColor(0xC9, 0xA2, 0x27)
LIGHT = RGBColor(0xE6, 0xED, 0xF3)
MUTED = RGBColor(0x8B, 0x94, 0x9E)
GREEN = RGBColor(0x3F, 0xB9, 0x50)
RED = RGBColor(0xCF, 0x22, 0x22)
INNER_PANEL = RGBColor(0x10, 0x17, 0x22)


def _delta_color(v: float | None) -> RGBColor:
    """Sign-aware colour for percentage deltas."""
    if v is None:
        return MUTED
    return GREEN if v >= 0 else RED


def _format_market_cap(mcap: float | None, currency: str) -> str:
    """SAR 74.0B / USD 2.4T / "—". Never "Price: 0.893"."""
    if mcap is None:
        return "—"
    abs_v = abs(mcap)
    if abs_v >= 1e12:
        s = f"{mcap / 1e12:.1f}T"
    elif abs_v >= 1e9:
        s = f"{mcap / 1e9:.1f}B"
    elif abs_v >= 1e6:
        s = f"{mcap / 1e6:.0f}M"
    else:
        s = f"{mcap:,.0f}"
    return f"{currency} {s}" if currency else s


def _format_target(tgt: float | None, currency: str) -> str:
    if tgt is None:
        return "—"
    s = f"{tgt:,.2f}" if tgt != int(tgt) else f"{int(tgt):,}"
    return f"{currency} {s}" if currency else s


def _format_upside(pct: float | None) -> str:
    if pct is None:
        return "—"
    return f"{pct:+.1f}%"


def _format_report_date(iso_date: str | None) -> str:
    if not iso_date or len(iso_date) < 10:
        return "—"
    try:
        y, m, d = int(iso_date[:4]), int(iso_date[5:7]), int(iso_date[8:10])
        months = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
        if 1 <= m <= 12:
            return f"{d} {months[m - 1]} {y}"
    except (ValueError, IndexError):
        pass
    return iso_date


def render(prs, blank_layout, cover: CoverData, *, tx, rect) -> None:
    """Render the cover slide onto `prs`.

    `tx` and `rect` are the textbox / rectangle helpers defined in the
    parent module — same closures the legacy renderer uses, so we don't
    duplicate them. `prs` is a python-pptx Presentation already configured
    with portrait dimensions (7.5in × 13.33in).
    """
    W = prs.slide_width
    H = prs.slide_height

    s1 = prs.slides.add_slide(blank_layout)
    rect(s1, 0, 0, W, H, DARK)

    # Top eyebrow + accent rule
    tx(s1, Inches(0.6), Inches(0.5), Inches(6), Inches(0.3),
       "EARNINGS PREVIEW NOTE", sz=10, bold=True, rgb=GOLD)
    rect(s1, Inches(0.6), Inches(0.85), Inches(1.8), Inches(0.04), GOLD)

    # Headline + period sub-title
    tx(s1, Inches(0.6), Inches(1.3), Inches(6.3), Inches(1.2),
       cover.company_name or "—", sz=36, bold=True, rgb=LIGHT)
    tx(s1, Inches(0.6), Inches(2.6), Inches(6.3), Inches(0.5),
       cover.period_label or "—", sz=18, rgb=LIGHT)

    # Metadata strip
    rows = [
        ("Sector:", cover.sector or "—"),
        ("Ticker:", cover.ticker or "—"),
        ("Market Cap:", _format_market_cap(cover.market_cap, cover.currency)),
        ("Report Date:", _format_report_date(cover.report_date)),
    ]
    my = 3.5
    for i, (lb, vl) in enumerate(rows):
        y = Inches(my + i * 0.4)
        tx(s1, Inches(0.6), y, Inches(1.8), Inches(0.3), lb, sz=11, rgb=MUTED)
        tx(s1, Inches(2.1), y, Inches(5.0), Inches(0.3), vl, sz=11, bold=True, rgb=LIGHT)

    # Rating / Target / Upside cards
    target_str = _format_target(cover.target_price, cover.currency)
    upside_str = _format_upside(cover.upside_pct)
    upside_color = _delta_color(cover.upside_pct)

    cards = [
        ("RATING", cover.rating or "—", LIGHT),
        ("TARGET PRICE", target_str, LIGHT),
        ("UPSIDE", upside_str, upside_color),
    ]
    by = Inches(5.5)
    bw = Inches(6.3)
    for j, (lb, vl, col) in enumerate(cards):
        y = by + Inches(j * 1.05)
        rect(s1, Inches(0.6), y, bw, Inches(0.9), INNER_PANEL, GOLD)
        tx(s1, Inches(0.85), y + Inches(0.12), Inches(2), Inches(0.25),
           lb, sz=9, bold=True, rgb=MUTED)
        tx(s1, Inches(0.85), y + Inches(0.38), bw - Inches(0.5), Inches(0.4),
           vl, sz=22, bold=True, rgb=col)

    # Footer disclaimer
    tx(s1, Inches(0), Inches(12.9), W, Inches(0.3),
       "CONFIDENTIAL | For Institutional Clients Only",
       sz=9, rgb=MUTED, al=PP_ALIGN.CENTER)
