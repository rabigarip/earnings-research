"""
Render slide 1 (cover) — institutional-research style.

Redesigned 2026-05 from the original 5-section vertical sprawl that
left ~5 inches of empty dark void at the bottom. New layout uses the
full vertical real estate with a tighter typographic ladder and a
horizontal hero strip:

    ┌─────────────────────────────────────────────────────────────┐
    │  EARNINGS PREVIEW NOTE                                      │
    │  ─────                                                       │
    │                                                              │
    │  Company Name                                  (28pt bold)  │
    │  TICKER · Sector / Industry                    (12pt muted) │
    │  Q2 2026 Earnings Preview                      (16pt gold)  │
    │                                                              │
    │  ─────────────────────────────────────                      │
    │                                                              │
    │  ┌──RATING──┐  ┌──TARGET──┐  ┌──UPSIDE──┐                  │
    │  │   BUY    │  │ OMR 0.43 │  │  +3.8%   │                  │
    │  └──────────┘  └──────────┘  └──────────┘                  │
    │                                                              │
    │  ─────────────────────────────────────                      │
    │                                                              │
    │  Quick Stats — 2x3 grid of supporting metrics               │
    │  Last Close · Market Cap · Coverage · P/E FY-est · Yield ·   │
    │  Report Date                                                 │
    │                                                              │
    │  ─────────────────────────────────────                      │
    │                                                              │
    │  Source · Generated timestamp                                │
    │                                                              │
    │  CONFIDENTIAL  ·  For Institutional Clients Only            │
    └─────────────────────────────────────────────────────────────┘

What stayed:
- Dark background, gold accent, sign-coloured upside.
- "—" sentinel for any missing field; never invent.
- Pure consumer of CoverData (no payload reach-back).
"""

from __future__ import annotations

from datetime import datetime

from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

from src.models.report_context import CoverData


# Palette
DARK = RGBColor(0x0D, 0x11, 0x17)
DARK_PANEL = RGBColor(0x10, 0x17, 0x22)
GOLD = RGBColor(0xC9, 0xA2, 0x27)
GOLD_DIM = RGBColor(0x6B, 0x55, 0x14)
LIGHT = RGBColor(0xE6, 0xED, 0xF3)
WHITE_SOFT = RGBColor(0xF6, 0xF8, 0xFB)
MUTED = RGBColor(0x8B, 0x94, 0x9E)
MUTED_DIM = RGBColor(0x55, 0x5C, 0x66)
GREEN = RGBColor(0x3F, 0xB9, 0x50)
RED = RGBColor(0xCF, 0x22, 0x22)


def _delta_color(v: float | None) -> RGBColor:
    if v is None:
        return MUTED
    return GREEN if v >= 0 else RED


def _format_market_cap(mcap: float | None, currency: str) -> str:
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


def _format_price(price: float | None, currency: str) -> str:
    if price is None:
        return "—"
    # Sub-1 prices (Oman / Bangladesh) need 3 decimals; normal prices 2.
    if abs(price) < 1:
        s = f"{price:.3f}"
    else:
        s = f"{price:,.2f}"
    return f"{currency} {s}" if currency else s


def _format_upside(pct: float | None) -> str:
    if pct is None:
        return "—"
    return f"{pct:+.1f}%"


def _format_pct(pct: float | None) -> str:
    if pct is None:
        return "—"
    return f"{pct:.2f}%"


def _format_multiple(x: float | None) -> str:
    if x is None:
        return "—"
    return f"{x:.1f}x"


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


def _hero_card(s, *, x, y, w, h, label, value, value_color, tx, rect) -> None:
    """One of the three top-of-cover hero cards."""
    rect(s, x, y, w, h, DARK_PANEL, GOLD)
    # Label band — top portion of the card.
    tx(s, x + Inches(0.18), y + Inches(0.12),
       w - Inches(0.36), Inches(0.22),
       label, sz=8, bold=True, rgb=GOLD)
    # Value — large, bold, dominant.
    tx(s, x + Inches(0.18), y + Inches(0.40),
       w - Inches(0.36), h - Inches(0.50),
       value, sz=20, bold=True, rgb=value_color, al=PP_ALIGN.CENTER)


def _stat_cell(s, *, x, y, w, h, label, value, tx) -> None:
    """One cell in the Quick Stats 3x2 grid."""
    tx(s, x, y,
       w, Inches(0.18),
       label.upper(), sz=7, bold=True, rgb=MUTED)
    tx(s, x, y + Inches(0.20),
       w, Inches(0.35),
       value, sz=13, bold=True, rgb=LIGHT)


def render(prs, blank_layout, cover: CoverData, *, tx, rect) -> None:
    """Render the cover. `tx` and `rect` are the shared pptx helpers."""
    W = prs.slide_width
    H = prs.slide_height

    s = prs.slides.add_slide(blank_layout)
    rect(s, 0, 0, W, H, DARK)

    # ── Eyebrow ──
    tx(s, Inches(0.6), Inches(0.5), Inches(6.3), Inches(0.25),
       "EARNINGS PREVIEW NOTE", sz=10, bold=True, rgb=GOLD)
    rect(s, Inches(0.6), Inches(0.82), Inches(0.9), Inches(0.04), GOLD)

    # ── Hero title block ──
    tx(s, Inches(0.6), Inches(1.05), Inches(6.3), Inches(0.85),
       cover.company_name or "—", sz=28, bold=True, rgb=LIGHT)

    # Ticker · Sector — combined into a single muted line below the name.
    sub_parts = []
    if cover.ticker:
        sub_parts.append(cover.ticker)
    if cover.sector:
        sub_parts.append(cover.sector)
    sub_line = "  ·  ".join(sub_parts)
    tx(s, Inches(0.6), Inches(1.95), Inches(6.3), Inches(0.3),
       sub_line, sz=11, bold=True, rgb=MUTED)

    # Period label — gold so it pops as the deck's framing line.
    tx(s, Inches(0.6), Inches(2.30), Inches(6.3), Inches(0.4),
       cover.period_label or "—", sz=15, bold=True, rgb=GOLD)

    # Thin gold rule separating identity from hero stats.
    rect(s, Inches(0.6), Inches(2.95), Inches(6.3), Inches(0.015), GOLD_DIM)

    # ── Hero strip: 3 horizontal cards ──
    card_y = Inches(3.20)
    card_h = Inches(1.20)
    card_w = Inches(2.0)
    card_gap = Inches(0.15)
    total_w = card_w * 3 + card_gap * 2
    start_x = (W - total_w) / 2

    target_str = _format_target(cover.target_price, cover.currency)
    upside_str = _format_upside(cover.upside_pct)
    upside_col = _delta_color(cover.upside_pct)

    rating_label = "RATING"
    if cover.n_analysts:
        rating_label = f"RATING  ·  {cover.n_analysts} ANALYSTS"

    cards = [
        (rating_label, cover.rating or "—", LIGHT),
        ("TARGET PRICE", target_str, LIGHT),
        ("UPSIDE TO TARGET", upside_str, upside_col),
    ]
    for j, (lb, vl, col) in enumerate(cards):
        cx = start_x + (card_w + card_gap) * j
        _hero_card(
            s, x=cx, y=card_y, w=card_w, h=card_h,
            label=lb, value=vl, value_color=col, tx=tx, rect=rect,
        )

    # ── Quick Stats — 3x2 grid ──
    rect(s, Inches(0.6), Inches(4.85), Inches(6.3), Inches(0.015), GOLD_DIM)
    tx(s, Inches(0.6), Inches(5.00), Inches(6.3), Inches(0.25),
       "KEY DATA", sz=10, bold=True, rgb=GOLD)

    stat_y = Inches(5.40)
    cell_w = Inches(2.05)
    cell_h = Inches(0.65)
    col_gap = Inches(0.10)
    row_gap = Inches(0.20)

    stats = [
        ("Last Close",   _format_price(cover.last_close, cover.currency)),
        ("Market Cap",   _format_market_cap(cover.market_cap, cover.currency)),
        ("Report Date",  _format_report_date(cover.report_date)),
        ("P/E (FY est)", _format_multiple(cover.pe_fy_e)),
        ("Div. Yield",   _format_pct(cover.div_yield_pct)),
        ("Currency",     cover.currency or "—"),
    ]
    for i, (lb, vl) in enumerate(stats):
        col = i % 3
        row = i // 3
        cx = Inches(0.6) + (cell_w + col_gap) * col
        cy = stat_y + (cell_h + row_gap) * row
        _stat_cell(s, x=cx, y=cy, w=cell_w, h=cell_h,
                   label=lb, value=vl, tx=tx)

    # ── Mid-page tagline area (institutional touch) ──
    rect(s, Inches(0.6), Inches(8.25), Inches(6.3), Inches(0.015), GOLD_DIM)
    tx(s, Inches(0.6), Inches(8.45), Inches(6.3), Inches(0.3),
       "INDEPENDENT EARNINGS PREVIEW", sz=10, bold=True, rgb=GOLD)
    tx(s, Inches(0.6), Inches(8.80), Inches(6.3), Inches(2.5),
       "This document synthesises consensus expectations, analyst sentiment, "
       "and recent broker activity ahead of the upcoming earnings release. "
       "Source data is drawn from MarketScreener (S&P Global Market "
       "Intelligence) and Yahoo Finance, with qualitative commentary "
       "reviewed by an independent research process.",
       sz=10, rgb=LIGHT, line_spacing=1.35)

    # ── Footer ──
    rect(s, Inches(0.6), Inches(12.55), Inches(6.3), Inches(0.015), GOLD_DIM)
    gen_ts = datetime.now().strftime("%d %B %Y")
    tx(s, Inches(0.6), Inches(12.70), Inches(6.3), Inches(0.25),
       f"Generated {gen_ts}",
       sz=8, rgb=MUTED, al=PP_ALIGN.LEFT)
    tx(s, Inches(0), Inches(12.97), W, Inches(0.25),
       "CONFIDENTIAL  ·  For Institutional Clients Only",
       sz=8, bold=True, rgb=MUTED, al=PP_ALIGN.CENTER)
