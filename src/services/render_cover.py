"""
Render slide 1 (cover) — institutional research-note style.

Design intent: every band on the cover carries real data (no generic
abstracts). Six horizontal bands stacked top-to-bottom, separated by
thin gold rules, fill the full 13.3" canvas with no empty zones:

    BAND 1  Identity        Eyebrow + name + ticker · sector + period
    BAND 2  Hero stats      RATING / TARGET / UPSIDE (3 cards)
    BAND 3  Key data        Last Close · Mcap · Report Date · P/E · Yield · ⬇
    BAND 4  Performance     1d / 1w / 1m / 3m / 6m / YTD price-change cells
    BAND 5  Highlights      Up to 3 analyst-strength bullets (from MS /ratings/)
    BAND 6  Footer          Generated date · CONFIDENTIAL

A "—" sentinel appears for any missing field; we never invent fallbacks.
The renderer is a pure consumer of CoverData and does not reach back
into the payload.
"""

from __future__ import annotations

from datetime import datetime

from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

from src.models.report_context import CoverData


# ── Palette ──
# Two neutrals (deep dark + soft light) and one accent (gold). All other
# colors are derived from those (sign-coloured deltas for upside chips).
DARK = RGBColor(0x0D, 0x11, 0x17)
DARK_PANEL = RGBColor(0x10, 0x17, 0x22)
DARK_LINE = RGBColor(0x1E, 0x26, 0x32)        # slightly lighter than DARK; for thin rules
GOLD = RGBColor(0xC9, 0xA2, 0x27)
GOLD_DIM = RGBColor(0x6B, 0x55, 0x14)         # subdued gold, used for fine rules
LIGHT = RGBColor(0xE6, 0xED, 0xF3)
SOFT = RGBColor(0xB0, 0xB8, 0xC0)             # mid-grey for muted body text
MUTED = RGBColor(0x8B, 0x94, 0x9E)
GREEN = RGBColor(0x3F, 0xB9, 0x50)
RED = RGBColor(0xCF, 0x22, 0x22)


def _delta_color(v: float | None, *, on_dark: bool = True) -> RGBColor:
    """Sign-aware colour for percentage deltas. The dark cover background
    needs slightly desaturated greens/reds so they don't strobe."""
    if v is None:
        return SOFT if on_dark else MUTED
    return GREEN if v >= 0 else RED


def _fmt_market_cap(mcap: float | None, currency: str) -> str:
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


def _fmt_target(tgt: float | None, currency: str) -> str:
    if tgt is None:
        return "—"
    s = f"{tgt:,.2f}" if tgt != int(tgt) else f"{int(tgt):,}"
    return f"{currency} {s}" if currency else s


def _fmt_price(p: float | None, currency: str) -> str:
    if p is None:
        return "—"
    s = f"{p:.3f}" if abs(p) < 1 else f"{p:,.2f}"
    return f"{currency} {s}" if currency else s


def _fmt_upside(pct: float | None) -> str:
    if pct is None:
        return "—"
    return f"{pct:+.1f}%"


def _fmt_perf(pct: float | None) -> str:
    if pct is None:
        return "—"
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


def _fmt_pct(pct: float | None) -> str:
    if pct is None:
        return "—"
    return f"{pct:.2f}%"


def _fmt_multiple(x: float | None) -> str:
    if x is None:
        return "—"
    return f"{x:.1f}x"


def _fmt_report_date(iso_date: str | None) -> str:
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


def _band_rule(s, *, x, y, w, color, rect) -> None:
    """Thin horizontal rule used to separate bands."""
    rect(s, x, y, w, Inches(0.012), color)


def _eyebrow(s, *, x, y, w, text, tx) -> None:
    """Small uppercase gold band header — used at the top of each section."""
    tx(s, x, y, w, Inches(0.20),
       text, sz=8, bold=True, rgb=GOLD)


def _hero_card(s, *, x, y, w, h, label, value, value_color, tx, rect) -> None:
    """One of the three top hero cards (rating/target/upside)."""
    rect(s, x, y, w, h, DARK_PANEL, GOLD)
    tx(s, x + Inches(0.18), y + Inches(0.14),
       w - Inches(0.36), Inches(0.20),
       label, sz=8, bold=True, rgb=GOLD)
    tx(s, x + Inches(0.18), y + Inches(0.42),
       w - Inches(0.36), h - Inches(0.50),
       value, sz=20, bold=True, rgb=value_color, al=PP_ALIGN.CENTER)


def _stat_cell(s, *, x, y, w, label, value, tx, value_color=None) -> None:
    """One cell in the Key Data 6-column row.

    Compact 2-line layout: label on top (small uppercase muted), value
    below (medium bold light or sign-coloured for performance cells).
    """
    tx(s, x, y, w, Inches(0.18),
       label.upper(), sz=7, bold=True, rgb=MUTED)
    tx(s, x, y + Inches(0.20), w, Inches(0.30),
       value, sz=12, bold=True,
       rgb=value_color if value_color is not None else LIGHT)


def render(prs, blank_layout, cover: CoverData, *, tx, rect) -> None:
    """Render the cover slide. `tx` and `rect` are the shared pptx helpers."""
    W = prs.slide_width
    H = prs.slide_height

    s = prs.slides.add_slide(blank_layout)
    rect(s, 0, 0, W, H, DARK)

    L = Inches(0.6)            # left margin
    R_W = Inches(6.3)          # content width

    # ════════════════════════════════════════════════════════════════
    # BAND 1 — Identity (top ~2.7")
    # ════════════════════════════════════════════════════════════════
    tx(s, L, Inches(0.50), R_W, Inches(0.22),
       "EARNINGS PREVIEW NOTE", sz=9, bold=True, rgb=GOLD)
    rect(s, L, Inches(0.78), Inches(0.7), Inches(0.04), GOLD)

    # Company name — slightly tighter typography for an institutional feel.
    tx(s, L, Inches(1.05), R_W, Inches(0.85),
       cover.company_name or "—", sz=28, bold=True, rgb=LIGHT)

    sub_parts = []
    if cover.ticker:
        sub_parts.append(cover.ticker)
    if cover.sector:
        sub_parts.append(cover.sector)
    sub_line = "  ·  ".join(sub_parts)
    tx(s, L, Inches(1.95), R_W, Inches(0.28),
       sub_line, sz=11, bold=True, rgb=SOFT)

    tx(s, L, Inches(2.30), R_W, Inches(0.40),
       cover.period_label or "—", sz=15, bold=True, rgb=GOLD)

    _band_rule(s, x=L, y=Inches(2.85), w=R_W, color=GOLD_DIM, rect=rect)

    # ════════════════════════════════════════════════════════════════
    # BAND 2 — Hero stats (3 cards horizontal, ~3.05–4.30)
    # ════════════════════════════════════════════════════════════════
    _eyebrow(s, x=L, y=Inches(3.00), w=R_W, text="ANALYST CONSENSUS", tx=tx)

    card_y = Inches(3.30)
    card_h = Inches(1.10)
    card_w = Inches(2.0)
    card_gap = Inches(0.15)
    total_w = card_w * 3 + card_gap * 2
    start_x = (W - total_w) / 2

    rating_label = "RATING"
    if cover.n_analysts:
        rating_label = f"RATING  ·  {cover.n_analysts} ANALYSTS"
    cards = [
        (rating_label, cover.rating or "—", LIGHT),
        ("TARGET PRICE", _fmt_target(cover.target_price, cover.currency), LIGHT),
        ("UPSIDE TO TARGET", _fmt_upside(cover.upside_pct), _delta_color(cover.upside_pct)),
    ]
    for j, (lb, vl, col) in enumerate(cards):
        cx = start_x + (card_w + card_gap) * j
        _hero_card(s, x=cx, y=card_y, w=card_w, h=card_h,
                   label=lb, value=vl, value_color=col, tx=tx, rect=rect)

    _band_rule(s, x=L, y=Inches(4.55), w=R_W, color=GOLD_DIM, rect=rect)

    # ════════════════════════════════════════════════════════════════
    # BAND 3 — Key Data (one horizontal row of 6 cells, ~4.70–5.55)
    # ════════════════════════════════════════════════════════════════
    _eyebrow(s, x=L, y=Inches(4.70), w=R_W, text="KEY DATA", tx=tx)

    kd_y = Inches(5.00)
    cells = [
        ("Last Close",   _fmt_price(cover.last_close, cover.currency)),
        ("Market Cap",   _fmt_market_cap(cover.market_cap, cover.currency)),
        ("Report Date",  _fmt_report_date(cover.report_date)),
        ("P/E (FY est)", _fmt_multiple(cover.pe_fy_e)),
        ("Div. Yield",   _fmt_pct(cover.div_yield_pct)),
        ("Currency",     cover.currency or "—"),
    ]
    cell_w = (R_W - Inches(0.50)) / 6  # 6 cells with 0.10" gutters
    gutter = Inches(0.10)
    for i, (lb, vl) in enumerate(cells):
        cx = L + (cell_w + gutter) * i
        _stat_cell(s, x=cx, y=kd_y, w=cell_w, label=lb, value=vl, tx=tx)

    _band_rule(s, x=L, y=Inches(5.85), w=R_W, color=GOLD_DIM, rect=rect)

    # ════════════════════════════════════════════════════════════════
    # BAND 4 — Recent Performance (sign-coloured 6-cell row, ~6.00–6.85)
    # ════════════════════════════════════════════════════════════════
    perfs = [
        ("1 day",     cover.perf_1d_pct),
        ("1 week",    cover.perf_1w_pct),
        ("1 month",   cover.perf_1m_pct),
        ("3 months",  cover.perf_3m_pct),
        ("6 months",  cover.perf_6m_pct),
        ("YTD",       cover.perf_ytd_pct),
    ]
    has_perf = any(v is not None for _, v in perfs)
    if has_perf:
        _eyebrow(s, x=L, y=Inches(6.00), w=R_W, text="RECENT PERFORMANCE", tx=tx)
        pf_y = Inches(6.30)
        for i, (lb, val) in enumerate(perfs):
            cx = L + (cell_w + gutter) * i
            _stat_cell(s, x=cx, y=pf_y, w=cell_w,
                       label=lb, value=_fmt_perf(val), tx=tx,
                       value_color=_delta_color(val))
        _band_rule(s, x=L, y=Inches(7.15), w=R_W, color=GOLD_DIM, rect=rect)
        highlights_y = Inches(7.30)
    else:
        highlights_y = Inches(6.00)

    # ════════════════════════════════════════════════════════════════
    # BAND 5 — Analyst Highlights (real bullets, not generic abstract)
    # Up to 3 strengths from MS /ratings/. We crop very long bullets to
    # ~140 chars so each fits 2 lines max at the cover's body size.
    # ════════════════════════════════════════════════════════════════
    if cover.top_strengths:
        _eyebrow(s, x=L, y=highlights_y, w=R_W,
                 text="ANALYST HIGHLIGHTS", tx=tx)
        bx = L
        by = highlights_y + Inches(0.30)
        bullet_h = Inches(1.30)  # tight, fits ~3 bullets in 4 inches
        for i, bullet in enumerate(cover.top_strengths[:3]):
            y = by + bullet_h * i
            # Gold square indicator instead of a bullet glyph — feels
            # more institutional and clearer on the dark background.
            rect(s, bx, y + Inches(0.12), Inches(0.10), Inches(0.10), GOLD)
            text = bullet
            if len(text) > 200:
                text = text[:197].rstrip() + "…"
            tx(s, bx + Inches(0.25), y, R_W - Inches(0.25), bullet_h - Inches(0.10),
               text, sz=11, rgb=LIGHT, line_spacing=1.30)

    # ════════════════════════════════════════════════════════════════
    # BAND 6 — Footer (always at the very bottom)
    # ════════════════════════════════════════════════════════════════
    _band_rule(s, x=L, y=Inches(12.55), w=R_W, color=GOLD_DIM, rect=rect)
    gen_ts = datetime.now().strftime("%d %B %Y")
    tx(s, L, Inches(12.70), R_W, Inches(0.22),
       f"Source: MarketScreener · Yahoo Finance  ·  Generated {gen_ts}",
       sz=8, rgb=MUTED, al=PP_ALIGN.LEFT)
    tx(s, Inches(0), Inches(12.97), W, Inches(0.22),
       "CONFIDENTIAL  ·  For Institutional Clients Only",
       sz=8, bold=True, rgb=MUTED, al=PP_ALIGN.CENTER)
