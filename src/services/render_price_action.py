"""
Render slide 6 — Price Action & Broker Activity.

Layout (portrait 7.5" × 13.33"):

    Header strip          : "<Company> | Price Action & Broker Activity"
    Title + accent rule   : "Price Action & Broker Activity"

    Performance grid (top row, ~3"): 7 cells in two rows
    ┌──────┬──────┬──────┬──────┬──────┬──────┬──────┐
    │ 1D   │ 1W   │ MTD  │ 1M   │ 3M   │ 6M   │ YTD  │
    │ -.85 │ -3.3 │ -1.7 │ -6.5 │ -26.6│ -27.5│ -23.2│
    └──────┴──────┴──────┴──────┴──────┴──────┴──────┘

    Course extremes panel (~2"): low-high ranges per period

    Recent Broker Actions panel (~5") — most recent 6, with date + headline
    Covering Brokers chip strip — comma-separated names (small, muted)

    Source chip (bottom)
"""

from __future__ import annotations

from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

from src.models.report_context import PriceActionData


WHITE = RGBColor(0xFF, 0xFF, 0xFF)
BLACK = RGBColor(0x1F, 0x23, 0x28)
GOLD = RGBColor(0xC9, 0xA2, 0x27)
MUTED = RGBColor(0x8B, 0x94, 0x9E)
GREEN = RGBColor(0x3F, 0xB9, 0x50)
RED = RGBColor(0xCF, 0x22, 0x22)
CARD_BG = RGBColor(0xFA, 0xF8, 0xF3)
CARD_BORDER = RGBColor(0xDB, 0xE0, 0xE6)
ROW_BG = RGBColor(0xFA, 0xFB, 0xFC)
TABLE_BORDER = RGBColor(0xE3, 0xE6, 0xEA)


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def _pct_color(v: float | None) -> RGBColor:
    if v is None:
        return MUTED
    if v > 0:
        return GREEN
    if v < 0:
        return RED
    return BLACK


def render(prs, blank_layout, price_action: PriceActionData, *, tx, rect,
           company_name: str = "", currency: str = "") -> None:
    """Render the Price Action & Broker Activity slide."""
    if not price_action or not price_action.has_data:
        return

    W = prs.slide_width
    s = prs.slides.add_slide(blank_layout)
    rect(s, 0, 0, W, prs.slide_height, WHITE)

    # ── Header ──
    head = (
        f"{company_name} | Price Action & Broker Activity"
        if company_name else "Price Action & Broker Activity"
    )
    tx(s, Inches(0.6), Inches(0.4), Inches(6.3), Inches(0.3),
       head, sz=12, bold=True, rgb=BLACK)
    tx(s, Inches(0.6), Inches(0.85), Inches(6), Inches(0.5),
       "Price Action & Broker Activity", sz=22, bold=True, rgb=BLACK)
    rect(s, Inches(0.6), Inches(1.35), Inches(2), Inches(0.06), GOLD)

    # ── Performance grid ──
    perf_y = Inches(1.7)
    perf_h = Inches(1.2)
    panel_w = Inches(6.3)
    cells = price_action.performance
    if cells:
        rect(s, Inches(0.6), perf_y, panel_w, perf_h, CARD_BG, CARD_BORDER)
        rect(s, Inches(0.6), perf_y, Inches(0.06), perf_h, GOLD)
        tx(s, Inches(0.78), perf_y + Inches(0.05),
           Inches(3), Inches(0.25),
           "PRICE PERFORMANCE", sz=9, bold=True, rgb=MUTED)
        n = len(cells)
        if n > 0:
            cell_w = (panel_w - Inches(0.4)) / n
            for i, c in enumerate(cells):
                cx = Inches(0.8) + cell_w * i
                tx(s, cx, perf_y + Inches(0.35),
                   cell_w, Inches(0.25),
                   c.label, sz=9, rgb=MUTED, al=PP_ALIGN.CENTER)
                tx(s, cx, perf_y + Inches(0.62),
                   cell_w, Inches(0.45),
                   _fmt_pct(c.value_pct),
                   sz=13, bold=True, rgb=_pct_color(c.value_pct),
                   al=PP_ALIGN.CENTER)

    # ── Course extremes panel ──
    ext_y = perf_y + perf_h + Inches(0.2)
    ext_h = Inches(1.5)
    extremes = [r for r in price_action.course_extremes if r.low is not None or r.high is not None]
    if extremes:
        rect(s, Inches(0.6), ext_y, panel_w, ext_h, CARD_BG, CARD_BORDER)
        rect(s, Inches(0.6), ext_y, Inches(0.06), ext_h, GOLD)
        tx(s, Inches(0.78), ext_y + Inches(0.05),
           Inches(3), Inches(0.25),
           "PRICE RANGE — LOW / HIGH", sz=9, bold=True, rgb=MUTED)

        n = min(len(extremes), 6)
        if n > 0:
            cell_w = (panel_w - Inches(0.4)) / n
            for i, r in enumerate(extremes[:n]):
                cx = Inches(0.8) + cell_w * i
                tx(s, cx, ext_y + Inches(0.35),
                   cell_w, Inches(0.25),
                   r.label, sz=9, rgb=MUTED, al=PP_ALIGN.CENTER)
                low_str = f"{r.low:.2f}" if r.low is not None else "—"
                high_str = f"{r.high:.2f}" if r.high is not None else "—"
                tx(s, cx, ext_y + Inches(0.62),
                   cell_w, Inches(0.3),
                   f"{low_str} / {high_str}",
                   sz=10, bold=True, rgb=BLACK, al=PP_ALIGN.CENTER)
                if currency:
                    tx(s, cx, ext_y + Inches(0.95),
                       cell_w, Inches(0.22),
                       currency, sz=8, rgb=MUTED, al=PP_ALIGN.CENTER)

    # ── Recent broker actions ──
    actions_y = ext_y + ext_h + Inches(0.25)
    actions_h = Inches(5.2)
    actions = price_action.broker_actions
    rect(s, Inches(0.6), actions_y, panel_w, actions_h, WHITE, CARD_BORDER)
    rect(s, Inches(0.6), actions_y, Inches(0.06), actions_h, GOLD)
    tx(s, Inches(0.78), actions_y + Inches(0.1),
       Inches(4.5), Inches(0.25),
       "RECENT BROKER ACTIONS", sz=10, bold=True, rgb=MUTED)

    if actions:
        cursor = actions_y + Inches(0.5)
        row_h = Inches(0.7)
        for action in actions[:6]:
            # Date column (small, muted)
            tx(s, Inches(0.8), cursor,
               Inches(0.9), Inches(0.3),
               action.date or "—", sz=9, bold=True, rgb=MUTED)
            # Headline (main content, wraps)
            line_count = max(1, (len(action.headline) // 60) + 1)
            wrap_h = Inches(0.22) * line_count + Inches(0.05)
            tx(s, Inches(1.75), cursor,
               panel_w - Inches(1.3), wrap_h,
               action.headline or "—", sz=10, rgb=BLACK, line_spacing=1.15)
            # Source chip
            if action.source:
                tx(s, Inches(0.8), cursor + Inches(0.32),
                   Inches(0.9), Inches(0.22),
                   action.source, sz=7, rgb=MUTED)
            # advance cursor and break if we'd overflow
            cursor = cursor + max(row_h, wrap_h + Inches(0.1))
            if cursor > actions_y + actions_h - Inches(0.6):
                break
    else:
        tx(s, Inches(0.78), actions_y + Inches(0.5),
           panel_w - Inches(0.3), Inches(0.4),
           "No recent broker actions on file.",
           sz=10, rgb=MUTED)

    # Covering brokers chip strip (bottom of actions panel)
    if price_action.covering_brokers:
        tx(s, Inches(0.78), actions_y + actions_h - Inches(0.5),
           panel_w - Inches(0.3), Inches(0.2),
           "COVERAGE", sz=8, bold=True, rgb=MUTED)
        brokers_str = "  ·  ".join(price_action.covering_brokers[:8])
        tx(s, Inches(0.78), actions_y + actions_h - Inches(0.3),
           panel_w - Inches(0.3), Inches(0.25),
           brokers_str, sz=9, rgb=BLACK)

    # ── Source ──
    tx(s, Inches(0.6), prs.slide_height - Inches(0.4),
       Inches(6.3), Inches(0.3),
       "Source: MarketScreener /consensus/ + summary page",
       sz=8, rgb=MUTED, al=PP_ALIGN.LEFT)
