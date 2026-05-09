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
    rect(s, Inches(0.6), Inches(1.32), Inches(0.9), Inches(0.04), GOLD)

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
    # Auto-sized to content. Previous fixed 5.2-inch panel left a huge
    # white box on tickers MS hasn't published broker actions for
    # (NBOB.OM, smaller listings) and the empty space wasn't pulling
    # any informational weight.
    actions_y = ext_y + ext_h + Inches(0.25)
    actions = price_action.broker_actions
    H = prs.slide_height
    available_to_footer = H - actions_y - Inches(0.55)  # reserve footer space

    if actions:
        # Estimate panel height from the rows we'll actually render.
        row_heights_in = []
        for action in actions[:6]:
            line_count = max(1, (len(action.headline or "") // 60) + 1)
            row_heights_in.append(0.22 * line_count + 0.18)
        body_h_in = sum(row_heights_in)
        cov_h_in = 0.55 if price_action.covering_brokers else 0.0
        actions_h = Inches(0.50 + body_h_in + cov_h_in)
        actions_h = min(actions_h, available_to_footer)

        rect(s, Inches(0.6), actions_y, panel_w, actions_h, WHITE, CARD_BORDER)
        rect(s, Inches(0.6), actions_y, Inches(0.06), actions_h, GOLD)
        tx(s, Inches(0.78), actions_y + Inches(0.10),
           Inches(4.5), Inches(0.22),
           "RECENT BROKER ACTIONS", sz=9, bold=True, rgb=MUTED)
        cursor = actions_y + Inches(0.40)
        for i, action in enumerate(actions[:6]):
            row_h = Inches(row_heights_in[i])
            tx(s, Inches(0.80), cursor,
               Inches(0.9), Inches(0.25),
               action.date or "—", sz=9, bold=True, rgb=MUTED)
            tx(s, Inches(1.75), cursor,
               panel_w - Inches(1.30), row_h - Inches(0.05),
               action.headline or "—", sz=10, rgb=BLACK, line_spacing=1.15)
            if action.source:
                tx(s, Inches(0.80), cursor + Inches(0.30),
                   Inches(0.9), Inches(0.22),
                   action.source, sz=7, rgb=MUTED)
            cursor = cursor + row_h
            if cursor > actions_y + actions_h - Inches(0.50):
                break

        if price_action.covering_brokers:
            cov_y = actions_y + actions_h - Inches(0.50)
            tx(s, Inches(0.78), cov_y, panel_w - Inches(0.30), Inches(0.18),
               "COVERAGE", sz=8, bold=True, rgb=MUTED)
            brokers_str = "  ·  ".join(price_action.covering_brokers[:8])
            tx(s, Inches(0.78), cov_y + Inches(0.20),
               panel_w - Inches(0.30), Inches(0.25),
               brokers_str, sz=9, rgb=BLACK)
    elif price_action.recent_quotes:
        # When MS hasn't published broker actions, fall back to the
        # recent-quotes table — a richer use of the same vertical band.
        # Each row carries date, price, change, volume.
        quotes = price_action.recent_quotes[:8]
        actions_h = Inches(0.40 + 0.30 * len(quotes) + 0.20)
        rect(s, Inches(0.6), actions_y, panel_w, actions_h, WHITE, CARD_BORDER)
        rect(s, Inches(0.6), actions_y, Inches(0.06), actions_h, GOLD)
        tx(s, Inches(0.78), actions_y + Inches(0.10),
           Inches(4.5), Inches(0.22),
           "RECENT TRADING ACTIVITY", sz=9, bold=True, rgb=MUTED)

        # Header row
        head_y = actions_y + Inches(0.40)
        cols = [
            ("Date",   Inches(0.78), Inches(1.20), PP_ALIGN.LEFT),
            ("Price",  Inches(2.00), Inches(1.50), PP_ALIGN.RIGHT),
            ("Change", Inches(3.55), Inches(1.20), PP_ALIGN.RIGHT),
            ("Volume", Inches(4.85), Inches(1.95), PP_ALIGN.RIGHT),
        ]
        for label, cx, cw, al in cols:
            tx(s, cx, head_y, cw, Inches(0.18),
               label, sz=8, bold=True, rgb=MUTED, al=al)

        cy = head_y + Inches(0.22)
        for q in quotes:
            for label, cx, cw, al in cols:
                value = ""
                if label == "Date":
                    value = q.get("date") or "—"
                elif label == "Price":
                    value = q.get("price") or "—"
                elif label == "Change":
                    cp = q.get("change_pct")
                    value = (f"+{cp:.2f}%" if isinstance(cp, (int, float)) and cp >= 0
                             else (f"{cp:.2f}%" if isinstance(cp, (int, float)) else "—"))
                elif label == "Volume":
                    value = q.get("volume") or "—"
                color = BLACK
                if label == "Change":
                    cp = q.get("change_pct")
                    color = (GREEN if isinstance(cp, (int, float)) and cp > 0
                             else (RED if isinstance(cp, (int, float)) and cp < 0
                                   else BLACK))
                tx(s, cx, cy, cw, Inches(0.22),
                   value, sz=9, rgb=color, al=al,
                   bold=(label == "Price"))
            cy = cy + Inches(0.28)
    else:
        # Genuinely no data: small unobtrusive note instead of a 5-inch panel.
        actions_h = Inches(0.80)
        rect(s, Inches(0.6), actions_y, panel_w, actions_h,
             RGBColor(0xFA, 0xF8, 0xF3), CARD_BORDER)
        rect(s, Inches(0.6), actions_y, Inches(0.06), actions_h, GOLD)
        tx(s, Inches(0.78), actions_y + Inches(0.10),
           Inches(4.5), Inches(0.22),
           "BROKER ACTIVITY", sz=9, bold=True, rgb=MUTED)
        tx(s, Inches(0.78), actions_y + Inches(0.40),
           panel_w - Inches(0.30), Inches(0.30),
           "No recent broker actions on file.",
           sz=10, rgb=MUTED)

    # ── Source ──
    tx(s, Inches(0.6), prs.slide_height - Inches(0.4),
       Inches(6.3), Inches(0.3),
       "Source: MarketScreener /consensus/ + summary page",
       sz=8, rgb=MUTED, al=PP_ALIGN.LEFT)
