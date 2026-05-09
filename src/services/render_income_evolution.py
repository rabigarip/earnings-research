"""
Render the new "Income Statement Evolution & Surprise" slide.

Two stacked panels in portrait:

    ┌───────────────────────────────────────────────────────────────┐
    │  Income Statement Evolution — Quarterly                       │
    │                                                                │
    │  [clustered bar chart: Sales / Operating Profit / Net Income] │
    │                                                                │
    │  Net Margin (avg): XX.X%  |  Operating Margin (avg): YY.Y%    │
    └───────────────────────────────────────────────────────────────┘

    ┌───────────────────────────────────────────────────────────────┐
    │  Quarterly Revenue — Rate of Surprise                         │
    │                                                                │
    │  [paired bar chart: Sales Actual / Sales Estimate]            │
    │                                                                │
    │  Beat-rate: X / Y quarters  |  Avg surprise: +Z.Z%            │
    └───────────────────────────────────────────────────────────────┘

Source: MarketScreener /finances/ (quarterly tab) + /consensus/

Each panel suppresses independently. The slide is gated upstream by
`income_evolution.has_data`; defensive None checks here are belt-and-
braces.
"""

from __future__ import annotations

from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

from src.models.report_context import IncomeEvolutionData
from src.services.chart_builders import (
    build_quarterly_income_chart,
    build_surprise_chart,
)


WHITE = RGBColor(0xFF, 0xFF, 0xFF)
BLACK = RGBColor(0x1F, 0x23, 0x28)
GOLD = RGBColor(0xC9, 0xA2, 0x27)
MUTED = RGBColor(0x8B, 0x94, 0x9E)
GREEN = RGBColor(0x3F, 0xB9, 0x50)
RED = RGBColor(0xCF, 0x22, 0x22)
CARD_BORDER = RGBColor(0xDB, 0xE0, 0xE6)


def _avg(values):
    nums = [v for v in (values or []) if isinstance(v, (int, float))]
    if not nums:
        return None
    return sum(nums) / len(nums)


def render(prs, blank_layout, income: IncomeEvolutionData, *, tx, rect,
           company_name: str = "") -> None:
    """Render the Income Statement Evolution & Surprise slide."""
    if not income or not income.has_data:
        return

    W = prs.slide_width
    H = prs.slide_height
    s = prs.slides.add_slide(blank_layout)
    rect(s, 0, 0, W, H, WHITE)

    # ── Header ──
    head = (
        f"{company_name} | Income Statement Evolution"
        if company_name else "Income Statement Evolution"
    )
    tx(s, Inches(0.6), Inches(0.4), Inches(6.3), Inches(0.3),
       head, sz=12, bold=True, rgb=BLACK)
    tx(s, Inches(0.6), Inches(0.85), Inches(6.3), Inches(0.5),
       "Income Statement Evolution & Surprise", sz=22, bold=True, rgb=BLACK)
    rect(s, Inches(0.6), Inches(1.35), Inches(2), Inches(0.06), GOLD)

    # ── Panel 1: Quarterly Income Statement ──
    p1_y = Inches(1.7)
    p1_h = Inches(5.0)
    qi = income.quarterly_income
    if qi is not None:
        rect(s, Inches(0.6), p1_y, Inches(6.3), Inches(0.35),
             RGBColor(0xFA, 0xF8, 0xF3))
        tx(s, Inches(0.78), p1_y + Inches(0.07),
           Inches(6.0), Inches(0.25),
           f"QUARTERLY INCOME STATEMENT  ·  {qi.units_label or ''}M",
           sz=9, bold=True, rgb=MUTED)
        # Chart fills the rest of the panel.
        build_quarterly_income_chart(
            s,
            Inches(0.5), p1_y + Inches(0.45),
            Inches(6.5), p1_h - Inches(1.0),
            qi.periods, qi.revenue, qi.ebit, qi.net_income,
            operating_margin_pct=qi.operating_margin_pct,
            net_margin_pct=qi.net_margin_pct,
            actuals_boundary=qi.actuals_boundary,
            units_label=qi.units_label,
        )
        # Margin summary chip below the chart.
        avg_op = _avg(qi.operating_margin_pct)
        avg_nm = _avg(qi.net_margin_pct)
        margin_y = p1_y + p1_h - Inches(0.4)
        margin_parts = []
        if avg_op is not None:
            margin_parts.append(f"Operating Margin avg: {avg_op:.1f}%")
        if avg_nm is not None:
            margin_parts.append(f"Net Margin avg: {avg_nm:.1f}%")
        margin_text = "   |   ".join(margin_parts) if margin_parts else ""
        if margin_text:
            tx(s, Inches(0.78), margin_y, Inches(6.0), Inches(0.25),
               margin_text, sz=9, bold=True, rgb=BLACK)
    else:
        # Quietly suppress the panel header when this side has no data.
        pass

    # ── Panel 2: Quarterly Surprise ──
    p2_y = Inches(7.0)
    p2_h = Inches(4.5)
    qs = income.quarterly_surprise
    if qs is not None:
        rect(s, Inches(0.6), p2_y, Inches(6.3), Inches(0.35),
             RGBColor(0xFA, 0xF8, 0xF3))
        tx(s, Inches(0.78), p2_y + Inches(0.07),
           Inches(6.0), Inches(0.25),
           f"QUARTERLY REVENUE — RATE OF SURPRISE  ·  {qs.units_label or ''}M",
           sz=9, bold=True, rgb=MUTED)

        build_surprise_chart(
            s,
            Inches(0.5), p2_y + Inches(0.45),
            Inches(6.5), p2_h - Inches(1.4),
            qs.periods, qs.actual, qs.estimate, qs.surprise_pct,
            units_label=qs.units_label,
        )

        # Beat-rate + average surprise chip — one line per metric so the
        # reader can compare Sales vs Net Income at a glance. MS often
        # publishes a Sales beat alongside a Net income miss (or vice
        # versa); collapsing them into one chip would hide the divergence.
        def _fmt_chip(label: str, surprises: list) -> tuple[str, "RGBColor"]:
            beats = sum(1 for sp in surprises
                        if isinstance(sp, (int, float)) and sp > 0)
            misses = sum(1 for sp in surprises
                         if isinstance(sp, (int, float)) and sp < 0)
            zeros = sum(1 for sp in surprises
                        if isinstance(sp, (int, float)) and sp == 0)
            total = beats + misses + zeros
            avg = _avg(surprises)
            parts = [label]
            if total > 0:
                parts.append(f"Beat {beats}/{total}")
                if misses:
                    parts.append(f"Miss {misses}/{total}")
            if avg is not None:
                sign = "+" if avg >= 0 else ""
                parts.append(f"Avg {sign}{avg:.1f}%")
            color = GREEN if (avg or 0) >= 0 else RED
            return ("   ·   ".join(parts), color)

        chip_y = p2_y + p2_h - Inches(0.65)
        sales_chip, sales_color = _fmt_chip("SALES", qs.surprise_pct)
        if any(isinstance(sp, (int, float)) for sp in qs.surprise_pct):
            tx(s, Inches(0.78), chip_y, Inches(6.0), Inches(0.22),
               sales_chip, sz=9, bold=True, rgb=sales_color)
        # Net income chip: only render when MS published it (banks +
        # most industrials publish; some thinly-covered tickers don't).
        if any(isinstance(sp, (int, float)) for sp in qs.net_income_surprise_pct):
            ni_chip, ni_color = _fmt_chip("NET INCOME", qs.net_income_surprise_pct)
            tx(s, Inches(0.78), chip_y + Inches(0.25),
               Inches(6.0), Inches(0.22),
               ni_chip, sz=9, bold=True, rgb=ni_color)

    # ── Source chip ──
    tx(s, Inches(0.6), H - Inches(0.4), Inches(6.3), Inches(0.3),
       "Source: MarketScreener /finances/ (quarterly) + /consensus/",
       sz=8, rgb=MUTED, al=PP_ALIGN.LEFT)
