"""
Render slide 5 — Sector Comparison.

Layout (portrait 7.5" × 13.33"):

    Header strip          : "<Company> | Sector Comparison"
    Title + accent rule   : "Sector Comparison"
    Sub-title             : "Sector: <Food Retail & Distribution>"

    Peer table (full width, ~10")
    ┌──────────────────────────────────────────────────────────────┐
    │ Company                  │ Cap (USD) │  YTD% │  1Y%  │ ESG   │
    ├──────────────────────────────────────────────────────────────┤
    │ █ SPINNEYS 1961 …        │  1.14B    │ -23.2 │ -22.7 │  —    │
    │   FOMENTO ECONÓMICO …    │ 37.93B    │ +14.6 │  +4.5 │  —    │
    │   CASEY'S GENERAL STORES │ 31.77B    │ +55.5 │ +89.9 │ AAA   │
    │   ...                    │           │       │       │       │
    └──────────────────────────────────────────────────────────────┘

    Footer: "Sector average YTD: +X%   |   Source: MarketScreener /sector/"

The first row (subject company) is highlighted with a light gold band so
the eye lands there first.
"""

from __future__ import annotations

from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

from src.models.report_context import SectorComparisonData


WHITE = RGBColor(0xFF, 0xFF, 0xFF)
BLACK = RGBColor(0x1F, 0x23, 0x28)
GOLD = RGBColor(0xC9, 0xA2, 0x27)
MUTED = RGBColor(0x8B, 0x94, 0x9E)
GREEN = RGBColor(0x3F, 0xB9, 0x50)
RED = RGBColor(0xCF, 0x22, 0x22)
SUBJECT_BG = RGBColor(0xFD, 0xF6, 0xE3)   # very light gold tint
ROW_ALT_BG = RGBColor(0xFA, 0xFB, 0xFC)
HEADER_BG = RGBColor(0xF1, 0xF3, 0xF5)
TABLE_BORDER = RGBColor(0xE3, 0xE6, 0xEA)


def _fmt_pct(v: float | None, *, signed: bool = True) -> str:
    if v is None:
        return "—"
    if signed:
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:.1f}%"
    return f"{v:.1f}%"


def _pct_color(v: float | None) -> RGBColor:
    if v is None:
        return MUTED
    if v > 0:
        return GREEN
    if v < 0:
        return RED
    return BLACK


def render(prs, blank_layout, sector: SectorComparisonData, *, tx, rect,
           company_name: str = "") -> None:
    """Render the Sector Comparison slide."""
    if not sector or not sector.has_data:
        return

    W = prs.slide_width
    s = prs.slides.add_slide(blank_layout)
    rect(s, 0, 0, W, prs.slide_height, WHITE)

    # ── Header ──
    head = (
        f"{company_name} | Sector Comparison"
        if company_name else "Sector Comparison"
    )
    tx(s, Inches(0.6), Inches(0.4), Inches(6.3), Inches(0.3),
       head, sz=12, bold=True, rgb=BLACK)
    tx(s, Inches(0.6), Inches(0.85), Inches(6), Inches(0.5),
       "Sector Comparison", sz=22, bold=True, rgb=BLACK)
    rect(s, Inches(0.6), Inches(1.32), Inches(0.9), Inches(0.04), GOLD)

    if sector.sector_label:
        tx(s, Inches(0.6), Inches(1.5), Inches(6.3), Inches(0.3),
           f"Sector: {sector.sector_label}", sz=11, rgb=MUTED)

    # ── Peer table ──
    tbl_x = Inches(0.6)
    tbl_y = Inches(2.0)
    tbl_w = Inches(6.3)
    row_h = Inches(0.36)
    header_h = Inches(0.42)

    # Column widths (sum = tbl_w = 6.3")
    col_widths = {
        "name": Inches(2.7),
        "cap":  Inches(0.95),
        "ytd":  Inches(0.85),
        "y1":   Inches(0.85),
        "y3":   Inches(0.55),
        "esg":  Inches(0.4),
    }
    cols = ["name", "cap", "ytd", "y1", "y3", "esg"]

    # Header row
    rect(s, tbl_x, tbl_y, tbl_w, header_h, HEADER_BG, TABLE_BORDER)
    cx = tbl_x
    headers = {
        "name": "Company",
        "cap":  "Cap (USD)",
        "ytd":  "YTD",
        "y1":   "1Y",
        "y3":   "3Y",
        "esg":  "ESG",
    }
    aligns = {
        "name": PP_ALIGN.LEFT,
        "cap":  PP_ALIGN.RIGHT,
        "ytd":  PP_ALIGN.RIGHT,
        "y1":   PP_ALIGN.RIGHT,
        "y3":   PP_ALIGN.RIGHT,
        "esg":  PP_ALIGN.CENTER,
    }
    for col in cols:
        tx(s, cx + Inches(0.08), tbl_y + Inches(0.1),
           col_widths[col] - Inches(0.16), Inches(0.25),
           headers[col], sz=9, bold=True, rgb=MUTED, al=aligns[col])
        cx += col_widths[col]

    # Body rows
    cursor_y = tbl_y + header_h
    for i, row in enumerate(sector.rows):
        bg = SUBJECT_BG if row.is_subject else (ROW_ALT_BG if i % 2 == 1 else WHITE)
        rect(s, tbl_x, cursor_y, tbl_w, row_h, bg, TABLE_BORDER)
        # Subject indicator: thin gold strip on left
        if row.is_subject:
            rect(s, tbl_x, cursor_y, Inches(0.06), row_h, GOLD)

        cx = tbl_x
        # Name (truncate aggressively to fit)
        display_name = row.name
        if len(display_name) > 32:
            display_name = display_name[:30].rstrip() + "…"
        tx(s, cx + Inches(0.12), cursor_y + Inches(0.07),
           col_widths["name"] - Inches(0.16), Inches(0.25),
           display_name, sz=9,
           bold=row.is_subject, rgb=BLACK, al=PP_ALIGN.LEFT)
        cx += col_widths["name"]

        # Cap
        tx(s, cx, cursor_y + Inches(0.07),
           col_widths["cap"] - Inches(0.08), Inches(0.25),
           row.market_cap_usd or "—", sz=9, rgb=BLACK, al=PP_ALIGN.RIGHT)
        cx += col_widths["cap"]

        # YTD %
        tx(s, cx, cursor_y + Inches(0.07),
           col_widths["ytd"] - Inches(0.08), Inches(0.25),
           _fmt_pct(row.change_ytd_pct), sz=9,
           rgb=_pct_color(row.change_ytd_pct), al=PP_ALIGN.RIGHT)
        cx += col_widths["ytd"]

        # 1Y %
        tx(s, cx, cursor_y + Inches(0.07),
           col_widths["y1"] - Inches(0.08), Inches(0.25),
           _fmt_pct(row.change_1y_pct), sz=9,
           rgb=_pct_color(row.change_1y_pct), al=PP_ALIGN.RIGHT)
        cx += col_widths["y1"]

        # 3Y %
        tx(s, cx, cursor_y + Inches(0.07),
           col_widths["y3"] - Inches(0.08), Inches(0.25),
           _fmt_pct(row.change_3y_pct), sz=8,
           rgb=_pct_color(row.change_3y_pct), al=PP_ALIGN.RIGHT)
        cx += col_widths["y3"]

        # ESG letter
        tx(s, cx, cursor_y + Inches(0.07),
           col_widths["esg"], Inches(0.25),
           row.esg_msci or "—", sz=9, bold=bool(row.esg_msci),
           rgb=GREEN if (row.esg_msci or "").startswith("A") else BLACK,
           al=PP_ALIGN.CENTER)

        cursor_y = cursor_y + row_h

    # ── Footer ──
    foot_text_parts = []
    if sector.average_ytd_pct is not None:
        foot_text_parts.append(f"Sector avg YTD: {_fmt_pct(sector.average_ytd_pct)}")
    foot_text_parts.append("Source: MarketScreener /sector/ + /ratings/")
    tx(s, Inches(0.6), prs.slide_height - Inches(0.45),
       Inches(6.3), Inches(0.3),
       "   |   ".join(foot_text_parts),
       sz=8, rgb=MUTED, al=PP_ALIGN.LEFT)
