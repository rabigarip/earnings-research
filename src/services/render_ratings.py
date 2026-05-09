"""
Render slide 4 — Ratings & Sentiment.

Layout (portrait 7.5" × 13.33"):

    Header strip          : "<Company> | Ratings & Sentiment"
    Title + accent rule   : "Ratings & Sentiment"

    Surperformance Ratings panel (top, ~2.5")
    ┌─────────────────────────────────────────────────────────┐
    │  TRADER  93   │  INVESTOR 26  │  GLOBAL 31  │ QUALITY 22│
    │  ████████░    │  ██░░░░░░░    │ ███░░░░░    │ ██░░░░░    │
    │              ESG MSCI: BBB  (or "—")                    │
    └─────────────────────────────────────────────────────────┘

    Strengths (left half, ~5") | Weaknesses (right half, ~5")
    ┌─────────────────────────┐ ┌─────────────────────────┐
    │  ▌ Strengths            │ │  ▌ Weaknesses           │
    │  • bullet 1             │ │  • bullet 1             │
    │  • bullet 2             │ │  • bullet 2             │
    │  ...                    │ │  ...                    │
    └─────────────────────────┘ └─────────────────────────┘

    Source chip (bottom-left) : "Source: MarketScreener /ratings/"

The slide is rendered only when `ratings.has_data` is True; the deck
builder gates the call. Renderer code does NOT reach back into payload.
"""

from __future__ import annotations

from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

from src.models.report_context import RatingsData


# Palette — matches render_summary.py
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
BLACK = RGBColor(0x1F, 0x23, 0x28)
GOLD = RGBColor(0xC9, 0xA2, 0x27)
MUTED = RGBColor(0x8B, 0x94, 0x9E)
GREEN = RGBColor(0x3F, 0xB9, 0x50)
RED = RGBColor(0xCF, 0x22, 0x22)
LIGHT = RGBColor(0xE6, 0xED, 0xF3)

CARD_BG = RGBColor(0xFA, 0xF8, 0xF3)
CARD_BORDER = RGBColor(0xDB, 0xE0, 0xE6)
STRENGTH_BG = RGBColor(0xF0, 0xF9, 0xF0)
STRENGTH_BAR = RGBColor(0x1A, 0x7F, 0x37)
WEAKNESS_BG = RGBColor(0xFE, 0xF0, 0xF0)
WEAKNESS_BAR = RGBColor(0xCF, 0x22, 0x22)
RATING_BAR_TRACK = RGBColor(0xEE, 0xEF, 0xF1)


def _score_color(score: int | None) -> RGBColor:
    """Color the filled bar portion based on the score band.

    < 40   = red (weak)
    40-69  = neutral grey (mid)
    >= 70  = green (strong)
    None   = neutral
    """
    if score is None:
        return MUTED
    if score >= 70:
        return GREEN
    if score < 40:
        return RED
    return MUTED


def _render_composite_bar(
    s, *, x, y, w, h, label: str, score: int | None, tx, rect
) -> None:
    """Draw one composite-rating block: label + score + horizontal bar.

    Layout: label on top, score on the right of the label, bar fills the
    remaining width below.
    """
    # Label (left) + score (right) on same row
    tx(s, x, y, w - Inches(0.6), Inches(0.28),
       label.upper(), sz=10, bold=True, rgb=MUTED)
    score_str = f"{score}" if score is not None else "—"
    tx(s, x + w - Inches(0.6), y, Inches(0.6), Inches(0.28),
       score_str, sz=14, bold=True, rgb=BLACK, al=PP_ALIGN.RIGHT)

    # Bar track (full width) and filled portion (proportional to score)
    bar_y = y + Inches(0.42)
    bar_h = Inches(0.18)
    rect(s, x, bar_y, w, bar_h, RATING_BAR_TRACK)
    if score is not None and score > 0:
        fill_w = w * (max(0, min(100, score)) / 100.0)
        rect(s, x, bar_y, fill_w, bar_h, _score_color(score))


def render(prs, blank_layout, ratings: RatingsData, *, tx, rect,
           company_name: str = "") -> None:
    """Render the Ratings & Sentiment slide.

    `tx` and `rect` are the textbox / rect helpers from generate_report.py
    (closures with the right styling defaults). `prs` must be portrait.
    """
    if not ratings or not ratings.has_data:
        return  # gated by caller; defensive double-check

    W = prs.slide_width
    s = prs.slides.add_slide(blank_layout)
    rect(s, 0, 0, W, prs.slide_height, WHITE)

    # ── Header ──
    head = (
        f"{company_name} | Ratings & Sentiment"
        if company_name else "Ratings & Sentiment"
    )
    tx(s, Inches(0.6), Inches(0.4), Inches(6.3), Inches(0.3),
       head, sz=12, bold=True, rgb=BLACK)
    tx(s, Inches(0.6), Inches(0.85), Inches(6), Inches(0.5),
       "Ratings & Sentiment", sz=26, bold=True, rgb=BLACK)
    rect(s, Inches(0.6), Inches(1.35), Inches(2), Inches(0.06), GOLD)

    # ── Composite ratings panel ──
    panel_x = Inches(0.6)
    panel_y = Inches(1.7)
    panel_w = Inches(6.3)
    panel_h = Inches(2.4)
    rect(s, panel_x, panel_y, panel_w, panel_h, CARD_BG, CARD_BORDER)
    rect(s, panel_x, panel_y, Inches(0.06), panel_h, GOLD)
    tx(s, panel_x + Inches(0.18), panel_y + Inches(0.10),
       panel_w - Inches(0.3), Inches(0.3),
       "SURPERFORMANCE RATINGS", sz=10, bold=True, rgb=MUTED)

    # Lay out the four composite bars in a 2x2 grid (fits portrait better
    # than a single 4-wide row).
    composites = list(ratings.composites)
    if composites:
        col_w = (panel_w - Inches(0.6)) / 2  # left/right columns with gap
        row_h = Inches(0.7)
        gx = panel_x + Inches(0.2)
        gy = panel_y + Inches(0.45)
        for i, comp in enumerate(composites[:4]):
            col = i % 2
            row = i // 2
            cx = gx + (col_w + Inches(0.2)) * col
            cy = gy + row_h * row + Inches(0.1) * row
            _render_composite_bar(
                s, x=cx, y=cy, w=col_w, h=row_h,
                label=comp.label, score=comp.score, tx=tx, rect=rect,
            )

    # ESG MSCI line at bottom of panel
    esg_y = panel_y + panel_h - Inches(0.4)
    esg_str = ratings.esg_msci or "—"
    tx(s, panel_x + Inches(0.2), esg_y,
       Inches(2.0), Inches(0.3),
       "ESG MSCI:", sz=10, bold=True, rgb=MUTED)
    tx(s, panel_x + Inches(1.4), esg_y,
       Inches(2.0), Inches(0.3),
       esg_str, sz=11, bold=True,
       rgb=GREEN if (ratings.esg_msci or "").startswith("A") else BLACK)

    # ── Strengths / Weaknesses split ──
    list_y = Inches(4.3)
    list_h = Inches(7.5)
    half_w = Inches(3.05)
    gap = Inches(0.2)

    # Strengths box
    sx = Inches(0.6)
    rect(s, sx, list_y, half_w, list_h, STRENGTH_BG, CARD_BORDER)
    rect(s, sx, list_y, Inches(0.06), list_h, STRENGTH_BAR)
    tx(s, sx + Inches(0.2), list_y + Inches(0.15),
       half_w - Inches(0.3), Inches(0.3),
       "STRENGTHS", sz=11, bold=True, rgb=STRENGTH_BAR)

    cursor = list_y + Inches(0.55)
    for bullet in ratings.strengths:
        tx(s, sx + Inches(0.25), cursor, Inches(0.2), Inches(0.3),
           "•", sz=11, bold=True, rgb=STRENGTH_BAR)
        # Estimate height via crude char-per-line (renders robustly even
        # without actual font metrics).
        line_count = max(1, (len(bullet) // 50) + 1)
        h = Inches(0.22) * line_count + Inches(0.08)
        tx(s, sx + Inches(0.45), cursor, half_w - Inches(0.6), h,
           bullet, sz=10, rgb=BLACK, line_spacing=1.15)
        cursor = cursor + h + Inches(0.05)

    # Weaknesses box
    wx = sx + half_w + gap
    rect(s, wx, list_y, half_w, list_h, WEAKNESS_BG, CARD_BORDER)
    rect(s, wx, list_y, Inches(0.06), list_h, WEAKNESS_BAR)
    tx(s, wx + Inches(0.2), list_y + Inches(0.15),
       half_w - Inches(0.3), Inches(0.3),
       "WEAKNESSES", sz=11, bold=True, rgb=WEAKNESS_BAR)

    cursor = list_y + Inches(0.55)
    for bullet in ratings.weaknesses:
        tx(s, wx + Inches(0.25), cursor, Inches(0.2), Inches(0.3),
           "•", sz=11, bold=True, rgb=WEAKNESS_BAR)
        line_count = max(1, (len(bullet) // 50) + 1)
        h = Inches(0.22) * line_count + Inches(0.08)
        tx(s, wx + Inches(0.45), cursor, half_w - Inches(0.6), h,
           bullet, sz=10, rgb=BLACK, line_spacing=1.15)
        cursor = cursor + h + Inches(0.05)

    # ── Source chip (bottom) ──
    tx(s, Inches(0.6), prs.slide_height - Inches(0.4),
       Inches(6.3), Inches(0.3),
       "Source: MarketScreener /ratings/",
       sz=8, rgb=MUTED, al=PP_ALIGN.LEFT)
