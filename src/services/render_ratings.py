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
       "Ratings & Sentiment", sz=22, bold=True, rgb=BLACK)
    rect(s, Inches(0.6), Inches(1.32), Inches(0.9), Inches(0.04), GOLD)

    # ── Composite ratings panel — single horizontal row ──
    # Was a 2x2 grid taking 2.4" of vertical real estate. Single row of
    # 4 columns plus a right-aligned ESG cell halves that and matches
    # the institutional convention of putting the rating ladder in one
    # eye-level scan.
    panel_x = Inches(0.6)
    panel_y = Inches(1.65)
    panel_w = Inches(6.3)
    panel_h = Inches(1.45)
    rect(s, panel_x, panel_y, panel_w, panel_h, CARD_BG, CARD_BORDER)
    rect(s, panel_x, panel_y, Inches(0.04), panel_h, GOLD)
    tx(s, panel_x + Inches(0.16), panel_y + Inches(0.10),
       panel_w - Inches(0.3), Inches(0.22),
       "SURPERFORMANCE RATINGS", sz=8, bold=True, rgb=MUTED)
    # ESG MSCI on the same eyebrow line, right-aligned.
    esg_str = ratings.esg_msci or "—"
    esg_color = GREEN if (ratings.esg_msci or "").startswith("A") else BLACK
    tx(s, panel_x + panel_w - Inches(2.0), panel_y + Inches(0.10),
       Inches(1.4), Inches(0.22),
       "ESG MSCI", sz=8, bold=True, rgb=MUTED, al=PP_ALIGN.RIGHT)
    tx(s, panel_x + panel_w - Inches(0.6), panel_y + Inches(0.10),
       Inches(0.4), Inches(0.22),
       esg_str, sz=10, bold=True, rgb=esg_color, al=PP_ALIGN.RIGHT)

    # 4 composite bars across the panel, each 1.40" wide with 0.10" gap
    # totalling 1.50" × 4 = 6.00" plus margins.
    composites = list(ratings.composites)
    if composites:
        bar_y = panel_y + Inches(0.42)
        bar_h = Inches(0.85)
        gx = panel_x + Inches(0.16)
        col_w = (panel_w - Inches(0.32) - Inches(0.30)) / 4  # 4 cols, 3 gaps × 0.10"
        gap = Inches(0.10)
        for i, comp in enumerate(composites[:4]):
            cx = gx + (col_w + gap) * i
            _render_composite_bar(
                s, x=cx, y=bar_y, w=col_w, h=bar_h,
                label=comp.label, score=comp.score, tx=tx, rect=rect,
            )

    # ── Strengths / Weaknesses split ──
    # Auto-sized to actual content. Previous fixed 8.5-inch panels left
    # ~5 inches of empty cream-coloured background below short bullet
    # lists (NBOB.OM has just 5 strengths and 3 weaknesses). Now each
    # bullet-list height is computed from its content; the renderer
    # caps the visible panel just below the last bullet so the panels
    # don't sprawl past their data.
    list_y = Inches(3.30)
    half_w = Inches(3.05)
    gap = Inches(0.2)
    sx = Inches(0.6)
    wx = sx + half_w + gap

    def _bullet_height(text: str) -> float:
        """Approximate height in inches for a wrapped bullet at sz=10
        body width 2.45". Empirical: ~50 chars/line, 0.22"/line."""
        n = max(1, (len(text) // 50) + 1)
        return 0.22 * n + 0.08

    s_content = sum(_bullet_height(b) for b in ratings.strengths)
    w_content = sum(_bullet_height(b) for b in ratings.weaknesses)
    panel_top_pad = 0.55
    panel_bot_pad = 0.20
    s_h_in = max(panel_top_pad + s_content + panel_bot_pad, 2.5)
    w_h_in = max(panel_top_pad + w_content + panel_bot_pad, 2.5)
    panel_h_in = max(s_h_in, w_h_in)
    list_h = Inches(panel_h_in)

    def _draw_bullet_panel(x, label, label_color, bg_color, bar_color,
                           bullets):
        rect(s, x, list_y, half_w, list_h, bg_color, CARD_BORDER)
        rect(s, x, list_y, Inches(0.06), list_h, bar_color)
        tx(s, x + Inches(0.20), list_y + Inches(0.16),
           half_w - Inches(0.30), Inches(0.28),
           label, sz=11, bold=True, rgb=label_color)
        cursor = list_y + Inches(0.55)
        for bullet in bullets:
            tx(s, x + Inches(0.25), cursor,
               Inches(0.18), Inches(0.30),
               "•", sz=11, bold=True, rgb=bar_color)
            h = Inches(_bullet_height(bullet))
            tx(s, x + Inches(0.45), cursor,
               half_w - Inches(0.60), h,
               bullet, sz=10, rgb=BLACK, line_spacing=1.15)
            cursor = cursor + h + Inches(0.05)

    _draw_bullet_panel(sx, "STRENGTHS", STRENGTH_BAR, STRENGTH_BG, STRENGTH_BAR,
                       ratings.strengths)
    _draw_bullet_panel(wx, "WEAKNESSES", WEAKNESS_BAR, WEAKNESS_BG, WEAKNESS_BAR,
                       ratings.weaknesses)

    # ── Peer ESG mini-table — fills the bottom band ──
    # Surfaces the cross-peer ESG comparison from /ratings/ as a compact
    # 6-row mini table. Different from slide 6 which shows full sector
    # peers with multi-period performance — this one focuses on rating
    # composition (Investor rating % + ESG letter) for the same group.
    peer_esg = list(ratings.peer_esg) if hasattr(ratings, "peer_esg") else []
    panel_bottom = list_y + list_h
    table_y = panel_bottom + Inches(0.30)
    if peer_esg and table_y < prs.slide_height - Inches(2.0):
        avail_h_in = (prs.slide_height - table_y) / Inches(1) - 0.8
        max_rows = max(1, int(avail_h_in / 0.30))
        rows_to_show = peer_esg[:min(max_rows, 10)]

        tx(s, Inches(0.6), table_y, Inches(6.3), Inches(0.25),
           "PEER COMPARISON — INVESTOR RATING & ESG MSCI",
           sz=8, bold=True, rgb=MUTED)

        head_y = table_y + Inches(0.32)
        row_h = Inches(0.30)

        # Column layout: name (3.6"), Investor rating (1.4"), ESG (1.0")
        col_x = {
            "name":     Inches(0.6),
            "rating":   Inches(0.6) + Inches(3.6),
            "esg":      Inches(0.6) + Inches(5.0),
        }
        col_w = {
            "name":     Inches(3.6),
            "rating":   Inches(1.4),
            "esg":      Inches(1.3),
        }
        # Header
        rect(s, Inches(0.6), head_y, Inches(6.3), Inches(0.28),
             RGBColor(0xF1, 0xF3, 0xF5))
        tx(s, col_x["name"] + Inches(0.10), head_y + Inches(0.06),
           col_w["name"], Inches(0.20),
           "Company", sz=8, bold=True, rgb=MUTED)
        tx(s, col_x["rating"], head_y + Inches(0.06),
           col_w["rating"] - Inches(0.10), Inches(0.20),
           "Investor Rating", sz=8, bold=True, rgb=MUTED, al=PP_ALIGN.RIGHT)
        tx(s, col_x["esg"], head_y + Inches(0.06),
           col_w["esg"], Inches(0.20),
           "ESG MSCI", sz=8, bold=True, rgb=MUTED, al=PP_ALIGN.CENTER)

        cy = head_y + Inches(0.30)
        for i, peer in enumerate(rows_to_show):
            bg = WHITE if i % 2 == 0 else RGBColor(0xFA, 0xFB, 0xFC)
            rect(s, Inches(0.6), cy, Inches(6.3), row_h, bg)
            name = peer.get("name") or "—"
            if len(name) > 36:
                name = name[:34].rstrip() + "…"
            rating_pct = peer.get("rating_pct")
            esg = peer.get("esg_msci") or "—"
            tx(s, col_x["name"] + Inches(0.10), cy + Inches(0.06),
               col_w["name"], Inches(0.22),
               name, sz=9, rgb=BLACK)
            rating_txt = f"{rating_pct}%" if rating_pct is not None else "—"
            tx(s, col_x["rating"], cy + Inches(0.06),
               col_w["rating"] - Inches(0.10), Inches(0.22),
               rating_txt, sz=9, bold=True,
               rgb=GREEN if (rating_pct or 0) >= 70 else
                   (BLACK if (rating_pct or 0) >= 40 else MUTED),
               al=PP_ALIGN.RIGHT)
            tx(s, col_x["esg"], cy + Inches(0.06),
               col_w["esg"], Inches(0.22),
               esg, sz=9, bold=True,
               rgb=GREEN if (esg or "").startswith("A") else BLACK,
               al=PP_ALIGN.CENTER)
            cy = cy + row_h

    # ── Source chip (bottom) ──
    tx(s, Inches(0.6), prs.slide_height - Inches(0.4),
       Inches(6.3), Inches(0.3),
       "Source: MarketScreener /ratings/",
       sz=8, rgb=MUTED, al=PP_ALIGN.LEFT)
