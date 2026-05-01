"""
Render slide 3 (Financial Snapshot) of the earnings preview deck.

Consumes only `FinancialSnapshotData`. Produces:
- Title with gold accent rule
- 4-column metric table (Metric | Prior(A) | Current(E) | YoY %)
  - Drops any row whose prior + est are both None (drives the "no fake
    EBITDA" policy: when MS lacks EBITDA, the row simply doesn't render).
- Valuation Summary: 4 cards (P/E FY-est | EV/EBITDA | P/B | Div Yield)
- 1-Year Price chart (skipped on sparse data)
- Footer attribution + bank disclaimer + quality flags
"""

from __future__ import annotations

from datetime import datetime

from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches

from src.models.report_context import (
    FinancialSnapshotData,
    PeriodRow,
)
from src.services.chart_builders import build_price_chart


WHITE = RGBColor(0xFF, 0xFF, 0xFF)
BLACK = RGBColor(0x1F, 0x23, 0x28)
GOLD = RGBColor(0xC9, 0xA2, 0x27)
MUTED = RGBColor(0x8B, 0x94, 0x9E)
GREEN = RGBColor(0x3F, 0xB9, 0x50)
RED = RGBColor(0xCF, 0x22, 0x22)
BORDER = RGBColor(0xDB, 0xE0, 0xE6)
EST_BG = RGBColor(0xFA, 0xF8, 0xF3)


def _delta_color(v: float | None) -> RGBColor:
    if v is None:
        return MUTED
    return GREEN if v >= 0 else RED


def _fmt_num(v) -> str:
    """Same convention as the legacy `pn()`: thousands-separated whole numbers
    for money rows, two-decimal for sub-1 EPS-like values, "—" for None."""
    if v is None:
        return "—"
    try:
        x = float(v)
    except (TypeError, ValueError):
        return str(v)
    if abs(x) >= 1e6:
        return f"{x:,.0f}"
    return f"{x:,.2f}" if x != int(x) else f"{int(x):,}"


def _fmt_signed_pct(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:+.1f}%"


def _fmt_multiple(v: float | None, suffix: str = "x") -> str:
    if v is None:
        return "—"
    return f"{v:.1f}{suffix}"


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:.1f}%"


def _row_label(metric_key: str, units_money: str, units_per_share: str) -> str:
    """Produce the leftmost cell label for a metric row."""
    return {
        "revenue":   f"Revenue {units_money}",
        "ebitda":    f"EBITDA {units_money}",
        "net_income": f"Net Income {units_money}",
        "eps":       f"EPS {units_per_share}",
    }[metric_key]


def _readable_date(s: str | None) -> str:
    if not s or len(s) < 10:
        return "—"
    try:
        y, m, d = int(s[:4]), int(s[5:7]), int(s[8:10])
        months = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
        if 1 <= m <= 12:
            return f"{d} {months[m - 1]} {y}"
    except (ValueError, IndexError):
        pass
    return s


def render(
    prs, blank_layout, snapshot: FinancialSnapshotData,
    *, tx, rect, quality_flags: list[str] | None = None,
) -> None:
    W = prs.slide_width
    H = prs.slide_height

    s3 = prs.slides.add_slide(blank_layout)
    rect(s3, 0, 0, W, H, WHITE)
    tx(s3, Inches(0.6), Inches(0.5), Inches(6), Inches(0.5),
       "Financial Snapshot", sz=26, bold=True, rgb=BLACK)
    rect(s3, Inches(0.6), Inches(1.0), Inches(2), Inches(0.06), GOLD)

    # ── Build the (Metric | Prior | Est | YoY) tuples we need to render. ──
    # Same drop-empty contract as the legacy renderer: rows where both
    # prior and est are None do not render. EBITDA naturally falls out when
    # MS doesn't publish it — never a fake EBIT mirror in its place.
    table = snapshot.table
    if not table.rows or len(table.rows) < 2:
        prior_row = table.rows[0] if table.rows else None
        est_row = None
    else:
        prior_row, est_row = table.rows[0], table.rows[1]

    def _prior(metric: str):
        return getattr(prior_row, metric, None) if prior_row else None

    def _est(metric: str):
        return getattr(est_row, metric, None) if est_row else None

    metric_rows: list[tuple[str, object, object, float | None]] = []
    for metric in ("revenue", "ebitda", "net_income", "eps"):
        p, e = _prior(metric), _est(metric)
        if p is None and e is None:
            continue
        label = _row_label(
            metric, table.units_label, table.units_label_per_share or ""
        )
        yoy = table.yoy_by_metric.get(metric)
        metric_rows.append((label, p, e, yoy))

    # ── Header row ──
    # When specific labels are known (e.g. "2025 Q4" / "2026 Q1" from the
    # /finances/ quarterly fallback path) prefer them. Otherwise fall back to
    # the generic "Q prior (A) / Q next (E)" headers — used by the calendar
    # source where exact labels are not propagated.
    prior_label = (prior_row.label if prior_row else "") or ""
    est_label = (est_row.label if est_row else "") or ""
    if table.mode == "quarterly":
        prior_hdr = f"{prior_label} (A)" if prior_label and prior_label != "Q prior" else "Q prior (A)"
        est_hdr = f"{est_label} (E)" if est_label and est_label != "Q next" else "Q next (E)"
        hdrs = ["Metric", prior_hdr, est_hdr, "YoY %"]
    else:
        hdrs = [
            "Metric",
            f"{prior_label or 'Prior'} (A)",
            f"{est_label or 'Current'} (E)",
            "YoY %",
        ]

    cws = [Inches(2.0), Inches(1.5), Inches(1.5), Inches(1.3)]
    tbx = Inches(0.6)
    tby = Inches(1.3)
    rh = Inches(0.42)
    x = tbx
    for j, h in enumerate(hdrs):
        rect(s3, x, tby, cws[j], rh, BLACK, BORDER)
        tx(s3, x + Inches(0.1), tby + Inches(0.08), cws[j] - Inches(0.2),
           rh, h, sz=10, bold=True, rgb=WHITE)
        x += cws[j]

    # ── Data rows ──
    for i, (lb, pa, ce, yoy) in enumerate(metric_rows):
        y = tby + rh * (i + 1)
        x = tbx
        cells = [lb, _fmt_num(pa), _fmt_num(ce), _fmt_signed_pct(yoy)]
        for j, v in enumerate(cells):
            fill = EST_BG if j == 2 else WHITE
            rect(s3, x, y, cws[j], rh, fill, BORDER)
            # Sign-aware colour on the YoY column only.
            colour = _delta_color(yoy) if (j == 3 and yoy is not None) else BLACK
            tx(s3, x + Inches(0.1), y + Inches(0.08), cws[j] - Inches(0.2),
               rh, str(v), sz=10, bold=(j == 0), rgb=colour)
            x += cws[j]

    # ── Valuation Summary ──
    tx(s3, Inches(0.6), Inches(4.2), Inches(6), Inches(0.4),
       "Valuation Summary", sz=22, bold=True, rgb=BLACK)
    rect(s3, Inches(0.6), Inches(4.6), Inches(2), Inches(0.05), GOLD)

    val = snapshot.valuation
    bank_disclaimer = val.bank_disclaimer_needed
    boxes = [
        ("P/E (FY est)", _fmt_multiple(val.pe_fy_e)),
        ("EV/EBITDA", _fmt_multiple(val.ev_ebitda)
         if val.ev_ebitda is not None else ("N/A*" if bank_disclaimer else "—")),
        ("P/B", _fmt_multiple(val.pb)),
        ("Div. Yield", _fmt_pct(val.div_yield)),
    ]
    vbw = Inches(3.05)
    for i, (lbl, value) in enumerate(boxes):
        x = Inches(0.6) + (Inches(3.2) if i % 2 else 0)
        y = Inches(4.85) + (Inches(1.1) if i >= 2 else 0)
        rect(s3, x, y, vbw, Inches(0.95), WHITE, BORDER)
        rect(s3, x, y, Inches(0.06), Inches(0.95), GOLD)
        tx(s3, x + Inches(0.18), y + Inches(0.12), vbw - Inches(0.3),
           Inches(0.2), lbl, sz=10, rgb=MUTED)
        tx(s3, x + Inches(0.18), y + Inches(0.4), vbw - Inches(0.3),
           Inches(0.35), value, sz=22, bold=True, rgb=GOLD)

    # ── 1-Year Price chart ──
    footer_y = Inches(7.3)
    if snapshot.price_history and len(snapshot.price_history.dates) >= 10:
        try:
            tx(s3, Inches(0.6), Inches(7.15), Inches(4), Inches(0.3),
               "1-Year Price", sz=14, bold=True, rgb=BLACK)
            build_price_chart(
                s3,
                Inches(0.6), Inches(7.55), Inches(6.3), Inches(2.4),
                snapshot.price_history.dates, snapshot.price_history.prices,
            )
            footer_y = Inches(10.15)
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning("Price chart failed: %s", exc)

    # ── Footer attribution ──
    as_of = _readable_date(table.estimates_as_of) if table.estimates_as_of else (
        datetime.now().strftime("%d %b %Y")
    )
    tx(s3, Inches(0.6), footer_y, Inches(6), Inches(0.3),
       f"Actuals: {table.actuals_source}  |  Estimates: {table.estimates_source} as of {as_of}",
       sz=9, rgb=MUTED)
    if bank_disclaimer:
        tx(s3, Inches(0.6), footer_y + Inches(0.2), Inches(6), Inches(0.3),
           "* EBITDA / EV-EBITDA not applicable for banks and financial institutions",
           sz=8, rgb=MUTED)
    if quality_flags:
        tx(s3, Inches(0.6), footer_y + Inches(0.4), Inches(6), Inches(0.3),
           "Data Quality: " + "; ".join(quality_flags[:4]),
           sz=9, rgb=MUTED)
