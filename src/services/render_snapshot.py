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


def _humanize_period(label: str | None) -> str:
    """Render a period label for slide-3 table headers.

    The MS scraper normalises "2025 Q4" → "2025Q4" for storage. Slide 3
    headers read better with the space restored: "2025 Q4 (A)" rather than
    "2025Q4 (A)". Also handles "Q4 2025" (Bloomberg) and "FY2026" formats.
    """
    if not label:
        return "—"
    s = str(label).strip()
    # "2025Q4" → "2025 Q4"
    import re
    m = re.match(r"^(20\d{2})Q([1-4])$", s)
    if m:
        return f"{m.group(1)} Q{m.group(2)}"
    # "Q4 2025" (BBG) — already nicely spaced
    m = re.match(r"^Q([1-4])\s+(20\d{2})$", s)
    if m:
        return s
    # "FY2026" / "FY2026E" — leave as is, just uppercase E to be tidy
    return s.upper().replace("FY ", "FY") if s.upper().startswith("FY") else s


def _render_multi_period(s3, table, grid, *, tbx, tby, rh, tx, rect) -> None:
    """Multi-period annual grid: 1 metric label column + N year columns.

    Mirrors the gold-standard deck. Each year column shaded `EST_BG` for
    estimates (no announcement_date) vs WHITE for actuals. Drops any metric
    whose values are all-None across the grid (so EBITDA disappears for
    banks, not faked from EBIT).
    """
    n_periods = len(grid.periods)
    if n_periods == 0:
        return
    # Layout: portrait slide is 7.5" wide; available width ~6.3".
    metric_w_in = 1.55
    metric_w = Inches(metric_w_in)
    avail_in = 6.3 - metric_w_in
    col_w = Inches(round(avail_in / max(1, n_periods), 2))
    # Header row
    rect(s3, tbx, tby, metric_w, rh, BLACK, BORDER)
    tx(s3, tbx + Inches(0.08), tby + Inches(0.08),
       metric_w - Inches(0.16), rh, "Metric",
       sz=10, bold=True, rgb=WHITE)
    cx = tbx + metric_w
    for i, period in enumerate(grid.periods):
        is_est = (
            i >= len(grid.announcement_dates)
            or not grid.announcement_dates[i]
        )
        suffix = "(E)" if is_est else "(A)"
        label = _humanize_period(period)
        rect(s3, cx, tby, col_w, rh, BLACK, BORDER)
        tx(s3, cx + Inches(0.04), tby + Inches(0.08),
           col_w - Inches(0.08), rh, f"{label} {suffix}",
           sz=8, bold=True, rgb=WHITE, al=PP_ALIGN.CENTER)
        cx += col_w

    # Data rows — each metric row drops out when all values are None.
    rendered = 0
    for metric_key, label_template in (
        ("revenue",    "Revenue {money}"),
        ("ebitda",     "EBITDA {money}"),
        ("ebit",       "EBIT {money}"),
        ("net_income", "Net Income {money}"),
        ("eps",        "EPS {per_share}"),
    ):
        values = list(getattr(grid, metric_key, []) or [])
        if not values or all(v is None for v in values):
            continue
        row_label = label_template.format(
            money=table.units_label,
            per_share=table.units_label_per_share or "",
        ).strip()
        y = tby + rh * (rendered + 1)
        rect(s3, tbx, y, metric_w, rh, WHITE, BORDER)
        tx(s3, tbx + Inches(0.08), y + Inches(0.08),
           metric_w - Inches(0.16), rh, row_label,
           sz=9, bold=True, rgb=BLACK)
        cx = tbx + metric_w
        for i in range(n_periods):
            v = values[i] if i < len(values) else None
            is_est = (
                i >= len(grid.announcement_dates)
                or not grid.announcement_dates[i]
            )
            fill = EST_BG if is_est else WHITE
            rect(s3, cx, y, col_w, rh, fill, BORDER)
            if v is None:
                display = "—"
            elif metric_key == "eps":
                # Preserve precision for sub-1 EPS (Oman / India). Two
                # decimals collapses 0.028 / 0.031 / 0.033 to identical
                # "0.03" — useless. Use 3 decimals when |v| < 0.5,
                # 2 decimals when 0.5 ≤ |v| < 100, 1 decimal for ≥ 100
                # (rare double-digit-EPS names like Industries Qatar).
                try:
                    fv = float(v)
                    if abs(fv) < 0.5:
                        display = f"{fv:.3f}"
                    elif abs(fv) < 100:
                        display = f"{fv:.2f}"
                    else:
                        display = f"{fv:.1f}"
                except (TypeError, ValueError):
                    display = str(v)
            else:
                try:
                    display = f"{float(v):,.0f}"
                except (TypeError, ValueError):
                    display = str(v)
            tx(s3, cx + Inches(0.04), y + Inches(0.08),
               col_w - Inches(0.08), rh, display,
               sz=8, rgb=BLACK, al=PP_ALIGN.CENTER)
            cx += col_w
        rendered += 1


def _render_prior_est_pair(s3, table, *, tbx, tby, rh, tx, rect) -> None:
    """Legacy 4-column (Metric | Prior(A) | Est(E) | Δ%) renderer.

    Used only as a fallback when no annual_grid is available — typically a
    BBG-quarterly-only payload with no FY series. The user's primary view
    is `_render_multi_period`; this preserves a reasonable display when
    only thin data is present.
    """
    if not table.rows or len(table.rows) < 2:
        prior_row = table.rows[0] if table.rows else None
        est_row = None
    else:
        prior_row, est_row = table.rows[0], table.rows[1]

    metric_rows: list[tuple[str, object, object, float | None]] = []
    for metric in ("revenue", "ebitda", "net_income", "eps"):
        p = getattr(prior_row, metric, None) if prior_row else None
        e = getattr(est_row, metric, None) if est_row else None
        if p is None and e is None:
            continue
        label = _row_label(
            metric, table.units_label, table.units_label_per_share or ""
        )
        metric_rows.append((label, p, e, table.yoy_by_metric.get(metric)))

    prior_label = _humanize_period(prior_row.label if prior_row else "")
    est_label = _humanize_period(est_row.label if est_row else "")
    delta_label = "YoY %"
    if table.mode == "quarterly" and not any(
        v is not None for v in table.yoy_by_metric.values()
    ):
        delta_label = "QoQ %"

    if table.mode == "quarterly":
        prior_hdr = (
            f"{prior_label} (A)" if prior_label and prior_label != "—"
            and "Q prior" not in prior_label else "Q prior (A)"
        )
        est_hdr = (
            f"{est_label} (E)" if est_label and est_label != "—"
            and "Q next" not in est_label else "Q next (E)"
        )
        hdrs = ["Metric", prior_hdr, est_hdr, delta_label]
    else:
        hdrs = [
            "Metric",
            f"{prior_label if prior_label != '—' else 'Prior'} (A)",
            f"{est_label if est_label != '—' else 'Current'} (E)",
            delta_label,
        ]

    cws = [Inches(2.0), Inches(1.5), Inches(1.5), Inches(1.3)]
    x = tbx
    for j, h in enumerate(hdrs):
        rect(s3, x, tby, cws[j], rh, BLACK, BORDER)
        tx(s3, x + Inches(0.1), tby + Inches(0.08),
           cws[j] - Inches(0.2), rh, h, sz=10, bold=True, rgb=WHITE)
        x += cws[j]

    for i, (lb, pa, ce, yoy) in enumerate(metric_rows):
        y = tby + rh * (i + 1)
        x = tbx
        cells = [lb, _fmt_num(pa), _fmt_num(ce), _fmt_signed_pct(yoy)]
        for j, v in enumerate(cells):
            fill = EST_BG if j == 2 else WHITE
            rect(s3, x, y, cws[j], rh, fill, BORDER)
            colour = _delta_color(yoy) if (j == 3 and yoy is not None) else BLACK
            tx(s3, x + Inches(0.1), y + Inches(0.08),
               cws[j] - Inches(0.2), rh, str(v),
               sz=10, bold=(j == 0), rgb=colour)
            x += cws[j]


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

    table = snapshot.table
    tbx = Inches(0.6)
    tby = Inches(1.3)
    rh = Inches(0.42)

    # Prefer the multi-period annual grid (matches the gold-standard deck:
    # 5–6 years of full income statement). Fall back to the (prior, est) pair
    # only when annual data is unavailable (rare — quarterly-only bundles).
    if table.annual_grid and table.annual_grid.periods:
        _render_multi_period(
            s3, table, table.annual_grid, tbx=tbx, tby=tby, rh=rh,
            tx=tx, rect=rect,
        )
    else:
        _render_prior_est_pair(
            s3, table, tbx=tbx, tby=tby, rh=rh, tx=tx, rect=rect,
        )

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
