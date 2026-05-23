"""Matplotlib-based chart images for slide 3.

Three exports, each returns PNG bytes the caller can drop into a pptx
shape via `slide.shapes.add_picture(BytesIO(png), ...)`:

  * `render_52w_price_chart(close_series, currency)` — cleaner date axis,
    larger plot area than the native-pptx LINE chart it replaces.
  * `render_pe_historical_chart(forecast_periods, forecast_pe_values,
        current_pe, history)` — 5-year P/E history with min-max range
    shaded and current multiple as a labeled marker. Falls back to a
    forecast-bar view when history is unavailable.
  * `render_earnings_history_chart(surprise_history, price_series,
        ticker, ccy)` — Koyfin-style dual-panel chart: price line +
    actual/estimate dots with connector lines + per-quarter
    annotation + surprise % bars in a lower panel.

Charts use a dark-on-light institutional palette aligned with
`jabal_design_tokens`. The font defaults to a system sans (Arial-like)
to keep the PNG portable across rendering hosts.
"""
from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import Optional

try:
    import matplotlib
    matplotlib.use("Agg")  # No display server on Render
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import FuncFormatter
    _MPL_AVAILABLE = True
except ImportError:
    _MPL_AVAILABLE = False


# ── Palette (RGB hex strings for matplotlib) ─────────────────
_BLACK = "#0d1117"
_GOLD = "#c9a227"
_GOLD_DK = "#8b6f1a"
_POS = "#1d8a4a"
_NEG = "#c0392b"
_MUTED = "#8b949e"
_CARD = "#f2efe8"
_GRID = "#dcd8cd"
_RING = "#aaaaaa"


def _setup_axes(ax):
    """Common axis styling: gold spines off top/right, gray text, tight grid."""
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(_MUTED)
        ax.spines[side].set_linewidth(0.8)
    ax.tick_params(colors=_MUTED, labelsize=8)
    ax.yaxis.label.set_color(_MUTED)
    ax.xaxis.label.set_color(_MUTED)
    ax.grid(True, which="major", axis="y", color=_GRID, linewidth=0.4, alpha=0.7)
    ax.set_axisbelow(True)


def _fig_to_png(fig, dpi: int = 180) -> bytes:
    """Render to PNG bytes; close the figure to free memory."""
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight",
                facecolor="white", edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ── 52-week price ─────────────────────────────────────────────

def render_52w_price_chart(close_series: list[dict], currency: str = "") -> Optional[bytes]:
    """Single-pane price line with a cleaned-up date axis.

    `close_series` items: {"date": "YYYY-MM-DD", "close": float}. Sparse
    series are accepted as-is — the date locator handles uneven spacing.

    Returns None when matplotlib is unavailable or the series is empty,
    so the caller can fall back to a text placeholder.
    """
    if not _MPL_AVAILABLE or not close_series:
        return None
    try:
        dates = [datetime.strptime(pt["date"], "%Y-%m-%d") for pt in close_series
                 if pt.get("date") and pt.get("close") is not None]
        vals = [float(pt["close"]) for pt in close_series
                if pt.get("date") and pt.get("close") is not None]
    except (ValueError, TypeError, KeyError):
        return None
    if len(dates) < 2:
        return None

    fig, ax = plt.subplots(figsize=(5.2, 2.2))
    ax.plot(dates, vals, color=_GOLD, linewidth=1.6)
    ax.fill_between(dates, vals, min(vals) * 0.98, color=_GOLD, alpha=0.08)
    _setup_axes(ax)
    # Six monthly major ticks — readable even on a tight slide column.
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
    # Y axis: currency-aware tick formatter, no comma for low-priced names.
    def _money(v, _pos):
        if abs(v) >= 1000: return f"{v:,.0f}"
        if abs(v) >= 10:   return f"{v:.1f}"
        return f"{v:.2f}"
    ax.yaxis.set_major_formatter(FuncFormatter(_money))
    # Last-close annotation pinned to the right edge.
    last_dt = dates[-1]
    last_val = vals[-1]
    ax.scatter([last_dt], [last_val], s=22, color=_GOLD_DK,
                zorder=3, edgecolors="white", linewidths=1.0)
    ax.annotate(f"{currency} {_money(last_val, None)}".strip(),
                xy=(last_dt, last_val),
                xytext=(6, 0), textcoords="offset points",
                fontsize=8, color=_BLACK, va="center", ha="left")
    fig.tight_layout(pad=0.4)
    return _fig_to_png(fig)


# ── P/E historical range ──────────────────────────────────────

def render_pe_historical_chart(history: list[dict] | None,
                                 forecast_periods: list[str] | None,
                                 forecast_values: list[Optional[float]] | None,
                                 current_pe: Optional[float]) -> Optional[bytes]:
    """Historical forward-P/E chart with min-max range shading + current marker.

    `history` items: {"date": "YYYY-MM-DD", "pe": float}. When the
    history is unavailable (yfinance doesn't expose historical forward
    P/E for most non-US names), fall back to a bar view of the explicit
    forecast periods.
    """
    if not _MPL_AVAILABLE:
        return None

    fig, ax = plt.subplots(figsize=(5.2, 2.2))
    _setup_axes(ax)

    # Path A: historical series available — line + range shading.
    rows = []
    for r in (history or []):
        try:
            d = datetime.strptime(r["date"], "%Y-%m-%d")
            v = float(r["pe"])
            rows.append((d, v))
        except (TypeError, ValueError, KeyError):
            continue
    if len(rows) >= 6:
        rows.sort()
        dates = [d for d, _ in rows]
        vals = [v for _, v in rows]
        lo, hi = min(vals), max(vals)
        ax.fill_between(dates, [lo] * len(dates), [hi] * len(dates),
                          color=_GOLD, alpha=0.08, label=f"Range {lo:.1f}x–{hi:.1f}x")
        ax.plot(dates, vals, color=_GOLD_DK, linewidth=1.4)
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0f}x"))
        if isinstance(current_pe, (int, float)):
            ax.axhline(current_pe, color=_BLACK, linewidth=1.0, linestyle="--", alpha=0.7)
            # Marker + label pinned to the right edge for a clean read.
            ax.scatter([dates[-1]], [current_pe], s=46, marker="D",
                        color=_BLACK, zorder=5, edgecolors="white", linewidths=1.0)
            ax.annotate(f"Current  {current_pe:.1f}x",
                        xy=(dates[-1], current_pe),
                        xytext=(-6, 8), textcoords="offset points",
                        fontsize=8, color=_BLACK, ha="right", weight="bold")
        ax.legend(loc="upper left", fontsize=7, frameon=False)
        fig.tight_layout(pad=0.4)
        return _fig_to_png(fig)

    # Path B: forecast bar view (one bar per FY period).
    periods = forecast_periods or []
    values = [v if isinstance(v, (int, float)) else None for v in (forecast_values or [])]
    pairs = [(p, v) for p, v in zip(periods, values) if v is not None]
    if not pairs:
        # Path C: nothing but a current P/E — render a single horizontal
        # tick + label so the slide doesn't show a bare "no data" stub.
        # Better an honest single-point view than an empty box.
        if isinstance(current_pe, (int, float)) and current_pe > 0:
            ax.axvline(current_pe, color=_BLACK, linewidth=1.4)
            pad = max(2, current_pe * 0.25)
            ax.set_xlim(max(0, current_pe - pad), current_pe + pad)
            ax.set_ylim(-1, 1)
            ax.set_yticks([])
            ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0f}x"))
            ax.text(current_pe, 0.4, f"Current  {current_pe:.1f}x",
                      fontsize=10, color=_BLACK, weight="bold", ha="center")
            ax.text(current_pe, -0.6,
                      "Forward / historical P/E unavailable for this ticker",
                      fontsize=7, color=_MUTED, ha="center", style="italic")
            fig.tight_layout(pad=0.4)
            return _fig_to_png(fig)
        plt.close(fig)
        return None
    plabels = [p for p, _ in pairs]
    pvals = [v for _, v in pairs]
    ymax = max(pvals) * 1.18
    ax.barh(range(len(plabels)), pvals, color=_CARD, edgecolor=_MUTED, height=0.55)
    ax.set_yticks(range(len(plabels)))
    ax.set_yticklabels(plabels)
    ax.invert_yaxis()
    ax.set_xlim(0, ymax)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.0f}x"))
    for i, v in enumerate(pvals):
        ax.text(v + ymax * 0.01, i, f"{v:.1f}x", va="center",
                  fontsize=8, color=_BLACK)
    if isinstance(current_pe, (int, float)) and current_pe > 0:
        ax.axvline(current_pe, color=_BLACK, linewidth=1.2, linestyle="--", alpha=0.8)
        ax.text(current_pe, -0.6, f"Current  {current_pe:.1f}x",
                  fontsize=8, color=_BLACK, weight="bold", ha="center")
    fig.tight_layout(pad=0.4)
    return _fig_to_png(fig)


# ── Earnings history (replaces the Market Sentiment box) ──────

def render_earnings_history_chart(
        surprise_history: list[dict],
        price_series: list[dict] | None,
        ticker: str = "",
        currency: str = "",
        max_quarters: int = 8,
        metric_label: str = "EPS",
) -> Optional[bytes]:
    """Dual-panel chart inspired by the Koyfin reference.

    Upper panel: stock price line (when available) on a secondary axis
    + per-quarter actual EPS (filled gold dot, green/red sentiment fill)
    and estimate EPS (open gray ring) on the primary axis, with a thin
    connector line between estimate and actual for each quarter and a
    small annotation showing the actual value.

    Lower panel: per-quarter surprise % bars (green = beat, red = miss).

    `surprise_history` rows: {"period": "Q2 2025", "eps_actual": ...,
    "eps_estimate": ..., "eps_surprise_pct": ...}. We use the most
    recent `max_quarters` rows.

    `price_series` is the same shape as the 52w chart but ideally
    spans ~2 years. When absent, the upper panel renders without the
    secondary price line.
    """
    if not _MPL_AVAILABLE or not surprise_history:
        return None

    # Take the most recent `max_quarters` rows; the source already lists
    # them most-recent-first in the canonical shape.
    #
    # Filter logic — accept any row that has a COMPLETE pair on either
    # metric. EPS is preferred; revenue is the fallback. For thinly-
    # covered tickers (BKMB.OM) Investing publishes the rows with
    # revenue actuals/estimates but no EPS — those still produce a
    # readable surprise chart on the revenue axis.
    def _has_eps_pair(r):
        return (isinstance(r.get("eps_actual"), (int, float))
                and isinstance(r.get("eps_estimate"), (int, float)))
    def _has_rev_pair(r):
        return (isinstance(r.get("revenue_actual"), (int, float))
                and isinstance(r.get("revenue_estimate"), (int, float)))

    rows = [r for r in surprise_history if _has_eps_pair(r) or _has_rev_pair(r)]
    rows = rows[:max_quarters]
    if not rows:
        return None

    # If no row in the kept set has a complete EPS pair, switch the
    # chart to revenue-axis mode. Otherwise stay on EPS for consistency.
    using_revenue = (not any(_has_eps_pair(r) for r in rows)) or metric_label.lower() in ("revenue", "net sales", "sales")
    if using_revenue and metric_label == "EPS":
        metric_label = "Revenue"   # auto-relabel — caller didn't know

    # Promote revenue values into the eps_* slot when EPS pair is
    # missing, so the rest of the chart code can stay metric-agnostic.
    if using_revenue:
        for r in rows:
            if not _has_eps_pair(r) and _has_rev_pair(r):
                r["eps_actual"] = r["revenue_actual"]
                r["eps_estimate"] = r["revenue_estimate"]
                # Derive surprise % if absent
                if not isinstance(r.get("eps_surprise_pct"), (int, float)):
                    est = r["revenue_estimate"]
                    if isinstance(est, (int, float)) and est not in (0, 0.0):
                        r["eps_surprise_pct"] = (r["revenue_actual"] - est) / est * 100.0

    rows = list(reversed(rows))  # Oldest first for left-to-right plotting.

    # Parse "Q2 2025" -> a real datetime so the x-axis lines up with the
    # secondary price line.
    def _q_to_dt(label: str) -> Optional[datetime]:
        import re
        m = re.match(r"\s*Q([1-4])\s+(\d{4})", label or "")
        if not m:
            return None
        q = int(m.group(1)); y = int(m.group(2))
        return datetime(y, 3 * q, 1)
    qdates = [_q_to_dt(r.get("period") or "") for r in rows]
    if any(d is None for d in qdates):
        # Fall back to evenly spaced positions if any label is unparseable.
        from datetime import timedelta
        last = datetime.today()
        qdates = [last - timedelta(days=90 * (len(rows) - i - 1)) for i in range(len(rows))]

    actuals = [float(r["eps_actual"]) for r in rows]
    estimates = [float(r["eps_estimate"]) for r in rows]
    surprises = [
        float(r["eps_surprise_pct"]) if isinstance(r.get("eps_surprise_pct"), (int, float)) else 0.0
        for r in rows
    ]

    # Two panels stacked, sharing the x-axis. The lower bar panel is
    # one-third the height of the upper to keep the focus on the
    # actual/estimate scatter.
    fig, (ax_top, ax_bot) = plt.subplots(
        nrows=2, ncols=1, sharex=True,
        figsize=(7.6, 3.2),
        gridspec_kw={"height_ratios": [3, 1], "hspace": 0.08},
    )
    _setup_axes(ax_top)
    _setup_axes(ax_bot)

    # Optional secondary axis for the price line.
    if price_series:
        try:
            pd = [(datetime.strptime(pt["date"], "%Y-%m-%d"), float(pt["close"]))
                  for pt in price_series
                  if pt.get("date") and pt.get("close") is not None]
            # Trim to the period covered by the earnings rows.
            if pd and qdates:
                pd = [(d, v) for d, v in pd if d >= qdates[0]]
            if len(pd) >= 2:
                ax_price = ax_top.twinx()
                ax_price.plot([d for d, _ in pd], [v for _, v in pd],
                                color=_MUTED, linewidth=1.0, alpha=0.85,
                                label="Price")
                ax_price.tick_params(colors=_MUTED, labelsize=8)
                ax_price.spines["top"].set_visible(False)
                ax_price.spines["right"].set_color(_MUTED)
                ax_price.spines["right"].set_linewidth(0.6)
                ax_price.set_ylabel(f"Price ({currency})" if currency else "Price",
                                    fontsize=8, color=_MUTED)
        except (TypeError, ValueError, KeyError):
            pass

    # Estimate (open gray ring) + actual (filled dot) + connector line.
    is_eps_metric = metric_label.upper() == "EPS"
    for d, est, act, surp in zip(qdates, estimates, actuals, surprises):
        fill = _POS if surp >= 0 else _NEG
        ax_top.plot([d, d], [est, act], color=_MUTED, linewidth=0.9, alpha=0.7, zorder=1)
        ax_top.scatter([d], [est], s=70, facecolors="white", edgecolors=_RING,
                          linewidths=1.4, zorder=3)
        ax_top.scatter([d], [act], s=80, color=fill, edgecolors="white",
                          linewidths=1.0, zorder=4)
        # Actual value annotation just above (beat) or below (miss) the dot.
        y_off = 12 if act >= est else -16
        annot = f"{act:.2f}" if is_eps_metric else f"{act:,.1f}"
        ax_top.annotate(annot,
                          xy=(d, act),
                          xytext=(0, y_off), textcoords="offset points",
                          fontsize=7, color=_BLACK, ha="center",
                          weight="bold")

    # Metric varies: EPS, Net Income (M currency), or Net Sales (M currency).
    # Pick a y-axis formatter that fits the range — EPS is sub-1 typically,
    # the others are millions and benefit from grouped formatting.
    is_eps_axis = metric_label.upper() == "EPS"
    ax_top.set_ylabel(metric_label, fontsize=8, color=_MUTED)
    if is_eps_axis:
        ax_top.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:.2f}"))
    else:
        ax_top.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:,.0f}"))

    # Surprise bars: positive green, negative red.
    colors = [_POS if s >= 0 else _NEG for s in surprises]
    ax_bot.bar(qdates, surprises, width=60, color=colors, alpha=0.85,
                edgecolor="white", linewidth=0.4)
    ax_bot.axhline(0, color=_MUTED, linewidth=0.6)
    ax_bot.set_ylabel("Surprise %", fontsize=8, color=_MUTED)
    ax_bot.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:+.0f}%"))
    ax_bot.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax_bot.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))

    # Title strip — kept short so it doesn't compete with the slide section label.
    title_bits = []
    if ticker:
        title_bits.append(ticker)
    title_bits.append(f"{metric_label} Actual vs Estimate")
    ax_top.set_title("  ·  ".join(title_bits), fontsize=10, color=_BLACK,
                       loc="left", pad=4, weight="bold")

    # Legend handles for the scatter pair.
    from matplotlib.lines import Line2D
    legend_items = [
        Line2D([0], [0], marker="o", color="w", markerfacecolor=_POS,
                 markeredgecolor="white", markersize=8, label="Actual (beat)"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=_NEG,
                 markeredgecolor="white", markersize=8, label="Actual (miss)"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor="white",
                 markeredgecolor=_RING, markersize=8, label="Estimate"),
    ]
    ax_top.legend(handles=legend_items, loc="upper left", fontsize=7,
                    frameon=False, ncol=3, handletextpad=0.4,
                    columnspacing=1.2)
    fig.tight_layout(pad=0.4)
    return _fig_to_png(fig)
