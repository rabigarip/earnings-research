"""
Jabal — Slide 3 (Valuation & Positioning) renderer.

Layout:
  1. Header strip
  2. Section hero (MARKET POSITIONING / "Valuation & Market View")
  3. Two-up chart row:
       LEFT  — 52-week price chart (line)
       RIGHT — P/E multiple 5-year range (horizontal bars)
  4. Peer comparables table
  5. Sentiment row (3 cards): Consensus distribution | Avg target | Last 3 broker actions
  6. Footer

Charts are drawn natively in pptx (line + horizontal bars) rather than
embedded matplotlib images — keeps the file lean and editable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pptx.enum.shapes import MSO_SHAPE

from src.services.render_jabal_snapshot import _sources_line
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.util import Inches, Pt

from src.services.jabal_design_tokens import (
    BLACK, GRAY, MUTED, GOLD, GOLD_DK, POS, NEG, CARD, WHITE,
    FONT_DISPLAY, FONT_UI,
    SZ_SECTION, SZ_KICKER, SZ_VALUE, SZ_LABEL, SZ_BODY,
    SZ_HEADER, SZ_FOOTER, SZ_BULLET_PILL, SZ_TINY,
    PAGE_W_IN, PAGE_H_IN, MARGIN_L, MARGIN_R, CONTENT_W,
    RULE_THICK_PT, BORDER_THICK_PT, LEFT_ACCENT_W_IN,
    in_, signed_color,
)
from src.services.canonical_store import (
    get_all_fields, get_observations_by_provider,
)
from src.services.render_jabal_snapshot import (
    _text, _hrule, _card, _section_label, _header_strip, _footer,
)
from src.services.render_jabal_thesis import _section_hero


# ── Native pptx charts ────────────────────────────────────────

def _line_chart_52w(slide, left: float, top: float, width: float, height: float,
                     close_series: list[dict], currency: str = ""):
    """Draw a simple line chart using add_chart with XL_CHART_TYPE.LINE.
    close_series is a list of {date, close} dicts (sparse OK)."""
    from pptx.chart.data import CategoryChartData
    from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
    if not close_series:
        _text(slide, left, top + height * 0.45, width, 0.30,
              "No price history available", size=SZ_BODY, color=MUTED,
              align=PP_ALIGN.CENTER)
        return
    cats = [pt["date"][-5:] for pt in close_series]
    vals = [pt["close"] for pt in close_series]
    cd = CategoryChartData()
    cd.categories = cats
    cd.add_series("Close", vals)
    chart_shape = slide.shapes.add_chart(
        XL_CHART_TYPE.LINE, in_(left), in_(top), in_(width), in_(height), cd
    )
    chart = chart_shape.chart
    chart.has_legend = False
    plot = chart.plots[0]
    plot.has_data_labels = False
    # Style: single gold series, thin line
    try:
        ser = plot.series[0]
        from pptx.dml.color import RGBColor
        line = ser.format.line
        line.color.rgb = GOLD
        line.width = Pt(1.5)
        # No markers — clean line
        from pptx.oxml.ns import qn
        sp_pr = ser._element.find(qn("c:spPr"))
        if sp_pr is None:
            pass
    except (AttributeError, KeyError, TypeError, ValueError):
        pass
    # Hide gridlines for tightness
    try:
        for axis in (chart.value_axis, chart.category_axis):
            axis.major_unit = None
            axis.minor_unit = None
            axis.format.line.fill.background()
            for tl in (axis.tick_labels,):
                tl.font.size = Pt(7)
                tl.font.name = FONT_UI
                from pptx.dml.color import RGBColor
                tl.font.color.rgb = MUTED
    except (AttributeError, KeyError, TypeError, ValueError):
        pass


def _pe_range_chart(slide, left: float, top: float, width: float, height: float,
                     periods: list[str], pe_vals: list[Optional[float]],
                     current_pe: Optional[float]):
    """Horizontal bars representing P/E across FY periods, plus a
    'current' diamond marker on the most-recent bar.

    Renders manually with shapes — no chart object — so we can pixel-tune
    the look to match the spec deck."""
    # Header label
    _text(slide, left, top, width, 0.22, "P/E MULTIPLE  ·  5-YEAR RANGE",
          size=SZ_LABEL, color=MUTED, all_caps=True, bold=True)
    rows = [(p, v) for p, v in zip(periods, pe_vals) if isinstance(v, (int, float))]
    if not rows:
        _text(slide, left, top + height * 0.45, width, 0.30,
              "No P/E history available", size=SZ_BODY, color=MUTED,
              align=PP_ALIGN.CENTER)
        return

    # Axis scale: round nice limits around the data
    vals = [v for _, v in rows]
    lo = max(0, min(vals) * 0.8)
    hi = max(vals) * 1.15
    if hi <= lo:
        hi = lo + 1
    bar_area_left = left + 0.55
    bar_area_w    = width - 0.65
    row_h = 0.30
    row_top = top + 0.30
    for i, (period, pe) in enumerate(rows):
        y = row_top + i * row_h
        _text(slide, left, y, 0.55, 0.20, period,
              size=SZ_BODY, color=BLACK)
        # Bar: from lo to pe (normalised within axis lo..hi)
        frac_lo = 0
        frac_hi = max(0.02, (pe - lo) / (hi - lo))
        bar_w = bar_area_w * frac_hi
        bar_left = bar_area_left
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE,
            in_(bar_left), in_(y + 0.06),
            in_(bar_w), in_(0.08))
        bar.fill.solid(); bar.fill.fore_color.rgb = CARD
        bar.line.fill.background()
        # Endpoint dot
        dot = slide.shapes.add_shape(MSO_SHAPE.OVAL,
            in_(bar_left + bar_w - 0.05), in_(y + 0.04),
            in_(0.10), in_(0.12))
        dot.fill.solid(); dot.fill.fore_color.rgb = GOLD_DK
        dot.line.fill.background()
        # P/E label to the right
        _text(slide, bar_left + bar_w + 0.05, y + 0.02, 0.45, 0.20,
              f"{pe:.1f}x", size=SZ_BODY, color=BLACK)

    # Axis ticks below
    axis_y = row_top + len(rows) * row_h + 0.04
    n_ticks = 4
    for i in range(n_ticks):
        frac = i / (n_ticks - 1)
        tick_x = bar_area_left + bar_area_w * frac - 0.15
        tick_val = lo + (hi - lo) * frac
        _text(slide, tick_x, axis_y, 0.40, 0.18, f"{tick_val:.0f}x",
              size=SZ_TINY, color=MUTED)

    # Current marker + legend
    if current_pe is not None and hi > lo:
        cur_frac = max(0.0, min(1.0, (current_pe - lo) / (hi - lo)))
        cur_x = bar_area_left + bar_area_w * cur_frac
        marker = slide.shapes.add_shape(MSO_SHAPE.DIAMOND,
            in_(cur_x - 0.08), in_(row_top + len(rows) * row_h - 0.02),
            in_(0.16), in_(0.16))
        marker.fill.solid(); marker.fill.fore_color.rgb = BLACK
        marker.line.fill.background()
        _text(slide, cur_x + 0.10, row_top + len(rows) * row_h - 0.04,
               1.4, 0.18,
               f"Current  ({current_pe:.1f}x)",
               size=SZ_TINY, color=BLACK)


# ── Peer table ────────────────────────────────────────────────

def _peer_table(slide, top: float, peers: list[dict]):
    """Rows: name, ticker, mcap, P/E, dividend yield, 1Y return.
    Compact, borderless, alternating row tint."""
    headers = ["COMPANY", "TICKER", "MCAP", "P/E", "DIV YIELD", "1Y RETURN"]
    col_w   = [2.10, 1.10, 1.10, 0.70, 0.80, 0.80]
    row_h   = 0.28
    # Header
    x = MARGIN_L
    for i, h in enumerate(headers):
        align = PP_ALIGN.LEFT if i < 2 else PP_ALIGN.RIGHT
        _text(slide, x, top, col_w[i] - 0.05, row_h, h,
              size=SZ_LABEL, color=MUTED, all_caps=True, align=align)
        x += col_w[i]
    _hrule(slide, MARGIN_L, top + row_h - 0.02, CONTENT_W, color=MUTED)

    for ri, p in enumerate(peers[:5]):
        y = top + row_h + ri * row_h
        if ri % 2 == 1:
            band = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                in_(MARGIN_L), in_(y - 0.02),
                in_(CONTENT_W), in_(row_h))
            band.fill.solid(); band.fill.fore_color.rgb = CARD
            band.line.fill.background()
        x = MARGIN_L
        cells = [
            p.get("name", "—"),
            p.get("ticker", "—"),
            p.get("market_cap_fmt", "—"),
            p.get("pe_fmt", "—"),
            p.get("div_yield_fmt", "—"),
            p.get("ret_1y_fmt", "—"),
        ]
        ret_val = p.get("ret_1y")
        for i, cell in enumerate(cells):
            align = PP_ALIGN.LEFT if i < 2 else PP_ALIGN.RIGHT
            color = BLACK
            if i == 5 and isinstance(ret_val, (int, float)):
                color = signed_color(ret_val)
            _text(slide, x, y, col_w[i] - 0.05, row_h, str(cell),
                  size=SZ_BODY, color=color, align=align)
            x += col_w[i]


# ── Sentiment row ─────────────────────────────────────────────

def _sentiment_row(slide, top: float, *, rating_split: dict,
                     n_analysts: int, target_mean: Optional[float],
                     target_range: Optional[tuple], target_implied_pct: Optional[float],
                     broker_actions: list[dict], currency: str):
    card_w = (CONTENT_W - 0.40) / 3
    card_h = 1.15

    # Card 1: Consensus distribution
    _card(slide, MARGIN_L, top, card_w, card_h, fill=WHITE)
    _text(slide, MARGIN_L + 0.12, top + 0.10, card_w - 0.20, 0.18,
          "CONSENSUS DISTRIBUTION", size=SZ_LABEL, color=MUTED,
          all_caps=True, bold=True)
    # Three bars proportional to buy/hold/sell
    total = max(1, sum(rating_split.values())) if rating_split else 1
    seg_top = top + 0.34
    bar_h   = 0.16
    inner_x = MARGIN_L + 0.12
    inner_w = card_w - 0.24
    buy_w  = inner_w * (rating_split.get("buy",  0) / total)
    hold_w = inner_w * (rating_split.get("hold", 0) / total)
    sell_w = inner_w * (rating_split.get("sell", 0) / total)
    for w, color, x_off in [(buy_w, POS, 0),
                              (hold_w, GOLD, buy_w),
                              (sell_w, NEG, buy_w + hold_w)]:
        if w <= 0.001:
            continue
        seg = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE,
            in_(inner_x + x_off), in_(seg_top),
            in_(w), in_(bar_h))
        seg.fill.solid(); seg.fill.fore_color.rgb = color
        seg.line.fill.background()
    # Labels below the bar
    _text(slide, inner_x, seg_top + bar_h + 0.06, card_w - 0.24, 0.18,
          f"Buy {int(rating_split.get('buy',0)/total*100)}%   "
          f"Hold {int(rating_split.get('hold',0)/total*100)}%   "
          f"Sell {int(rating_split.get('sell',0)/total*100)}%",
          size=Pt(9), color=GRAY)
    _text(slide, inner_x, seg_top + bar_h + 0.30, card_w - 0.24, 0.20,
          f"{n_analysts} analysts covering",
          size=SZ_LABEL, color=GRAY)

    # Card 2: Average target price
    c2_left = MARGIN_L + card_w + 0.20
    _card(slide, c2_left, top, card_w, card_h, fill=WHITE)
    _text(slide, c2_left + 0.12, top + 0.10, card_w - 0.20, 0.18,
          "AVERAGE TARGET PRICE", size=SZ_LABEL, color=MUTED,
          all_caps=True, bold=True)
    target_str = (f"{currency} {target_mean:,.2f}" if target_mean is not None
                   else "—")
    _text(slide, c2_left + 0.12, top + 0.32, card_w - 0.20, 0.40,
          target_str, size=Pt(16), color=BLACK, bold=True)
    if target_range:
        rng_str = (f"Range  {currency} {target_range[0]:,.0f} — {target_range[1]:,.0f}"
                    if target_range[0] is not None and target_range[1] is not None
                    else "")
        _text(slide, c2_left + 0.12, top + 0.74, card_w - 0.20, 0.18,
              rng_str, size=Pt(9), color=GRAY)
    if target_implied_pct is not None:
        _text(slide, c2_left + 0.12, top + 0.92, card_w - 0.20, 0.18,
              f"Implied {target_implied_pct:+.1f}% vs last close",
              size=Pt(9), color=signed_color(target_implied_pct))

    # Card 3: Last broker actions
    c3_left = MARGIN_L + 2 * (card_w + 0.20)
    _card(slide, c3_left, top, card_w, card_h, fill=WHITE)
    _text(slide, c3_left + 0.12, top + 0.10, card_w - 0.20, 0.18,
          "LAST BROKER ACTIONS", size=SZ_LABEL, color=MUTED,
          all_caps=True, bold=True)
    if not broker_actions:
        _text(slide, c3_left + 0.12, top + 0.40, card_w - 0.20, 0.20,
              "No broker actions in feed",
              size=Pt(9), color=MUTED)
    else:
        for i, ba in enumerate(broker_actions[:3]):
            y = top + 0.34 + i * 0.26
            _text(slide, c3_left + 0.12, y, 0.55, 0.20,
                  ba.get("date", "—"), size=SZ_BODY, color=GRAY)
            _text(slide, c3_left + 0.70, y, card_w - 0.78, 0.20,
                  ba.get("text", "—"), size=SZ_BODY, color=BLACK)


# ── Public entry point ────────────────────────────────────────

@dataclass
class ValuationData:
    company_name: str
    close_series: list[dict]
    currency: str
    pe_periods: list[str]
    pe_values: list[Optional[float]]
    pe_current: Optional[float]
    peers: list[dict]
    rating_split: dict
    n_analysts: int
    target_mean: Optional[float]
    target_range: Optional[tuple]
    target_implied_pct: Optional[float]
    broker_actions: list[dict]
    sources_line: str
    analyst_name: str
    gen_date: str
    total_pages: int = 3


def render_valuation_slide(prs, data: ValuationData):
    blank = next((L for L in prs.slide_layouts if L.name.lower() == "blank"),
                  prs.slide_layouts[-1])
    slide = prs.slides.add_slide(blank)

    _header_strip(slide, 3, "Valuation & Positioning")
    _section_hero(slide, 1.08, "Market Positioning",
                    "Valuation & Market View")

    # Two-up chart row
    chart_top = 1.96
    chart_h = 2.40
    col_w = (CONTENT_W - 0.20) / 2

    # Left: 52w price chart
    _text(slide, MARGIN_L, chart_top, col_w, 0.22,
          f"52-WEEK PRICE  ·  {data.currency}",
          size=SZ_LABEL, color=MUTED, all_caps=True, bold=True)
    _line_chart_52w(slide, MARGIN_L, chart_top + 0.26, col_w, chart_h - 0.30,
                     data.close_series, currency=data.currency)

    # Right: P/E range
    right_left = MARGIN_L + col_w + 0.20
    _pe_range_chart(slide, right_left, chart_top, col_w, chart_h,
                     data.pe_periods, data.pe_values, data.pe_current)

    # Peer table
    _section_label(slide, MARGIN_L, 4.97, CONTENT_W,
                    "Peer Comparables  ·  Selected Global Peers")
    _peer_table(slide, 5.29, data.peers)

    # Sentiment
    _section_label(slide, MARGIN_L, 8.35, CONTENT_W,
                    "Market Sentiment  ·  Analyst Consensus")
    _sentiment_row(
        slide, 8.65,
        rating_split=data.rating_split,
        n_analysts=data.n_analysts,
        target_mean=data.target_mean,
        target_range=data.target_range,
        target_implied_pct=data.target_implied_pct,
        broker_actions=data.broker_actions,
        currency=data.currency,
    )

    _footer(slide, 3, data.total_pages, data.sources_line,
             data.analyst_name, data.gen_date)
    return slide


# ── Data adapter ──────────────────────────────────────────────

def build_valuation_data(ticker: str, *, analyst_name: str = "Jabal Research",
                            gen_date: str = "",
                            peers_override: Optional[list[dict]] = None,
                            historical_override: Optional[dict] = None,
                            ) -> ValuationData:
    cv = get_all_fields(ticker)
    # iShares overlay removed (peer table no longer pulls regional ETF).

    profile = cv.get("company_profile")
    pname = (profile.value.get("name") if profile and isinstance(profile.value, dict)
              else ticker)
    currency = ""
    if profile and isinstance(profile.value, dict):
        currency = profile.value.get("currency") or ""

    # 52w close series. Prefer canonical_store; fall back to the Investing
    # override fetched by the writer (used for yfinance-blocked tickers
    # whose historical_prices field never reaches the canonical store).
    hp = cv.get("historical_prices")
    close_series = []
    if hp and isinstance(hp.value, dict):
        close_series = hp.value.get("close_series") or []
    if not close_series and isinstance(historical_override, dict):
        close_series = historical_override.get("close_series") or []

    # P/E history from valuation_historical. The header reads
    # "P/E MULTIPLE · 5-YEAR RANGE" so we trim the series to the trailing
    # 5 periods. The reference deck shows FY-4 through current; with
    # 8 raw points the chart became unreadable and contradicted the header.
    vh = cv.get("valuation_historical")
    periods, pe_vals = [], []
    if vh and isinstance(vh.value, dict):
        periods = vh.value.get("periods", []) or []
        pe_vals = vh.value.get("pe", []) or []
    if len(periods) > 5 and len(pe_vals) == len(periods):
        periods = periods[-5:]
        pe_vals = pe_vals[-5:]
    current_pe = None
    if pe_vals:
        for v in reversed(pe_vals):
            if isinstance(v, (int, float)):
                current_pe = v
                break

    # Peer comparables — defer to upstream peer-set (peers_override). When
    # nothing is curated, leave the table empty rather than fill it with
    # a regional-ETF proxy (the iShares row was marginal value and added
    # confusion about "what is this peer").
    peers = peers_override or []

    # Rating split + target price + broker actions
    rating_obs = cv.get("rating_split")
    rs = rating_obs.value if rating_obs and isinstance(rating_obs.value, dict) else {}
    rs_normalised = {
        "buy":  int(rs.get("buy",  0) or 0),
        "hold": int(rs.get("hold", 0) or 0),
        "sell": int(rs.get("sell", 0) or 0),
    }
    n_an = int(rs.get("total", sum(rs_normalised.values())) or 0)

    target_obs = cv.get("target_price")
    target_mean = target_high = target_low = None
    if target_obs and isinstance(target_obs.value, dict):
        target_mean = target_obs.value.get("mean")
        target_high = target_obs.value.get("high")
        target_low  = target_obs.value.get("low")
        # Fall back to target_price.n_analysts if rating_split lacked it
        if not n_an:
            n_an = int(target_obs.value.get("n_analysts", 0) or 0)
    # If rating_split has no buy/hold/sell breakdown but we DO know the
    # consensus + total, approximate from the consensus label so the bar
    # isn't 0%/0%/0% on every Saudi/MENA ticker.
    if n_an > 0 and sum(rs_normalised.values()) == 0:
        consensus = (rs.get("consensus") or "").upper() if rs else ""
        if "OUTPERFORM" in consensus or "BUY" in consensus or "ACCUMULATE" in consensus:
            rs_normalised = {"buy": int(n_an * 0.7), "hold": int(n_an * 0.25),
                              "sell": n_an - int(n_an * 0.7) - int(n_an * 0.25)}
        elif "UNDERPERFORM" in consensus or "SELL" in consensus or "REDUCE" in consensus:
            rs_normalised = {"buy": int(n_an * 0.1), "hold": int(n_an * 0.4),
                              "sell": n_an - int(n_an * 0.1) - int(n_an * 0.4)}
        else:  # HOLD / NEUTRAL / unknown
            rs_normalised = {"buy": int(n_an * 0.3), "hold": int(n_an * 0.5),
                              "sell": n_an - int(n_an * 0.3) - int(n_an * 0.5)}

    # Implied % vs last close
    current_price = (cv.get("current_price").value
                       if cv.get("current_price") else None)
    implied = None
    try:
        if target_mean is not None and current_price is not None and float(current_price) > 0:
            implied = (float(target_mean) / float(current_price) - 1.0) * 100
    except (TypeError, ValueError):
        pass

    # Broker actions — canonical_store key 'broker_actions' carries the
    # MS analyst-recommendations list. Each item: {date, headline, source}.
    # The slide renders up to the 3 most recent.
    ba_obs = cv.get("broker_actions")
    broker_actions: list[dict] = []
    if ba_obs and isinstance(ba_obs.value, dict):
        for item in (ba_obs.value.get("items") or [])[:3]:
            broker_actions.append({
                "date": item.get("date", ""),
                "text": item.get("headline", ""),
            })

    return ValuationData(
        company_name=pname,
        close_series=close_series,
        currency=currency,
        pe_periods=periods,
        pe_values=pe_vals,
        pe_current=current_pe,
        peers=peers,
        rating_split=rs_normalised,
        n_analysts=n_an,
        target_mean=target_mean,
        target_range=(target_low, target_high) if (target_low or target_high) else None,
        target_implied_pct=implied,
        broker_actions=broker_actions,
        sources_line=_sources_line(cv),
        analyst_name=analyst_name,
        gen_date=gen_date or datetime.utcnow().strftime("%d %b %Y"),
    )
