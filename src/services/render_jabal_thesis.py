"""
Jabal — Slide 2 (Thesis & Expectations) renderer.

Layout (per design_spec.md):
  1. Header strip
  2. Section label (INVESTMENT THESIS) + Georgia 17 title ("Executive Summary")
  3. Body card — 4-6 sentence thesis paragraph
  4. Section label (Q2 2026 EARNINGS EXPECTATIONS) + table:
        Metric | Jabal Est. | Consensus | Δ | YoY%
        rows for Revenue / EBITDA / EBITDA margin / Net income / EPS / Dividend
  5. Catalysts + Key Risks — two side-by-side cards (3 bullets each)
  6. Numbered "What to watch on the print"
  7. Footer

Data sources used:
  - canonical_store.valuation_forward            → consensus EPS/PE/etc.
  - canonical_store.valuation_historical         → YoY base
  - get_observations_by_provider(commodities)    → commodity context for thesis text
  - get_observations_by_provider(macro)          → IMF/WB context for thesis text
  - canonical_store.rating_split                 → consensus colour in opening sentence
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.util import Inches, Pt

from src.services.jabal_design_tokens import (
    BLACK, GRAY, MUTED, GOLD, POS, NEG, CARD, WHITE,
    FONT_DISPLAY, FONT_UI,
    SZ_SECTION, SZ_KICKER, SZ_VALUE, SZ_VALUE_LG, SZ_LABEL, SZ_BODY,
    SZ_META, SZ_HEADER, SZ_FOOTER, SZ_BULLET_PILL, SZ_TAB_NUM, SZ_TINY,
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


# ── Slide 2 sections ──────────────────────────────────────────

def _section_hero(slide, top: float, label: str, title: str):
    """Section label + Georgia 17 title underneath, used for slide 2/3 heroes."""
    _hrule(slide, MARGIN_L, top, CONTENT_W, color=MUTED)
    _text(slide, MARGIN_L, top + 0.10, CONTENT_W, 0.22, label,
          size=SZ_KICKER, color=GRAY, all_caps=True, bold=True)
    _text(slide, MARGIN_L, top + 0.32, CONTENT_W, 0.50, title,
          font=FONT_DISPLAY, size=SZ_SECTION, color=BLACK)


def _body_card(slide, left: float, top: float, width: float, height: float,
                body: str):
    """Cream-fill card with gold left-accent and the thesis paragraph inside."""
    _card(slide, left, top, width, height, fill=CARD, border=MUTED,
           left_accent=GOLD)
    # Inset 0.20 from accent edge
    _text(slide, left + 0.20, top + 0.10, width - 0.32, height - 0.20,
          body, size=SZ_BODY, color=BLACK)


def _estimates_table(slide, top: float, rows: list[dict]):
    """Borderless 4-column table.

    rows: list of dicts {metric, jabal, consensus, delta_bps_or_pct, yoy_pct}
    Column widths sum to CONTENT_W."""
    headers = ["METRIC", "JABAL EST.", "CONSENSUS", "Δ vs CONSENSUS", "YoY"]
    col_w   = [2.40, 1.10, 1.10, 1.10, 0.90]
    row_h   = 0.30
    header_top = top
    # Header
    x = MARGIN_L
    for i, h in enumerate(headers):
        align = PP_ALIGN.LEFT if i == 0 else PP_ALIGN.RIGHT
        _text(slide, x, header_top, col_w[i] - 0.05, row_h, h,
              size=SZ_LABEL, color=MUTED, all_caps=True, align=align)
        x += col_w[i]
    # Header rule
    _hrule(slide, MARGIN_L, header_top + row_h - 0.02, CONTENT_W,
            color=MUTED)
    # Body rows
    for ri, row in enumerate(rows):
        y = header_top + row_h + ri * row_h
        # Zebra fill (alt row tint) — every other row
        if ri % 2 == 1:
            band = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                in_(MARGIN_L), in_(y - 0.02),
                in_(CONTENT_W), in_(row_h))
            band.fill.solid(); band.fill.fore_color.rgb = CARD
            band.line.fill.background()
        x = MARGIN_L
        for i, key in enumerate(["metric", "jabal", "consensus", "delta", "yoy"]):
            val = row.get(key, "—")
            align = PP_ALIGN.LEFT if i == 0 else PP_ALIGN.RIGHT
            color = BLACK
            if key == "delta" and isinstance(val, (int, float)):
                color = signed_color(val)
                val = f"{val:+.1f}%"
            elif key == "yoy" and isinstance(val, (int, float)):
                color = signed_color(val)
                val = f"{val:+.1f}%"
            elif val is None:
                val = "—"
            _text(slide, x, y, col_w[i] - 0.05, row_h, str(val),
                  size=SZ_BODY, color=color, align=align)
            x += col_w[i]


def _two_col_pillared_card(slide, top: float, height: float,
                              left_title: str, left_bullets: list[str],
                              right_title: str, right_bullets: list[str]):
    """Side-by-side cards (Catalysts | Key Risks)."""
    card_w = (CONTENT_W - 0.20) / 2
    # LEFT
    _card(slide, MARGIN_L, top, card_w, height, fill=WHITE, border=MUTED,
           left_accent=POS)
    _text(slide, MARGIN_L + 0.18, top + 0.08, card_w - 0.20, 0.30,
          left_title, size=SZ_KICKER, color=GRAY, all_caps=True, bold=True)
    bullet_y = top + 0.50
    for b in left_bullets[:3]:
        _bullet_dot(slide, MARGIN_L + 0.20, bullet_y + 0.02, color=POS)
        _text(slide, MARGIN_L + 0.42, bullet_y - 0.02, card_w - 0.50, 0.42, b,
              size=SZ_BODY, color=BLACK)
        bullet_y += 0.46
    # RIGHT
    r_left = MARGIN_L + card_w + 0.20
    _card(slide, r_left, top, card_w, height, fill=WHITE, border=MUTED,
           left_accent=NEG)
    _text(slide, r_left + 0.18, top + 0.08, card_w - 0.20, 0.30,
          right_title, size=SZ_KICKER, color=GRAY, all_caps=True, bold=True)
    bullet_y = top + 0.50
    for b in right_bullets[:3]:
        _bullet_dot(slide, r_left + 0.20, bullet_y + 0.02, color=NEG)
        _text(slide, r_left + 0.42, bullet_y - 0.02, card_w - 0.50, 0.42, b,
              size=SZ_BODY, color=BLACK)
        bullet_y += 0.46


def _bullet_dot(slide, left, top, *, color=GOLD, size=0.12):
    dot = slide.shapes.add_shape(MSO_SHAPE.OVAL,
                                  in_(left), in_(top),
                                  in_(size), in_(size))
    dot.fill.solid()
    dot.fill.fore_color.rgb = color
    dot.line.fill.background()
    return dot


def _numbered_list(slide, top: float, items: list[str]):
    """1/2/3 numerals in gold + body text. Used for 'What to watch on the print'."""
    row_h = 0.27
    for i, body in enumerate(items[:5]):
        y = top + i * row_h
        _text(slide, MARGIN_L, y, 0.25, 0.22, str(i + 1),
              size=SZ_BODY, color=GOLD, bold=True)
        _text(slide, MARGIN_L + 0.30, y, CONTENT_W - 0.30, 0.22, body,
              size=SZ_BODY, color=BLACK)


# ── Public entry point ────────────────────────────────────────

@dataclass
class ThesisData:
    """Slide 2 inputs. Built by the orchestrator from canonical_store +
    light templating of commodities / macro context."""
    exec_summary_body: str
    estimates_rows: list[dict]
    estimates_footnote: str
    catalysts: list[str]
    risks: list[str]
    watch_list: list[str]
    sources_line: str
    analyst_name: str
    gen_date: str
    total_pages: int = 3


def render_thesis_slide(prs, data: ThesisData):
    blank = next((L for L in prs.slide_layouts if L.name.lower() == "blank"),
                  prs.slide_layouts[-1])
    slide = prs.slides.add_slide(blank)

    _header_strip(slide, 2, "Thesis & Expectations")

    # Investment Thesis hero + body card
    _section_hero(slide, 1.08, "Investment Thesis", "Executive Summary")
    _body_card(slide, MARGIN_L, 1.96, CONTENT_W, 1.85,
                data.exec_summary_body)

    # Q2 estimates
    _section_label(slide, MARGIN_L, 3.96, CONTENT_W, "Q2 2026 Earnings Expectations")
    _text(slide, MARGIN_L, 4.26, CONTENT_W, 0.18,
          "Jabal estimates vs. consensus  ·  Local currency unless stated",
          size=Pt(9), color=GRAY)
    _estimates_table(slide, 4.48, data.estimates_rows)
    _text(slide, MARGIN_L, 6.88, CONTENT_W, 0.18,
          data.estimates_footnote,
          size=Pt(9), color=GRAY)

    # Catalysts + Risks
    _two_col_pillared_card(
        slide, 7.38, 1.95,
        "Catalysts", data.catalysts,
        "Key Risks", data.risks,
    )

    # Numbered list
    _section_label(slide, MARGIN_L, 9.53, CONTENT_W, "What to Watch on the Print")
    _numbered_list(slide, 9.81, data.watch_list)

    _footer(slide, 2, data.total_pages, data.sources_line,
             data.analyst_name, data.gen_date)
    return slide


# ── Data adapter ──────────────────────────────────────────────

def _template_exec_summary(cv: dict, commodities: dict,
                              macro_obs: dict) -> str:
    """Compose a 4–6 sentence thesis paragraph from canonical data.
    This is a generic template; for a polished deck the analyst rewrites
    it, but the data-driven scaffold means it's never blank."""
    name = "the company"
    sector = industry = "—"
    profile = cv.get("company_profile")
    if profile and isinstance(profile.value, dict):
        name = profile.value.get("name") or name
        sector = profile.value.get("sector") or sector
        industry = profile.value.get("industry") or industry

    rating_val = cv.get("rating_split")
    rating = ""
    n_an = 0
    if rating_val and isinstance(rating_val.value, dict):
        rating = (rating_val.value.get("consensus") or "").lower()
        n_an   = int(rating_val.value.get("total") or 0)

    target = cv.get("target_price")
    target_text = ""
    if target and isinstance(target.value, dict):
        m = target.value.get("mean")
        if m:
            target_text = f"; consensus target {m:.2f}"

    val_hist = cv.get("valuation_historical")
    pe_text = ""
    if val_hist and isinstance(val_hist.value, dict):
        pe = val_hist.value.get("pe")
        if isinstance(pe, list) and any(pe):
            recent = [p for p in pe if isinstance(p, (int, float))]
            if recent:
                pe_text = f" The shares trade at a P/E around {recent[-1]:.1f}x trailing earnings."

    commodity_text = ""
    industry_commodities = (commodities.get("company_profile") or {}).get("industry_commodities", {}) \
        if commodities else {}
    if industry_commodities:
        bits = []
        for tag, info in industry_commodities.items():
            if not isinstance(info, dict):
                continue
            val = info.get("value")
            yoy = info.get("yoy_pct")
            unit = info.get("unit", "")
            if val is None:
                continue
            yoy_str = f" ({yoy:+.1f}% YoY)" if yoy is not None else ""
            bits.append(f"{tag} at {val:.0f} {unit}{yoy_str}")
        if bits:
            commodity_text = " The macro and commodity backdrop is anchored by " \
                + "; ".join(bits[:2]) + "."

    macro_text = ""
    mp = macro_obs.get("company_profile") if macro_obs else None
    if mp:
        gdp_act = mp.get("gdp_growth_pct")
        gdp_fcst = mp.get("gdp_growth_fcst_next_pct")
        infl_fcst = mp.get("inflation_fcst_next_pct")
        parts = []
        if gdp_act is not None:
            parts.append(f"local-economy GDP growth recently at {gdp_act:.1f}%")
        if gdp_fcst is not None:
            parts.append(f"IMF projecting {gdp_fcst:.1f}% next year")
        if infl_fcst is not None:
            parts.append(f"inflation forecast at {infl_fcst:.1f}%")
        if parts:
            macro_text = " Macro context: " + ", ".join(parts) + "."

    rating_line = ""
    if rating and n_an:
        rating_line = f" Street consensus is {rating} ({n_an} analysts covering){target_text}."

    body = (
        f"Jabal maintains a constructive view into the upcoming print. "
        f"{name} sits in the {sector} sector ({industry})."
        f"{rating_line}"
        f"{pe_text}"
        f"{commodity_text}"
        f"{macro_text}"
        f" The thesis hinges on three factors: demand trajectory through H2, "
        f"feedstock/input-cost discipline, and management's tone on guidance "
        f"during the call. We expect the print itself to be within consensus "
        f"bounds; the read on forward commentary is the swing factor."
    )
    return body


def _build_estimates_rows(cv: dict) -> list[dict]:
    """Build the Q2 estimates table from canonical_store + Jabal stub.

    For the proof-of-concept Jabal estimate column derives from MS forward
    consensus with a slight bias adjustment — in production this is the
    analyst's own model. Falls back to '—' for any metric we don't have."""
    val_fwd = cv.get("valuation_forward")
    fwd = val_fwd.value if val_fwd and isinstance(val_fwd.value, dict) else {}
    val_hist = cv.get("valuation_historical")
    hist = val_hist.value if val_hist and isinstance(val_hist.value, dict) else {}

    def _pair(name: str, jabal_v, consensus_v, yoy_pct=None) -> dict:
        delta = None
        try:
            if jabal_v is not None and consensus_v is not None and consensus_v != 0:
                delta = (float(jabal_v) / float(consensus_v) - 1.0) * 100
        except (TypeError, ValueError):
            pass
        return {
            "metric":    name,
            "jabal":     "—" if jabal_v is None else f"{jabal_v:,.2f}",
            "consensus": "—" if consensus_v is None else f"{consensus_v:,.2f}",
            "delta":     delta if delta is not None else "—",
            "yoy":       yoy_pct if yoy_pct is not None else "—",
        }

    rows = []

    def _firstnum(*keys):
        for k in keys:
            v = fwd.get(k)
            if isinstance(v, (int, float)):
                return v
        return None

    def _fmt_money_b(v):
        """Render a raw money value as e.g. 'SAR 493.3B' when large."""
        if not isinstance(v, (int, float)):
            return None
        if abs(v) >= 1e12: return f"{v/1e12:,.2f}T"
        if abs(v) >= 1e9:  return f"{v/1e9:,.1f}B"
        if abs(v) >= 1e6:  return f"{v/1e6:,.0f}M"
        return f"{v:,.0f}"

    # Two-column FY estimates: FY+1 EPS + Revenue + Next-Q EPS.
    fy1_year = fwd.get("fy1_year") or fwd.get("fy_year") or ""
    fy2_year = fwd.get("fy2_year") or ""
    next_q_period = fwd.get("next_q_period") or "Next Q"

    eps_fy1 = _firstnum("eps_fy1", "eps_2026", "eps_2027", "eps")
    rev_fy1 = _firstnum("revenue_fy1", "revenue_2026", "revenue_2027", "revenue")
    eps_fy2 = _firstnum("eps_fy2", "eps_2027", "eps_2028")
    rev_fy2 = _firstnum("revenue_fy2", "revenue_2027", "revenue_2028")
    eps_q   = _firstnum("eps_next_q")
    rev_q   = _firstnum("revenue_next_q")

    rows.append({
        "metric": f"EPS — {next_q_period}",
        "jabal": "—",
        "consensus": (f"{eps_q:.3f}" if eps_q else "—"),
        "delta": "—", "yoy": "—",
    })
    rows.append({
        "metric": f"Revenue — {next_q_period}",
        "jabal": "—",
        "consensus": (_fmt_money_b(rev_q) or "—"),
        "delta": "—", "yoy": "—",
    })
    rows.append({
        "metric": f"EPS — FY{fy1_year}" if fy1_year else "EPS (FY est.)",
        "jabal": "—",
        "consensus": (f"{eps_fy1:.3f}" if eps_fy1 else "—"),
        "delta": "—", "yoy": "—",
    })
    rows.append({
        "metric": f"Revenue — FY{fy1_year}" if fy1_year else "Revenue (FY est.)",
        "jabal": "—",
        "consensus": (_fmt_money_b(rev_fy1) or "—"),
        "delta": "—", "yoy": "—",
    })
    if eps_fy2 or rev_fy2:
        rows.append({
            "metric": f"EPS — FY{fy2_year}" if fy2_year else "EPS (FY+1 est.)",
            "jabal": "—",
            "consensus": (f"{eps_fy2:.3f}" if eps_fy2 else "—"),
            "delta": "—", "yoy": "—",
        })
        rows.append({
            "metric": f"Revenue — FY{fy2_year}" if fy2_year else "Revenue (FY+1)",
            "jabal": "—",
            "consensus": (_fmt_money_b(rev_fy2) or "—"),
            "delta": "—", "yoy": "—",
        })
    # Dividend yield (TTM, from canonical)
    div_y = (cv.get("dividend_yield").value
              if cv.get("dividend_yield") else None)
    rows.append({"metric": "Dividend yield (TTM)",
                  "jabal": "—",
                  "consensus": (f"{float(div_y):.2f}%" if div_y is not None else "—"),
                  "delta": "—", "yoy": "—"})
    return rows


def build_thesis_data(ticker: str, *, analyst_name: str = "Jabal Research",
                        gen_date: str = "",
                        catalysts: Optional[list[str]] = None,
                        risks: Optional[list[str]] = None,
                        watch_list: Optional[list[str]] = None,
                        ) -> ThesisData:
    cv = get_all_fields(ticker)
    commodities_obs = get_observations_by_provider(ticker, "commodities")
    macro_obs       = get_observations_by_provider(ticker, "macro")
    investing_obs   = get_observations_by_provider(ticker, "investing")

    summary = _template_exec_summary(cv, commodities_obs, macro_obs)
    rows = _build_estimates_rows(cv)

    # Compose the table footnote: lead with the next-Q anchor (date,
    # consensus source, analyst count) since that's the strongest data
    # point we have for the full panel.
    val_fwd = cv.get("valuation_forward")
    rs = cv.get("rating_split")
    fwd_dict = val_fwd.value if val_fwd and isinstance(val_fwd.value, dict) else {}
    rs_dict  = rs.value if rs and isinstance(rs.value, dict) else {}
    n_an = int(rs_dict.get("total", 0) or 0)
    nq_period = fwd_dict.get("next_q_period") or ""
    nq_date   = fwd_dict.get("next_q_report_date") or ""
    fwd_source = (val_fwd.canonical_source if val_fwd else "—").title()
    footnote_bits = []
    if nq_period and nq_date:
        footnote_bits.append(f"Next print: {nq_date} (period {nq_period})")
    footnote_bits.append(f"Consensus: {fwd_source}")
    if n_an:
        footnote_bits.append(f"{n_an} analysts covering")
    estimates_footnote = "  ·  ".join(footnote_bits)

    # Surface Investing's surprise history as a track-record catalyst line.
    surprise = (investing_obs.get("income_statement_quarterly") or {}).get(
        "surprise_history", [])
    track_record_catalyst = None
    if surprise:
        beats = sum(1 for r in surprise[:4]
                     if isinstance(r.get("eps_surprise_pct"), (int, float))
                     and r["eps_surprise_pct"] > 0)
        n = min(4, len(surprise))
        last = surprise[0]
        last_dir = "beat" if (last.get("eps_surprise_pct") or 0) > 0 else "missed"
        last_pct = abs(last.get("eps_surprise_pct") or 0)
        track_record_catalyst = (
            f"EPS {last_dir} consensus by {last_pct:.1f}% last quarter; "
            f"{beats} of last {n} quarters above estimates"
        )
    default_catalysts = [
        track_record_catalyst or
        "Positive quarterly surprise consistent with prior track record",
        "Constructive guidance on H2 demand and pricing outlook",
        "Capex/project milestones tracking on schedule",
    ]

    from datetime import datetime
    return ThesisData(
        exec_summary_body=summary,
        estimates_rows=rows,
        estimates_footnote=estimates_footnote,
        catalysts=catalysts or default_catalysts,
        risks=risks or [
            "Cautious management tone could validate target-price gap",
            "Feedstock / input cost volatility pressures margin trajectory",
            "Macro / commodity-price softness weighs on top-line growth",
        ],
        watch_list=watch_list or [
            "Forward demand commentary — Q3 order book and pricing trajectory",
            "Feedstock cost outlook and supply-chain commentary",
            "Updated capex schedule and any project-pipeline updates",
        ],
        sources_line=", ".join(sorted({
            c.canonical_source for c in cv.values()
        })) or "free-source stack",
        analyst_name=analyst_name,
        gen_date=gen_date or datetime.utcnow().strftime("%d %b %Y"),
    )
