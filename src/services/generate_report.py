"""Earnings preview: PPTX output + sector IV helpers (imported by qa_engine)."""
from __future__ import annotations
import os
from datetime import datetime
from pathlib import Path

from src.config import cfg, report_output_dir
from src.models.report_payload import ReportPayload
from src.models.step_result import Status, StepResult, StepTimer

STEP = "generate_report"

# Minimum character length for IV paragraphs before using fallback (with Recent Context we accept shorter LLM output)
MIN_IV_LEN_WITH_RECENT_CONTEXT = 20
MIN_IV_LEN_DEFAULT = 40
IV_STYLE_DEFAULT = "balanced"
IV_STYLE_ALLOWED = {"balanced", "tactical", "conservative"}



def _fmt_num(val, in_millions: bool = False) -> str:
    if val is None:
        return "—"
    if isinstance(val, (int, float)):
        if in_millions and val >= 1e3:
            return f"{val / 1e3:,.2f}bn"
        if val >= 1e9:
            return f"{val / 1e9:.2f}B"
        if val >= 1e6:
            return f"{val / 1e6:,.0f}M"
        if abs(val) < 1e-6 and val != 0:
            return f"{val:.4f}"
        return f"{val:,.2f}" if val != int(val) else f"{int(val):,}"
    return str(val)


def _fmt_pct(val, signed: bool = False) -> str:
    if val is None:
        return "—"
    if isinstance(val, (int, float)):
        return f"{val:+.1f}%" if signed else f"{val}%"
    return str(val)


def _field_display(f, default: str = "—"):
    """Return display value only when field status allows rendering; else default."""
    if not f or not isinstance(f, dict):
        return default
    if f.get("status") not in ("pass", "stale", "estimated", "manually_entered"):
        return default
    v = f.get("display_value") if f.get("display_value") is not None else f.get("value")
    return v if v is not None else default


def _recent_context_allowed_sources() -> set[str]:
    """Allowed provider ids for Recent Context (from config); memo does not hardcode providers."""
    try:
        sources = cfg().get("news", {}).get("recent_context_sources") or ["reuters", "zawya"]
        return {str(s).strip().lower() for s in sources if s}
    except Exception:
        return {"reuters", "zawya"}


def _is_valid_recent_context_article(art) -> bool:
    """Only fully validated articles: headline, URL, and from an allowed context provider."""
    headline = (getattr(art, "headline", None) or "").strip()
    url = (getattr(art, "url", None) or "").strip()
    source = (getattr(art, "source", None) or "").strip().lower()
    if not headline or headline == "(No title)":
        return False
    if not url or not url.startswith("http"):
        return False
    if source and source not in _recent_context_allowed_sources():
        return False
    return True


def _company_attr(company, key: str, default: str = ""):
    """Read sector/industry/etc. from company whether it's a model instance or a dict (e.g. after serialization)."""
    if company is None:
        return default
    if isinstance(company, dict):
        return (company.get(key) or default) if isinstance(default, str) else company.get(key, default)
    return (getattr(company, key, None) or default) if isinstance(default, str) else getattr(company, key, default)


def _sector_operating_kpis_and_what_matters(company) -> tuple[list[str], list[str], str]:
    """
    Return (operating_metrics_kpis[4], what_matters_bullets[5], fallback_para2_snippet).
    fallback_para2 is publishable analyst prose only (no "Focus on...", "Do not use..." instructions).
    """
    sector = (_company_attr(company, "sector", "") or "").strip().lower()
    industry = (_company_attr(company, "industry", "") or "").strip().lower()
    ind = industry or sector
    is_bank = bool(_company_attr(company, "is_bank", False))

    if is_bank:
        kpis = ["Loans", "Deposits", "NIM", "Cost of Risk"]
        matters = ["Loan / financing growth", "NIM / margin", "Asset quality", "Funding mix", "Capital return"]
        p2 = "For banks, the story usually turns on NIM, loan growth, and asset quality. Earnings quality—recurring versus one-offs—and any weakness versus consensus or deterioration in asset quality are key for the multiple."
        return kpis, matters, p2

    if "oil" in ind or "gas" in ind or "energy" in sector or "exploration" in ind or "petroleum" in ind:
        kpis = ["Production volumes", "Realized oil/gas prices", "Lifting costs", "Capex / project ramp-up"]
        matters = ["Production volumes", "Realized oil/gas prices", "Lifting costs", "Reserve replacement / field startup", "Capex and project ramp-up"]
        p2 = "For oil and gas, the narrative typically turns on production volumes, realized prices, and lifting costs; reserve replacement and field startup impact also matter, and capex and project ramp-up often drive the story."
        return kpis, matters, p2

    if "telecom" in ind or "communication" in sector or "communication" in ind:
        kpis = ["Subscribers", "ARPU", "Churn", "Capex intensity"]
        matters = ["Subscriber additions", "ARPU trend", "Churn", "Capex intensity", "India wireless competition" if "india" in ((_company_attr(company, "country", "") or "").lower()) else "Wireless competition", "Enterprise / data centre contribution"]
        p2 = "For telecoms and communication equipment, subscriber trends, ARPU, churn, and capex intensity are central where relevant; enterprise and product-cycle dynamics often drive the story."
        return kpis, matters[:5], p2

    if "technology" in sector or "software" in ind or "semiconductor" in ind or "equipment" in ind:
        kpis = ["Revenue growth", "Margin", "Guidance", "Key product metrics"]
        matters = ["Revenue mix and growth", "Margin and profitability", "Guidance", "Product cycles", "Competitive dynamics"]
        p2 = "For technology and communication equipment names, the narrative typically turns on revenue mix, margins, and guidance; product cycles and competitive dynamics often drive the stock."
        return kpis, matters[:5], p2

    if "industrial" in sector or "capital good" in ind or "aerospace" in ind or "machinery" in ind:
        kpis = ["Orders / backlog", "Utilization", "Pricing", "Guidance"]
        matters = ["Demand and orders", "Backlog / utilization", "Margin and pricing", "Guidance", "Key metrics"]
        p2 = "For industrials, demand, orders, backlog, and utilization drive the story; margin and pricing matter, and guidance and key metrics often move the stock."
        return kpis, matters, p2

    if "internet" in ind or "e-commerce" in ind or "retail" in ind:
        kpis = ["GMV", "Cloud revenue growth", "International commerce", "Customer management revenue"]
        matters = ["GMV and engagement", "Margin and pricing", "Guidance", "Key metrics"]
        p2 = "Sector operating metrics and headline results versus consensus are key; guidance and main metrics typically drive the stock."
        return kpis, matters[:5], p2

    if "mining" in ind or "metals" in ind:
        kpis = ["Production / throughput", "Commodity prices", "Costs", "Guidance"]
        matters = ["Production and sales volume", "Commodity price realizations", "Cost and margin", "Guidance", "Key metrics"]
        p2 = "For metals and mining, the narrative typically turns on production, realized commodity prices, and costs; guidance and key operating metrics often drive the stock."
        return kpis, matters[:5], p2

    if "chem" in ind or "material" in ind or "material" in sector:
        kpis = ["Volume", "Realized price", "Utilization", "Feedstock spread"]
        matters = ["Volume and realized price", "Utilization", "Feedstock spread", "Guidance", "Key metrics"]
        p2 = "Volume, realized price, utilization, and feedstock spread are the main levers; guidance and key metrics drive the story."
        return kpis, matters[:5], p2

    # Default: labeled rows for manual entry
    kpis = ["Key metric 1", "Key metric 2", "Key metric 3", "Key metric 4"]
    matters = ["Headline vs consensus", "Margin and pricing", "Guidance", "Key metrics"]
    p2 = "This quarter, sector operating metrics and headline results versus consensus matter most; earnings quality—whether a beat or miss is recurring or one-off—and guidance drive the narrative."
    return kpis, matters[:5], p2


def _iv_fallback_style() -> str:
    """
    Fallback IV style selector.
    Priority: env IV_FALLBACK_STYLE -> config report.iv_fallback_style -> default balanced.
    """
    style = (os.environ.get("IV_FALLBACK_STYLE") or "").strip().lower()
    if not style:
        try:
            style = str(cfg().get("report", {}).get("iv_fallback_style", "")).strip().lower()
        except Exception:
            style = ""
    if style not in IV_STYLE_ALLOWED:
        style = IV_STYLE_DEFAULT
    return style


def _build_analytical_iv_paragraph_1(
    company_name: str,
    preview_short: str,
    rec: str,
    an_str: str,
    price: float | None,
    spread: float | None,
    rev_surprise: float | None,
    eps_surprise: float | None,
    memo: dict,
    _fmt_pct,
    _fmt_num,
    style: str = IV_STYLE_DEFAULT,
) -> str:
    """
    Build a single analytical paragraph for the Investment View fallback.
    Interprets consensus, surprise history, and key preview rather than listing data points.
    """
    sentences = []

    # Opening: frame the setup
    if style == "tactical":
        sentences.append(f"Into the {preview_short} print, {company_name} screens as a tactical setup.")
    elif style == "conservative":
        sentences.append(f"Into {preview_short}, {company_name} has a constructive but not low-risk setup.")
    else:
        sentences.append(f"{company_name} reports {preview_short}.")

    # Street view and what underpins it
    if rec and rec != "—":
        line = f"The street has a {rec} rating"
        if an_str:
            line += f" ({an_str})"
        if price is not None:
            line += f", with the average target at {_fmt_num(price)}"
            if spread is not None:
                line += f", implying {_fmt_pct(spread, signed=True)} upside"
        line += "."
        sentences.append(line)
        # Interpret surprise history
        if rev_surprise is not None and eps_surprise is not None:
            rev_beat = rev_surprise > 0
            eps_beat = eps_surprise > 0
            if rev_beat and not eps_beat:
                if style == "tactical":
                    sentences.append(
                        f"Revenue has tended to beat (avg {_fmt_pct(rev_surprise, signed=True)}) while EPS has lagged ({_fmt_pct(eps_surprise, signed=True)}); "
                        "the immediate trigger is whether top-line resilience converts into cleaner earnings."
                    )
                elif style == "conservative":
                    sentences.append(
                        f"Revenue has tended to beat consensus (avg {_fmt_pct(rev_surprise, signed=True)}), "
                        f"while EPS has lagged ({_fmt_pct(eps_surprise, signed=True)}), so earnings conversion remains the main risk into the quarter."
                    )
                else:
                    sentences.append(
                        f"Revenue has tended to beat consensus (avg {_fmt_pct(rev_surprise, signed=True)}), "
                        f"while EPS has lagged ({_fmt_pct(eps_surprise, signed=True)}); the story into the print hinges on whether top-line strength can translate into earnings delivery."
                    )
            elif not rev_beat and eps_beat:
                sentences.append(
                    f"EPS has run ahead of consensus (avg {_fmt_pct(eps_surprise, signed=True)}), though revenue has been softer ({_fmt_pct(rev_surprise, signed=True)}); the focus will be on sustainability of margins and guidance."
                )
            elif rev_beat and eps_beat:
                sentences.append(
                    f"Both revenue and EPS have tended to beat (revenue avg {_fmt_pct(rev_surprise, signed=True)}, EPS {_fmt_pct(eps_surprise, signed=True)}), which supports the constructive setup but raises the bar for this quarter."
                )
            else:
                sentences.append(
                    f"Versus consensus, revenue has averaged {_fmt_pct(rev_surprise, signed=True)} and EPS {_fmt_pct(eps_surprise, signed=True)}; the quarter will need to show improvement or a clear path to it for the rating to hold."
                )
        elif rev_surprise is not None:
            sentences.append(
                f"Revenue versus consensus has averaged {_fmt_pct(rev_surprise, signed=True)}; "
                + ("that consistency supports the constructive view." if rev_surprise > 0 else "delivery this quarter will be important for confidence.")
            )
        elif eps_surprise is not None:
            sentences.append(
                f"EPS surprise has averaged {_fmt_pct(eps_surprise, signed=True)}; "
                + ("earnings delivery has underpinned the rating." if eps_surprise > 0 else "the market will be looking for better earnings consistency.")
            )

    # Key preview: tougher comp / context for the quarter
    calendar_prior = memo.get("calendar_prior_quarter_released") or {}
    calendar_same_ly = memo.get("calendar_same_q_prior_yr_released") or {}
    has_prior = (calendar_prior.get("net_sales") is not None) or (memo.get("prior_quarter_actual_revenue") is not None)
    has_same_ly = (calendar_same_ly.get("net_sales") is not None) or (memo.get("same_quarter_prior_year_revenue") is not None)
    qoq_rev = memo.get("qoq_revenue_pct")
    yoy_rev = memo.get("yoy_revenue_pct_table")
    if (qoq_rev is not None and has_prior) or (yoy_rev is not None and has_same_ly):
        q_part = _fmt_pct(qoq_rev, signed=True) if (qoq_rev is not None and has_prior) else None
        y_part = _fmt_pct(yoy_rev, signed=True) if (yoy_rev is not None and has_same_ly) else None
        if q_part is not None or y_part is not None:
            bits = []
            if q_part is not None:
                bits.append(f"QoQ {q_part}")
            if y_part is not None:
                bits.append(f"YoY {y_part}")
            preview_phrase = " and ".join(bits)
            if style == "tactical":
                sentences.append(
                    f"Key preview points to {preview_phrase}; with tougher comps, an in-line outcome may be enough only if margin/cost delivery is clean."
                )
            elif style == "conservative":
                sentences.append(
                    f"Key preview points to {preview_phrase}—a tougher comparison that raises execution risk; "
                    "the quarter needs a credible delivery path to preserve confidence."
                )
            else:
                sentences.append(
                    f"The key preview points to {preview_phrase}—a tougher comparison; "
                    "the focus will be on whether the company can meet or beat the bar and sustain the narrative."
                )

    # Expectations into the print
    if spread is not None:
        if spread > 0:
            if style == "tactical":
                sentences.append("Expectations look supportive; an in-line or better print likely keeps near-term positioning constructive.")
            elif style == "conservative":
                sentences.append("Expectations look supportive, but the reaction still depends on earnings quality and guidance credibility.")
            else:
                sentences.append("Expectations into the print look supportive; an in-line or better outcome would likely be well received.")
        elif spread < 0:
            sentences.append("Expectations look demanding; the stock may need a clear beat or raise to re-rate.")
        else:
            sentences.append("Expectations into the print look balanced.")
    else:
        sentences.append("Expectations into the print look balanced.")

    return " ".join(sentences)



def _iv_text_and_watch(payload: ReportPayload, memo_data: dict | None, iv_style: str) -> tuple[str, list[str]]:
    """LLM IV if long enough; else analytical + sector p2 + optional recent-coverage snippet."""
    c, memo = payload.company, payload.memo_computed or {}
    sections = (memo_data or {}).get("pptx_sections") or {}
    if isinstance(sections, dict):
        thesis = (sections.get("investment_thesis") or "").strip()
        wtw = sections.get("what_to_watch") if isinstance(sections.get("what_to_watch"), list) else []
        wtw = [str(x).strip() for x in (wtw or []) if str(x).strip()]
        if thesis:
            return thesis, (wtw[:4] if wtw else _sector_operating_kpis_and_what_matters(c)[1])
    ns = getattr(payload, "news_summary", None)
    min_len = MIN_IV_LEN_WITH_RECENT_CONTEXT if (ns and getattr(ns, "referenced_articles", None)) else MIN_IV_LEN_DEFAULT
    p1 = (getattr(ns, "investment_view_paragraph_1", "") or "").strip() if ns else ""
    p2 = (getattr(ns, "investment_view_paragraph_2", "") or "").strip() if ns else ""
    _, matters, p2_fb = _sector_operating_kpis_and_what_matters(c)
    if len(p1) >= min_len and len(p2) >= min_len:
        return f"{p1} {p2}".strip(), matters
    header = (memo_data or {}).get("header") or {}
    rec = _field_display(header.get("recommendation"), "—")
    n_an = _field_display(header.get("analyst_count"))
    try:
        n_an = int(n_an) if n_an not in (None, "—", "") else None
    except (TypeError, ValueError):
        n_an = None
    an_str = f"{n_an} analysts" if isinstance(n_an, int) and n_an else ""
    price = _field_display(header.get("average_target_price"))
    try:
        price = float(price) if price not in (None, "—", "") else None
    except (TypeError, ValueError):
        price = None
    spread = _field_display(header.get("upside_pct"))
    try:
        spread = float(spread) if spread not in (None, "—", "") else None
    except (TypeError, ValueError):
        spread = None
    preview_short = (memo_data or {}).get("preview_short") or memo.get("preview_quarter_short") or f"{(datetime.now().month - 1) // 3 + 1}Q{datetime.now().strftime('%y')}"
    company_name = getattr(c, "company_name", None) or _company_attr(c, "company_name", "")
    fb1 = _build_analytical_iv_paragraph_1(
        company_name=company_name,
        preview_short=preview_short,
        rec=rec,
        an_str=an_str,
        price=price,
        spread=spread,
        rev_surprise=memo.get("avg_revenue_surprise_pct"),
        eps_surprise=memo.get("avg_eps_surprise_pct"),
        memo=memo,
        _fmt_pct=_fmt_pct,
        _fmt_num=_fmt_num,
        style=iv_style,
    )
    art = next((a for a in (getattr(payload, "news_items", None) or []) if _is_valid_recent_context_article(a)), None)
    if art:
        fact = (getattr(art, "extracted_fact", None) or getattr(art, "snippet", None) or getattr(art, "headline", None) or "").strip()
        if len(fact) > 200:
            fact = fact[:197] + "…"
        if fact:
            fb1 += f" Recent coverage: {fact}"
    # Expand fallback IV so exec summary is more comprehensive even if Gemini fails.
    focus = ""
    try:
        focus_bits = [m for m in (matters or []) if m][:4]
        if focus_bits:
            focus = " Key focus areas include " + ", ".join(focus_bits) + "."
    except Exception:
        focus = ""
    return f"{fb1} {p2_fb}{focus}".strip(), matters


def _write_preview_pptx(
    payload: ReportPayload,
    path: Path,
    memo_data: dict | None,
    iv_text: str,
    watch: list[str],
    quality_flags: list[str] | None = None,
) -> None:
    from pptx import Presentation
    from pptx.dml.color import RGBColor
    from pptx.enum.shapes import MSO_SHAPE
    from pptx.enum.text import PP_ALIGN
    from pptx.util import Inches, Pt

    def pn(v, bil=False):
        if v is None:
            return "—"
        try:
            x = float(v)
            if bil and abs(x) >= 1e9:
                return f"{x/1e9:.1f}B"
            if abs(x) >= 1e6:
                return f"{x:,.0f}"
            return f"{x:,.2f}" if x != int(x) else f"{int(x):,}"
        except (TypeError, ValueError):
            return str(v)

    def pp(v, signed=False):
        if v is None:
            return "—"
        try:
            x = float(v)
            return f"{x:+.1f}%" if signed else f"{x}%"
        except (TypeError, ValueError):
            return str(v)

    def rat(rec):
        if rec is None or str(rec).strip() in ("", "—"):
            return "—"
        return str(rec).strip().upper()[:20]

    def rdate(exp_date):
        if exp_date is None or str(exp_date).strip() in ("", "—"):
            return "—"
        s = str(exp_date).strip()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            try:
                y, m, d = int(s[:4]), int(s[5:7]), int(s[8:10])
                mo = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
                if 1 <= m <= 12:
                    return f"{d} {mo[m-1]} {y}"
            except Exception:
                pass
        return s

    def qlab(ps):
        raw = (ps or f"{(datetime.now().month - 1) // 3 + 1}Q{datetime.now().strftime('%y')}").replace(" ", "").strip().upper()
        qn = next((c for c in raw if c in "1234"), "1")
        _cy2 = datetime.now().strftime("%y")
        _search_yrs = [_cy2, str(int(_cy2)+1), str(int(_cy2)-1), str(int(_cy2)+2)]
        yr = next(("20" + y for y in _search_yrs if y in raw), str(datetime.now().year))
        return f"Q{qn} {yr}"

    def tx(
        sl,
        x,
        y,
        w,
        h,
        t,
        *,
        sz=14,
        bold=False,
        rgb=RGBColor(0, 0, 0),
        al=PP_ALIGN.LEFT,
        word_wrap: bool = True,
        line_spacing: float | None = None,
    ):
        b = sl.shapes.add_textbox(x, y, w, h)
        tf = b.text_frame
        tf.clear()
        tf.word_wrap = word_wrap
        # Keep generous but not wasteful padding so short bullet lists fit.
        tf.margin_left = Pt(2)
        tf.margin_right = Pt(2)
        tf.margin_top = Pt(2)
        tf.margin_bottom = Pt(2)
        text = "" if t is None else str(t)
        lines = text.split("\n") if text else [""]

        def _style_paragraph(p):
            p.alignment = al
            if line_spacing is not None:
                try:
                    p.line_spacing = line_spacing
                except Exception:
                    pass
            try:
                p.space_after = Pt(0)
                p.space_before = Pt(0)
            except Exception:
                pass

        p = tf.paragraphs[0]
        def _set_para(para, line_text):
            para.text = line_text
            _style_paragraph(para)
            if para.runs:
                run = para.runs[0]
                run.font.name = "Arial"
                run.font.size = Pt(sz)
                run.font.bold = bold
                run.font.color.rgb = rgb

        _set_para(p, lines[0])

        for ln in lines[1:]:
            pp = tf.add_paragraph()
            _set_para(pp, ln)

    def rect(sl, x, y, w, h, fill, line=None, lw=1.0):
        sh = sl.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, w, h)
        sh.fill.solid()
        sh.fill.fore_color.rgb = fill
        if line is None:
            sh.line.fill.background()
        else:
            sh.line.color.rgb = line
            sh.line.width = Pt(lw)
        return sh

    c, q, memo = payload.company, payload.quote, payload.memo_computed or {}
    header, cs, vm = (memo_data or {}).get("header") or {}, payload.consensus_summary or {}, payload.ms_valuation_multiples or {}
    # MS price fallback when Yahoo has no price (Oman, Bahrain, some UAE)
    _ms_price = cs.get("last_close_price") if cs else None
    _ms_ccy = cs.get("price_currency") if cs else None
    name = getattr(c, "company_name", None) or _company_attr(c, "company_name", "")
    tk = getattr(c, "ticker", None) or _company_attr(c, "ticker", "")
    sec = f"{_company_attr(c, 'sector', '')} / {_company_attr(c, 'industry', '')}".strip(" /") or "—"
    curr = (getattr(c, "currency", None) or "USD").strip()
    if not curr or curr == "USD":
        curr = _ms_ccy or curr  # Use MS currency if company currency unknown
    display_ccy = (cfg().get("report", {}).get("display_currency", "") or "").strip().upper() or curr
    pshort = (memo_data or {}).get("preview_short") or memo.get("preview_quarter_short") or f"{(datetime.now().month - 1) // 3 + 1}Q{datetime.now().strftime('%y')}"
    q_label = qlab(pshort)
    ed = header.get("expected_report_date") or {}
    ev = ed.get("display_value") if isinstance(ed, dict) else ed
    exp = rdate(ev) if ev else (memo.get("next_earnings_date") or "—")
    rr = header.get("recommendation") or {}
    rec = (rr.get("display_value") if isinstance(rr, dict) else rr) or (cs.get("consensus_rating") or "—")
    tr = header.get("average_target_price") or {}
    tgt = (tr.get("display_value") if isinstance(tr, dict) else tr)
    tgt = tgt if tgt is not None else cs.get("average_target_price")
    sr = header.get("upside_pct") or {}
    spr = (sr.get("display_value") if isinstance(sr, dict) else sr)
    spr = None  # Always recalculate from live price for accuracy
    mcap = q.market_cap if q else None
    # Use MS price as fallback when Yahoo has no price (frontier markets)
    _live_price = (q.price if q else None) or _ms_price
    if not mcap and _ms_price:
        # Estimate market cap from MS last_close × shares (from valuation page)
        _shares = None
        try:
            _shares_arr = vm.get("shares") or []
            _shares = next((s for s in reversed(_shares_arr) if s), None)
        except Exception:
            pass
    # Yahoo fallbacks for rating/target
    if q:
        if rec in (None, "—", ""):
            _yrec = getattr(q, "recommendation_key", None) or ""
            if _yrec and _yrec != "none":
                rec = _yrec.upper().replace("_", " ")
        if tgt is None and getattr(q, "target_mean_price", None):
            tgt = q.target_mean_price
    # Compute upside from best available price (Yahoo live → MS last_close)
    if tgt is not None and _live_price and _live_price > 0:
        try:
            spr = round((float(tgt) - _live_price) / _live_price * 100, 1)
        except (TypeError, ValueError):
            spr = memo.get("spread_pct") or cs.get("upside_to_average_target_pct")
    if display_ccy and display_ccy != curr:
        try:
            from src.utils.currency import convert
            tgt_conv = convert(tgt if isinstance(tgt, (int, float)) else None, curr, display_ccy) if tgt is not None else None
            mcap_conv = convert(mcap if isinstance(mcap, (int, float)) else None, curr, display_ccy) if mcap is not None else None
            # Only switch display currency when conversion succeeds (no invented FX fallbacks).
            if tgt_conv is not None or mcap_conv is not None:
                if tgt_conv is not None:
                    tgt = tgt_conv
                if mcap_conv is not None:
                    mcap = mcap_conv
                curr = display_ccy
        except Exception:
            pass
    ts = pn(tgt)
    if curr and ts != "—":
        ts = f"{curr} {ts}"
    ms = pn(mcap, bil=True)
    if curr and ms != "—":
        ms = f"{curr} {ms}"
    cp, cn = memo.get("calendar_prior_quarter_released") or {}, memo.get("calendar_next_quarter") or {}

    # ── Annual fallback when quarterly data is unavailable ──
    _af = getattr(payload, "ms_annual_forecasts", None) or {}
    _ann = _af.get("annual", {}) if isinstance(_af, dict) else {}
    _eps_div = getattr(payload, "ms_eps_dividend_forecasts", None) or {}
    _ann_periods = _ann.get("periods") or _eps_div.get("periods") or []
    _ann_fy_prior = -1
    _ann_fy_est = -1
    # Use announcement dates to determine actual vs estimate boundary:
    # If announcement date exists (not "-" or empty), it's an actual.
    _ann_dates = _ann.get("announcement_dates") or []
    for _ai, _ap in enumerate(_ann_periods):
        _has_ann_date = (_ai < len(_ann_dates) and _ann_dates[_ai] and str(_ann_dates[_ai]).strip() not in ("", "-", "None"))
        if _has_ann_date:
            _ann_fy_prior = _ai  # latest actual (keeps overwriting)
        elif _ann_fy_est == -1:
            _ann_fy_est = _ai    # first estimate
    # Fallback if no announcement dates available: use year-based logic
    if _ann_fy_prior == -1 and _ann_fy_est == -1:
        _curr_yr = datetime.now().year
        for _ai, _ap in enumerate(_ann_periods):
            _yr = "".join(c2 for c2 in str(_ap) if c2.isdigit())[:4]
            try:
                _yr_int = int(_yr)
            except ValueError:
                continue
            if _yr_int < _curr_yr:
                _ann_fy_prior = _ai
            if _yr_int >= _curr_yr and _ann_fy_est == -1:
                _ann_fy_est = _ai
    if _ann_fy_est == -1 and len(_ann_periods) >= 2:
        _ann_fy_prior = len(_ann_periods) - 2
        _ann_fy_est = len(_ann_periods) - 1

    _vm = getattr(payload, "ms_valuation_multiples", None) or {}

    def _ann_val(key, idx):
        arr = _ann.get(key) or []
        if 0 <= idx < len(arr) and arr[idx] is not None:
            return arr[idx]
        if key == "eps":
            # Fallback 1: /valuation-dividend/ page
            eps_arr = _eps_div.get("eps") or []
            if 0 <= idx < len(eps_arr) and eps_arr[idx] is not None:
                return eps_arr[idx]
            # Fallback 2: /valuation/ page (has EPS in its own row)
            vm_eps = _vm.get("eps") or []
            if 0 <= idx < len(vm_eps) and vm_eps[idx] is not None:
                return vm_eps[idx]
        return None

    def _yoy_pct(prior, est):
        if prior and est and isinstance(prior, (int, float)) and isinstance(est, (int, float)) and prior != 0:
            return round((est - prior) / abs(prior) * 100, 1)
        return None

    _has_quarterly = bool((cp.get("net_sales") or cp.get("revenue")) and (cn.get("net_sales") or cn.get("revenue")))
    _cM = f"({curr}M)" if curr else "(M)"
    _cU = f"({curr})" if curr else ""
    if _has_quarterly:
        rows = [
            (f"Revenue {_cM}", cp.get("net_sales"), cn.get("net_sales") or cn.get("revenue"), memo.get("yoy_revenue_pct_table")),
            (f"EBITDA {_cM}", cp.get("ebitda"), cn.get("ebitda"), memo.get("yoy_ebitda_pct_table")),
            (f"Net Income {_cM}", cp.get("net_income"), cn.get("net_income"), memo.get("yoy_ni_pct_table")),
            (f"EPS {_cU}", cp.get("eps"), cn.get("eps"), memo.get("yoy_eps_pct_table")),
            (f"FCF {_cM}", cp.get("fcf"), cn.get("fcf"), None),
        ]
    else:
        _rev_p, _rev_e = _ann_val("net_sales", _ann_fy_prior), _ann_val("net_sales", _ann_fy_est)
        _ebitda_p, _ebitda_e = _ann_val("ebitda", _ann_fy_prior), _ann_val("ebitda", _ann_fy_est)
        _ni_p, _ni_e = _ann_val("net_income", _ann_fy_prior), _ann_val("net_income", _ann_fy_est)
        _ebit_p, _ebit_e = _ann_val("ebit", _ann_fy_prior), _ann_val("ebit", _ann_fy_est)
        _eps_p = _ann_val("eps", _ann_fy_prior)
        _eps_e = _ann_val("eps", _ann_fy_est)
        rows = [
            (f"Revenue {_cM}", _rev_p, _rev_e, _yoy_pct(_rev_p, _rev_e)),
            (f"EBITDA {_cM}", _ebitda_p or _ebit_p, _ebitda_e or _ebit_e, _yoy_pct(_ebitda_p or _ebit_p, _ebitda_e or _ebit_e)),
            (f"Net Income {_cM}", _ni_p, _ni_e, _yoy_pct(_ni_p, _ni_e)),
            (f"EPS {_cU}", _eps_p, _eps_e, _yoy_pct(_eps_p, _eps_e)),
            (f"FCF {_cM}", _ann_val("fcf", _ann_fy_prior), _ann_val("fcf", _ann_fy_est), None),
        ]

    # Last resort: Yahoo or MS income statement actuals when forecasts unavailable
    _rows_empty = all(r[1] is None and r[2] is None for r in rows)
    if _rows_empty:
        # Try MS income statement actuals first (works for Oman, Bahrain, frontier markets)
        _ms_is = getattr(payload, "ms_income_statement_actuals", None) or {}
        _ms_is_periods = _ms_is.get("periods") or []
        _ms_is_rev = _ms_is.get("total_revenues") or _ms_is.get("revenues_before_provision_for_loan_losses") or []
        _ms_is_ni = _ms_is.get("net_income_is") or _ms_is.get("net_income_to_company") or []
        _ms_is_eps = _ms_is.get("eps_basic") or []
        if len(_ms_is_periods) >= 2 and (_ms_is_rev or _ms_is_ni):
            _p, _c = -2, -1  # second-last and last period
            def _ms_is_val(arr, idx):
                if arr and 0 <= (len(arr)+idx) < len(arr):
                    return arr[idx]
                return None
            def _ms_toM(v):
                if v is None: return None
                try:
                    x = float(v)
                    return round(x / 1e6, 1) if abs(x) >= 1e6 else x
                except: return v
            rows = [
                (f"Revenue {_cM}", _ms_toM(_ms_is_val(_ms_is_rev, _p)), _ms_toM(_ms_is_val(_ms_is_rev, _c)), _yoy_pct(_ms_is_val(_ms_is_rev, _p), _ms_is_val(_ms_is_rev, _c))),
                (f"Net Income {_cM}", _ms_toM(_ms_is_val(_ms_is_ni, _p)), _ms_toM(_ms_is_val(_ms_is_ni, _c)), _yoy_pct(_ms_is_val(_ms_is_ni, _p), _ms_is_val(_ms_is_ni, _c))),
                (f"EPS {_cU}", _ms_is_val(_ms_is_eps, _p), _ms_is_val(_ms_is_eps, _c), _yoy_pct(_ms_is_val(_ms_is_eps, _p), _ms_is_val(_ms_is_eps, _c))),
            ]

    _rows_empty = all(r[1] is None and r[2] is None for r in rows)
    if _rows_empty:
        ya = sorted(getattr(payload, "annual_actuals", None) or [], key=lambda p: p.period_label, reverse=True)
        if len(ya) >= 2:
            _ya_cur, _ya_pri = ya[0], ya[1]
            def _toM(v):
                """Scale raw Yahoo values to millions. Yahoo returns full units (e.g. 5e8 for 500M)."""
                if v is None: return None
                try:
                    x = float(v)
                    return round(x / 1e6, 1) if abs(x) >= 1e6 else x
                except (TypeError, ValueError):
                    return v
            _is_bank = bool(_company_attr(c, "is_bank", False))
            _ebitda_row = (f"EBITDA {_cM}", _toM(_ya_pri.ebitda), _toM(_ya_cur.ebitda), _yoy_pct(_ya_pri.ebitda, _ya_cur.ebitda))
            # Banks: replace EBITDA with EBIT (operating income) if available
            if _is_bank and _ya_pri.ebitda is None:
                _ebitda_row = (f"EBIT {_cM}", _toM(getattr(_ya_pri, "ebit", None)), _toM(getattr(_ya_cur, "ebit", None)), _yoy_pct(getattr(_ya_pri, "ebit", None), getattr(_ya_cur, "ebit", None)))
            rows = [
                (f"Revenue {_cM}", _toM(_ya_pri.revenue), _toM(_ya_cur.revenue), _yoy_pct(_ya_pri.revenue, _ya_cur.revenue)),
                _ebitda_row,
                (f"Net Income {_cM}", _toM(_ya_pri.net_income), _toM(_ya_cur.net_income), _yoy_pct(_ya_pri.net_income, _ya_cur.net_income)),
                (f"EPS {_cU}", _ya_pri.eps, _ya_cur.eps, _yoy_pct(_ya_pri.eps, _ya_cur.eps)),
                (f"FCF {_cM}", None, None, None),
            ]

    pv = vm.get("periods") or []
    _cy = str(datetime.now().year)
    _ny = str(datetime.now().year + 1)
    i26 = next((i for i, p in enumerate(pv) if _cy in str(p) or _ny in str(p)), len(pv) - 1 if pv else -1)

    def pick(arr):
        if not arr:
            return None
        if 0 <= i26 < len(arr) and arr[i26] is not None:
            return arr[i26]
        for v in reversed(arr):
            if v is not None:
                return v
        return None

    pe, evv, pb, dy = pick(vm.get("pe") or []), pick(vm.get("ev_ebitda") or []) or pick(vm.get("ev_ebit") or []), pick(vm.get("pbr") or []), pick(vm.get("yield_pct") or [])
    # Yahoo fallback for valuation
    if q:
        if pe is None:
            pe = getattr(q, "forward_pe", None) or getattr(q, "trailing_pe", None)
        if evv is None:
            evv = getattr(q, "ev_to_ebitda", None)
        if pb is None:
            pb = getattr(q, "price_to_book", None)
        if dy is None:
            _dy_raw = getattr(q, "dividend_yield", None)
            if _dy_raw is not None:
                dy = round(_dy_raw * 100, 2) if _dy_raw < 1 else _dy_raw
    drv = getattr(payload, "derived", None)
    pe_vs = getattr(drv, "pe_vs_sector_pct", None) if drv is not None else None
    ev_vs = getattr(drv, "ev_ebitda_vs_sector_pct", None) if drv is not None else None
    sections = (memo_data or {}).get("pptx_sections") or {}
    wl = (sections.get("what_to_watch") if isinstance(sections, dict) else None) or watch or [
        "Guidance revision and forward outlook commentary",
        "Segment-level performance and geographic mix",
        "Macro factors: FX, commodity pricing, regulatory",
        "Capital allocation: buybacks, dividends, M&A",
    ]
    rv, ev_eps = cn.get("net_sales") or cn.get("revenue"), cn.get("eps")
    rc, ec = memo.get("yoy_revenue_pct_table") or memo.get("qoq_revenue_pct"), memo.get("yoy_eps_pct_table") or memo.get("qoq_eps_pct")
    em = cn.get("ebitda_margin") or cp.get("ebitda_margin")
    if not rv and not _has_quarterly:
        rv = _ann_val("net_sales", _ann_fy_est)
        rc = _yoy_pct(_ann_val("net_sales", _ann_fy_prior), rv)
    if not ev_eps and not _has_quarterly:
        ev_eps = _ann_val("eps", _ann_fy_est)
        ec = _yoy_pct(_ann_val("eps", _ann_fy_prior), ev_eps)
    if not em and not _has_quarterly:
        _ebitda_est = _ann_val("ebitda", _ann_fy_est) or _ann_val("ebit", _ann_fy_est)
        _rev_est = _ann_val("net_sales", _ann_fy_est)
        if _ebitda_est and _rev_est and _rev_est != 0:
            em = round(_ebitda_est / _rev_est * 100, 1)
    # Yahoo actuals fallback for key expectations when all else fails
    if not rv and _rows_empty:
        _ya_sorted = sorted(getattr(payload, "annual_actuals", None) or [], key=lambda p: p.period_label, reverse=True)
        if _ya_sorted:
            _toM_l = lambda v: round(float(v) / 1e6, 1) if v is not None and abs(float(v)) >= 1e8 else v
            rv = _toM_l(_ya_sorted[0].revenue)
            ev_eps = _ya_sorted[0].eps
            if len(_ya_sorted) >= 2 and _ya_sorted[1].revenue:
                rc = _yoy_pct(_ya_sorted[1].revenue, _ya_sorted[0].revenue)
                ec = _yoy_pct(_ya_sorted[1].eps, ev_eps)
            if _ya_sorted[0].ebitda and _ya_sorted[0].revenue and _ya_sorted[0].revenue != 0:
                em = round(_ya_sorted[0].ebitda / _ya_sorted[0].revenue * 100, 1)
    DARK, GOLD, LIGHT, MUTED, GREEN, WHITE, BLACK = (
        RGBColor(0x0D, 0x11, 0x17), RGBColor(0xC9, 0xA2, 0x27), RGBColor(0xE6, 0xED, 0xF3),
        RGBColor(0x8B, 0x94, 0x9E), RGBColor(0x3F, 0xB9, 0x50), RGBColor(0xFF, 0xFF, 0xFF), RGBColor(0x1F, 0x23, 0x28),
    )
    prs = Presentation()
    prs.slide_width, prs.slide_height = Inches(13.33), Inches(7.5)
    blank = prs.slide_layouts[6]
    s1 = prs.slides.add_slide(blank)
    rect(s1, 0, 0, prs.slide_width, prs.slide_height, DARK)
    tx(s1, Inches(0.8), Inches(0.55), Inches(6), Inches(0.4), "EARNINGS PREVIEW NOTE", sz=11, bold=True, rgb=GOLD)
    rect(s1, Inches(0.8), Inches(0.95), Inches(2.2), Inches(0.05), GOLD)
    tx(s1, Inches(0.8), Inches(1.55), Inches(11.6), Inches(0.9), name or "—", sz=44, bold=True, rgb=LIGHT)
    tx(s1, Inches(0.8), Inches(2.55), Inches(8), Inches(0.6), f"{q_label} Earnings Preview", sz=22, rgb=LIGHT)
    my = 3.55
    for i, (lb, vl) in enumerate([("Sector:", sec), ("Ticker:", tk), ("Market Cap:", ms), ("Report Date:", exp)]):
        tx(s1, Inches(0.8), Inches(my + i * 0.35), Inches(2.0), Inches(0.3), lb, sz=12, rgb=MUTED)
        tx(s1, Inches(2.2), Inches(my + i * 0.35), Inches(7.5), Inches(0.3), str(vl), sz=12, bold=True, rgb=LIGHT)
    sx, sy, sw, sh = Inches(0.8), Inches(5.45), Inches(11.0), Inches(1.05)
    rect(s1, sx, sy, sw, sh, RGBColor(0x10, 0x17, 0x22), GOLD)
    cw = sw / 3
    for j, (lb, vl, col) in enumerate([("RATING", rat(rec), LIGHT), ("TARGET PRICE", ts, LIGHT), ("UPSIDE", pp(spr, True) if spr is not None else "—", GREEN)]):
        x = sx + cw * j
        tx(s1, x + Inches(0.25), sy + Inches(0.15), cw - Inches(0.3), Inches(0.25), lb, sz=10, bold=True, rgb=MUTED)
        tx(s1, x + Inches(0.25), sy + Inches(0.45), cw - Inches(0.3), Inches(0.45), str(vl), sz=24, bold=True, rgb=col)
        if j in (0, 1):
            rect(s1, x + cw - Inches(0.02), sy + Inches(0.15), Inches(0.02), sh - Inches(0.3), RGBColor(0x30, 0x36, 0x3D))
    tx(s1, Inches(0), Inches(7.15), prs.slide_width, Inches(0.3), "CONFIDENTIAL | For Institutional Clients Only", sz=10, rgb=MUTED, al=PP_ALIGN.CENTER)

    s2 = prs.slides.add_slide(blank)
    rect(s2, 0, 0, prs.slide_width, prs.slide_height, WHITE)
    tx(s2, Inches(0.8), Inches(0.45), Inches(12), Inches(0.35), f"{name} | {q_label}", sz=14, bold=True, rgb=BLACK)
    tx(s2, Inches(0.8), Inches(0.95), Inches(6), Inches(0.6), "Executive Summary", sz=30, bold=True, rgb=BLACK)
    rect(s2, Inches(0.8), Inches(1.55), Inches(2.4), Inches(0.08), GOLD)
    rect(s2, Inches(0.8), Inches(1.85), Inches(11.8), Inches(1.35), RGBColor(0xFA, 0xF8, 0xF3), RGBColor(0xDB, 0xE0, 0xE6))
    rect(s2, Inches(0.8), Inches(1.85), Inches(0.08), Inches(1.35), GOLD)
    iv_len = len((iv_text or "").strip())
    # Slide 2: tighter line-height for longer LLM theses.
    # Font scales down as text gets longer to reduce vertical overflow.
    iv_sz = 14 if iv_len <= 520 else (13 if iv_len <= 720 else (12 if iv_len <= 900 else 11))
    tx(
        s2,
        Inches(1.0),
        Inches(2.05),
        Inches(11.4),
        Inches(1.05),
        iv_text or "—",
        sz=iv_sz,
        rgb=BLACK,
        line_spacing=0.85,
    )
    tx(s2, Inches(0.8), Inches(3.35), Inches(4), Inches(0.35), "Key Expectations", sz=16, bold=True, rgb=BLACK)
    cy, ch, cw2, g = Inches(3.75), Inches(1.05), Inches(3.75), Inches(0.3)
    _ib = bool(_company_attr(c, "is_bank", False))
    cards = [("Revenue", pn(rv), pp(rc, True) if rc is not None else "—"), ("EPS", pn(ev_eps), pp(ec, True) if ec is not None else "—"), ("EBITDA Margin", pp(em) if em is not None else ("N/A*" if _ib else "—"), "—")]
    for i, (lb, va, chg) in enumerate(cards):
        x = Inches(0.8) + i * (cw2 + g)
        rect(s2, x, cy, cw2, ch, WHITE, RGBColor(0xDB, 0xE0, 0xE6))
        tx(s2, x + Inches(0.25), cy + Inches(0.15), cw2 - Inches(0.5), Inches(0.25), lb, sz=12, rgb=MUTED)
        tx(s2, x + Inches(0.25), cy + Inches(0.42), cw2 - Inches(0.5), Inches(0.35), va, sz=22, bold=True, rgb=BLACK)
        tx(s2, x + Inches(0.25), cy + Inches(0.78), cw2 - Inches(0.5), Inches(0.25), chg, sz=12, bold=True, rgb=GREEN)
    tx(s2, Inches(0.8), Inches(4.95), Inches(4), Inches(0.35), "What to Watch", sz=16, bold=True, rgb=BLACK)
    wx, wy = Inches(0.8), Inches(5.35)
    for i, item in enumerate(wl[:4]):
        cx = wx + (Inches(5.9) if i % 2 else Inches(0))
        cyy = wy + (Inches(0.55) if i >= 2 else Inches(0))
        circ = s2.shapes.add_shape(MSO_SHAPE.OVAL, cx, cyy, Inches(0.32), Inches(0.32))
        circ.fill.solid()
        circ.fill.fore_color.rgb = RGBColor(0xF0, 0xE6, 0xD3)
        circ.line.fill.background()
        tx(s2, cx, cyy + Inches(0.01), Inches(0.32), Inches(0.32), str(i + 1), sz=12, bold=True, rgb=BLACK, al=PP_ALIGN.CENTER)
        tx(s2, cx + Inches(0.42), cyy - Inches(0.02), Inches(5.3), Inches(0.4), item, sz=13, rgb=BLACK)
    tx(s2, Inches(0.8), Inches(6.35), Inches(4.5), Inches(0.35), "Catalysts & Risks", sz=16, bold=True, rgb=BLACK)
    bx, by, bw3, bh3 = Inches(0.8), Inches(6.7), Inches(5.8), Inches(0.7)
    rx = Inches(6.8)
    rect(s2, bx, by, bw3, bh3, RGBColor(0xF0, 0xF9, 0xF0), RGBColor(0xDB, 0xE0, 0xE6))
    rect(s2, bx, by, Inches(0.08), bh3, RGBColor(0x1A, 0x7F, 0x37))
    # Bullets are rendered as separate paragraphs (one per bullet) and word wrapping is disabled.
    # The LLM is instructed to keep each bullet short enough to fit horizontally.
    tx(s2, bx + Inches(0.18), by + Inches(0.06), bw3 - Inches(0.3), Inches(0.22), "CATALYSTS", sz=10, bold=True, rgb=RGBColor(0x1A, 0x7F, 0x37))

    c_list = (sections.get("catalysts") if isinstance(sections, dict) else None) or []
    catalysts = [str(x).strip() for x in c_list if str(x).strip()][:3] if isinstance(c_list, list) else []
    if not catalysts:
        catalysts = ["Product / volume upside", "Cost or mix tailwind", "Positive policy / regulatory development"]
    tx(
        s2,
        bx + Inches(0.18),
        by + Inches(0.26),
        bw3 - Inches(0.3),
        Inches(0.44),
        "↑ " + "\n↑ ".join(catalysts),
        sz=8,
        rgb=BLACK,
        word_wrap=False,
        line_spacing=0.85,
    )
    rect(s2, rx, by, bw3, bh3, RGBColor(0xFE, 0xF0, 0xF0), RGBColor(0xDB, 0xE0, 0xE6))
    rect(s2, rx, by, Inches(0.08), bh3, RGBColor(0xCF, 0x22, 0x22))
    r_list = (sections.get("risks") if isinstance(sections, dict) else None) or []
    risks = [str(x).strip() for x in r_list if str(x).strip()][:3] if isinstance(r_list, list) else []
    if not risks:
        uf = (memo_data or {}).get("uncertainty_factors") or []
        risks = [str(x).strip() for x in uf if str(x).strip()][:3] if isinstance(uf, list) else []
    if not risks:
        risks = ["Macro / demand downside", "Pricing / competition pressure", "Execution or guidance risk"]

    tx(s2, rx + Inches(0.18), by + Inches(0.06), bw3 - Inches(0.3), Inches(0.22), "KEY RISKS", sz=10, bold=True, rgb=RGBColor(0xCF, 0x22, 0x22))
    tx(
        s2,
        rx + Inches(0.18),
        by + Inches(0.26),
        bw3 - Inches(0.3),
        Inches(0.44),
        "↓ " + "\n↓ ".join(risks),
        sz=8,
        rgb=BLACK,
        word_wrap=False,
        line_spacing=0.85,
    )

    s3 = prs.slides.add_slide(blank)
    rect(s3, 0, 0, prs.slide_width, prs.slide_height, WHITE)
    tx(s3, Inches(0.8), Inches(0.65), Inches(8), Inches(0.6), "Financial Snapshot", sz=32, bold=True, rgb=BLACK)
    rect(s3, Inches(0.8), Inches(1.25), Inches(2.7), Inches(0.08), GOLD)
    tbx, tby = Inches(0.8), Inches(1.6)
    cws = [Inches(4.0), Inches(2.3), Inches(2.3), Inches(2.0)]
    rh = Inches(0.45)
    if _has_quarterly:
        hdrs = ["Metric", "Q prior (A)", "Q next (E)", "YoY %"]
    elif _first_est_period:
        _last_act = None
        for _i2, _d2 in enumerate(_ann_dates_early):
            if _d2 and str(_d2).strip() not in ("", "-", "None"):
                _last_act = _ann_periods_early[_i2] if _i2 < len(_ann_periods_early) else None
        hdrs = ["Metric", f"{_last_act or 'Prior'} (A)", f"{_first_est_period} (E)", "YoY %"]
    else:
        hdrs = ["Metric", "Prior (A)", "Current (E)", "YoY %"]
    x = tbx
    for j, h in enumerate(hdrs):
        rect(s3, x, tby, cws[j], rh, BLACK, RGBColor(0xDB, 0xE0, 0xE6))
        tx(s3, x + Inches(0.15), tby + Inches(0.08), cws[j] - Inches(0.3), rh, h, sz=12, bold=True, rgb=WHITE)
        x += cws[j]
    rows = [(lb, pa, ce, yy) for lb, pa, ce, yy in rows if pa is not None or ce is not None]
    for i, (lb, pa, ce, yy) in enumerate(rows):
        y = tby + rh * (i + 1)
        x = tbx
        vals = [lb, pn(pa), pn(ce), pp(yy, True) if yy is not None else "—"]
        for j, v in enumerate(vals):
            fl = RGBColor(0xFA, 0xF8, 0xF3) if j == 2 else WHITE
            rect(s3, x, y, cws[j], rh, fl, RGBColor(0xDB, 0xE0, 0xE6))
            tx(s3, x + Inches(0.15), y + Inches(0.08), cws[j] - Inches(0.3), rh, str(v), sz=12, bold=(j == 0), rgb=BLACK)
            x += cws[j]
    tx(s3, Inches(0.8), Inches(4.6), Inches(7), Inches(0.5), "Valuation Summary", sz=26, bold=True, rgb=BLACK)
    rect(s3, Inches(0.8), Inches(5.05), Inches(2.5), Inches(0.06), GOLD)
    _is_bank_l = bool(_company_attr(c, "is_bank", False))
    _ev_label = "EV/EBITDA"
    _ev_val = f"{evv:.1f}x" if evv is not None else ("N/A*" if _is_bank_l else "—")
    boxes = [
        ("P/E (FY26E)", f"{pe:.1f}x" if pe is not None else "—", pe_vs),
        (_ev_label, _ev_val, ev_vs),
        ("P/B", f"{pb:.1f}x" if pb is not None else "—", None),
        ("Div. Yield", f"{dy:.1f}%" if dy is not None else "—", None),
    ]
    bxx, byy = Inches(0.8), Inches(5.25)
    bww, bhh = Inches(5.8), Inches(0.9)
    for i, (lbl, val, vs) in enumerate(boxes):
        x = bxx + (Inches(6.1) if i % 2 else Inches(0))
        y = byy + (Inches(1.05) if i >= 2 else Inches(0))
        rect(s3, x, y, bww, bhh, WHITE, RGBColor(0xDB, 0xE0, 0xE6))
        rect(s3, x, y, Inches(0.08), bhh, GOLD)
        tx(s3, x + Inches(0.2), y + Inches(0.12), bww - Inches(0.3), Inches(0.25), lbl, sz=12, rgb=MUTED)
        tx(s3, x + Inches(0.2), y + Inches(0.38), bww - Inches(0.3), Inches(0.35), val, sz=24, bold=True, rgb=GOLD)
        vs_text = "vs. — sector avg" if vs is None else f"vs. {vs:+.0f}% sector avg"
        tx(s3, x + Inches(2.8), y + Inches(0.45), bww - Inches(2.9), Inches(0.3), vs_text, sz=11, rgb=MUTED)
    _src_y = Inches(7.15)
    tx(s3, Inches(0.8), _src_y, Inches(12), Inches(0.3), f"Actuals: company filings via Yahoo Finance  |  Estimates: MarketScreener analyst consensus as of {datetime.now().strftime('%d %b %Y')}", sz=10, rgb=MUTED)
    if _is_bank_l:
        tx(s3, Inches(0.8), _src_y + Inches(0.2), Inches(12), Inches(0.3), "* EBITDA / EV-EBITDA not applicable for banks and financial institutions", sz=9, rgb=MUTED)
    if quality_flags:
        tx(
            s3,
            Inches(0.8),
            Inches(7.35),
            Inches(12),
            Inches(0.3),
            "Data Quality: " + "; ".join(quality_flags[:4]),
            sz=10,
            rgb=MUTED,
        )

    # ── Slide 4: Important Disclosures (dark theme) ───────────
    s4 = prs.slides.add_slide(blank)
    rect(s4, 0, 0, prs.slide_width, prs.slide_height, DARK)
    tx(s4, Inches(0), Inches(1.0), prs.slide_width, Inches(0.7), "Important Disclosures", sz=32, bold=True, rgb=LIGHT, al=PP_ALIGN.CENTER)
    rect(s4, Inches(5.5), Inches(1.7), Inches(2.3), Inches(0.06), GOLD)
    disclosures = (
        "This document is provided for informational purposes only and does not constitute an offer, "
        "solicitation, or recommendation to buy or sell any security. The information contained herein "
        "is based on sources believed to be reliable, but no representation or warranty, express or "
        "implied, is made regarding its accuracy, completeness, or timeliness.\n\n"
        "All financial data, estimates, and projections are derived from publicly available sources "
        "including MarketScreener and Yahoo Finance, supplemented by AI-generated qualitative analysis. "
        "Past performance is not indicative of future results. Investors should conduct their own due "
        "diligence and consult with a qualified financial advisor before making investment decisions.\n\n"
        "This report does not take into account the specific investment objectives, financial situation, "
        "or particular needs of any individual investor. The securities discussed may not be suitable for "
        "all investors. Investing involves risks, including the possible loss of principal."
    )
    tx(s4, Inches(1.5), Inches(2.2), Inches(10.3), Inches(3.5), disclosures, sz=12, rgb=MUTED, line_spacing=1.2)
    gen_ts = datetime.now().strftime("%d %B %Y at %H:%M UTC")
    tx(s4, Inches(1.5), Inches(5.8), Inches(10.3), Inches(0.5),
       f"Data Sources: MarketScreener, Yahoo Finance, Google Gemini  |  Generated: {gen_ts}",
       sz=10, rgb=RGBColor(0x60, 0x66, 0x70), al=PP_ALIGN.CENTER)
    tx(s4, Inches(0), Inches(6.8), prs.slide_width, Inches(0.3),
       f"\u00a9 {datetime.now().year} Earnings Research  |  All rights reserved",
       sz=10, rgb=RGBColor(0x60, 0x66, 0x70), al=PP_ALIGN.CENTER)

    path.parent.mkdir(parents=True, exist_ok=True)
    prs.save(str(path))


def _write_preview_pptx_portrait(
    payload: ReportPayload,
    path: Path,
    memo_data: dict | None,
    iv_text: str,
    watch: list[str],
    quality_flags: list[str] | None = None,
) -> None:
    """Portrait-oriented (7.5 x 13.33 in) PPTX with expanded space for qualitative writeup."""
    from pptx import Presentation
    from pptx.dml.color import RGBColor
    from pptx.enum.shapes import MSO_SHAPE
    from pptx.enum.text import PP_ALIGN
    from pptx.util import Inches, Pt

    def pn(v, bil=False):
        if v is None:
            return "—"
        try:
            x = float(v)
            if bil and abs(x) >= 1e9:
                return f"{x/1e9:.1f}B"
            if abs(x) >= 1e6:
                return f"{x:,.0f}"
            return f"{x:,.2f}" if x != int(x) else f"{int(x):,}"
        except (TypeError, ValueError):
            return str(v)

    def pp(v, signed=False):
        if v is None:
            return "—"
        try:
            x = float(v)
            return f"{x:+.1f}%" if signed else f"{x}%"
        except (TypeError, ValueError):
            return str(v)

    def rat(rec):
        if rec is None or str(rec).strip() in ("", "—"):
            return "—"
        return str(rec).strip().upper()[:20]

    def rdate(exp_date):
        if exp_date is None or str(exp_date).strip() in ("", "—"):
            return "—"
        s = str(exp_date).strip()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            try:
                y, m, d = int(s[:4]), int(s[5:7]), int(s[8:10])
                mo = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
                if 1 <= m <= 12:
                    return f"{d} {mo[m-1]} {y}"
            except Exception:
                pass
        return s

    def qlab(ps):
        raw = (ps or f"{(datetime.now().month - 1) // 3 + 1}Q{datetime.now().strftime('%y')}").replace(" ", "").strip().upper()
        qn = next((c for c in raw if c in "1234"), "1")
        _cy2p = datetime.now().strftime("%y")
        _search_yrs_p = [_cy2p, str(int(_cy2p)+1), str(int(_cy2p)-1), str(int(_cy2p)+2)]
        yr = next(("20" + y for y in _search_yrs_p if y in raw), str(datetime.now().year))
        return f"Q{qn} {yr}"

    def tx(sl, x, y, w, h, t, *, sz=12, bold=False, rgb=RGBColor(0, 0, 0), al=PP_ALIGN.LEFT, word_wrap=True, line_spacing=None):
        b = sl.shapes.add_textbox(x, y, w, h)
        tf = b.text_frame
        tf.clear()
        tf.word_wrap = word_wrap
        tf.margin_left = Pt(2)
        tf.margin_right = Pt(2)
        tf.margin_top = Pt(2)
        tf.margin_bottom = Pt(2)
        text = "" if t is None else str(t)
        lines = text.split("\n") if text else [""]

        def _style_paragraph(p):
            p.alignment = al
            if line_spacing is not None:
                try:
                    p.line_spacing = line_spacing
                except Exception:
                    pass
            try:
                p.space_after = Pt(0)
                p.space_before = Pt(0)
            except Exception:
                pass

        def _set_para(para, line_text):
            para.text = line_text
            _style_paragraph(para)
            if para.runs:
                run = para.runs[0]
                run.font.name = "Arial"
                run.font.size = Pt(sz)
                run.font.bold = bold
                run.font.color.rgb = rgb

        _set_para(tf.paragraphs[0], lines[0])
        for ln in lines[1:]:
            p2 = tf.add_paragraph()
            _set_para(p2, ln)

    def rect(sl, x, y, w, h, fill, line=None, lw=1.0):
        sh = sl.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, w, h)
        sh.fill.solid()
        sh.fill.fore_color.rgb = fill
        if line is None:
            sh.line.fill.background()
        else:
            sh.line.color.rgb = line
            sh.line.width = Pt(lw)
        return sh

    c, q, memo = payload.company, payload.quote, payload.memo_computed or {}
    header, cs, vm = (memo_data or {}).get("header") or {}, payload.consensus_summary or {}, payload.ms_valuation_multiples or {}
    _ms_price = cs.get("last_close_price") if cs else None
    _ms_ccy = cs.get("price_currency") if cs else None
    name = getattr(c, "company_name", None) or _company_attr(c, "company_name", "")
    tk = getattr(c, "ticker", None) or _company_attr(c, "ticker", "")
    sec = f"{_company_attr(c, 'sector', '')} / {_company_attr(c, 'industry', '')}".strip(" /") or "—"
    curr = (getattr(c, "currency", None) or "USD").strip()
    if not curr or curr == "USD":
        curr = _ms_ccy or curr
    # Determine report type: quarterly preview vs annual consensus
    _cp_early = memo.get("calendar_prior_quarter_released") or {}
    _cn_early = memo.get("calendar_next_quarter") or {}
    # Quarterly mode only when BOTH prior AND estimate exist (not just one sparse quarter)
    _has_quarterly_early = bool(
        (_cp_early.get("net_sales") or _cp_early.get("revenue")) and
        (_cn_early.get("net_sales") or _cn_early.get("revenue"))
    )

    # Annual data boundary
    _af_early = getattr(payload, "ms_annual_forecasts", None) or {}
    _ann_early = _af_early.get("annual", {}) if isinstance(_af_early, dict) else {}
    _ann_dates_early = _ann_early.get("announcement_dates") or []
    _ann_periods_early = _ann_early.get("periods") or []
    _first_est_period = None
    for _i, _d in enumerate(_ann_dates_early):
        if not _d or str(_d).strip() in ("", "-", "None"):
            _first_est_period = _ann_periods_early[_i] if _i < len(_ann_periods_early) else None
            break

    if _has_quarterly_early:
        pshort = (memo_data or {}).get("preview_short") or memo.get("preview_quarter_short") or f"{(datetime.now().month - 1) // 3 + 1}Q{datetime.now().strftime('%y')}"
        q_label = qlab(pshort)
        _title_suffix = f"{q_label} Earnings Preview"
    else:
        # Annual mode: label as "FY2026 Consensus Preview" not "Q1 2026 Earnings Preview"
        _fy_label = _first_est_period or f"FY{datetime.now().year}"
        q_label = _fy_label
        _title_suffix = f"{_fy_label} Consensus Preview"

    ed = header.get("expected_report_date") or {}
    ev = ed.get("display_value") if isinstance(ed, dict) else ed
    exp = rdate(ev) if ev else (memo.get("next_earnings_date") or "—")
    rr = header.get("recommendation") or {}
    rec = (rr.get("display_value") if isinstance(rr, dict) else rr) or (cs.get("consensus_rating") or "—")
    tr = header.get("average_target_price") or {}
    tgt = (tr.get("display_value") if isinstance(tr, dict) else tr) or cs.get("average_target_price")
    spr = None
    mcap = q.market_cap if q else None

    # Suppress rating/target when data quality warnings indicate unreliable MS data
    _has_dq_warnings = bool(quality_flags)
    if _has_dq_warnings and any("entity mismatch" in (f or "").lower() or "missing current" in (f or "").lower() for f in (quality_flags or [])):
        if cs and not (q and getattr(q, "target_mean_price", None)):
            # MS data unreliable and no Yahoo fallback — suppress
            rec = "—"
            tgt = None

    # Yahoo fallbacks for rating/target
    if q:
        if rec in (None, "—", ""):
            _yrec = getattr(q, "recommendation_key", None) or ""
            if _yrec and _yrec != "none":
                rec = _yrec.upper().replace("_", " ")
        if tgt is None and getattr(q, "target_mean_price", None):
            tgt = q.target_mean_price
    # Compute upside from best available price (Yahoo live → MS last_close)
    _live_price = (q.price if q else None) or _ms_price
    if tgt is not None and _live_price and _live_price > 0:
        try:
            spr = round((float(tgt) - _live_price) / _live_price * 100, 1)
        except (TypeError, ValueError):
            spr = memo.get("spread_pct") or cs.get("upside_to_average_target_pct")

    ts_val = pn(tgt)
    if curr and ts_val != "—":
        ts_val = f"{curr} {ts_val}"
    ms_val = pn(mcap, bil=True)
    if ms_val == "—" and _ms_price:
        ms_val = f"Price: {_ms_price}"  # Show price when market cap unavailable
    if curr and ms_val != "—":
        ms_val = f"{curr} {ms_val}"
    sections = (memo_data or {}).get("pptx_sections") or {}

    # Build rows/cards data from parent scope (passed through payload)
    _af = getattr(payload, "ms_annual_forecasts", None) or {}
    _ann = _af.get("annual", {}) if isinstance(_af, dict) else {}
    _eps_div = getattr(payload, "ms_eps_dividend_forecasts", None) or {}
    _ann_periods = _ann.get("periods") or _eps_div.get("periods") or []
    _ann_fy_prior, _ann_fy_est = -1, -1
    _ann_dates = _ann.get("announcement_dates") or []
    for _ai, _ap in enumerate(_ann_periods):
        _has_ann_date = (_ai < len(_ann_dates) and _ann_dates[_ai] and str(_ann_dates[_ai]).strip() not in ("", "-", "None"))
        if _has_ann_date:
            _ann_fy_prior = _ai
        elif _ann_fy_est == -1:
            _ann_fy_est = _ai
    if _ann_fy_prior == -1 and _ann_fy_est == -1:
        _curr_yr = datetime.now().year
        for _ai, _ap in enumerate(_ann_periods):
            _yr = "".join(c2 for c2 in str(_ap) if c2.isdigit())[:4]
            try:
                _yr_int = int(_yr)
            except ValueError:
                continue
            if _yr_int < _curr_yr:
                _ann_fy_prior = _ai
            if _yr_int >= _curr_yr and _ann_fy_est == -1:
                _ann_fy_est = _ai
    if _ann_fy_est == -1 and len(_ann_periods) >= 2:
        _ann_fy_prior = len(_ann_periods) - 2
        _ann_fy_est = len(_ann_periods) - 1

    _vm = getattr(payload, "ms_valuation_multiples", None) or {}

    def _ann_val(key, idx):
        arr = _ann.get(key) or []
        if 0 <= idx < len(arr) and arr[idx] is not None:
            return arr[idx]
        if key == "eps":
            # Fallback 1: /valuation-dividend/ page
            eps_arr = _eps_div.get("eps") or []
            if 0 <= idx < len(eps_arr) and eps_arr[idx] is not None:
                return eps_arr[idx]
            # Fallback 2: /valuation/ page (has EPS in its own row)
            vm_eps = _vm.get("eps") or []
            if 0 <= idx < len(vm_eps) and vm_eps[idx] is not None:
                return vm_eps[idx]
        return None

    def _yoy_pct(prior, est):
        if prior and est and isinstance(prior, (int, float)) and isinstance(est, (int, float)) and prior != 0:
            return round((est - prior) / abs(prior) * 100, 1)
        return None

    cp = memo.get("calendar_prior_quarter_released") or {}
    cn = memo.get("calendar_next_quarter") or {}
    _has_quarterly = bool((cp.get("net_sales") or cp.get("revenue")) and (cn.get("net_sales") or cn.get("revenue")))
    _cM = f"({curr}M)" if curr else "(M)"
    _cU = f"({curr})" if curr else ""
    if _has_quarterly:
        rows = [
            (f"Revenue {_cM}", cp.get("net_sales"), cn.get("net_sales") or cn.get("revenue"), memo.get("yoy_revenue_pct_table")),
            (f"EBITDA {_cM}", cp.get("ebitda"), cn.get("ebitda"), memo.get("yoy_ebitda_pct_table")),
            (f"Net Income {_cM}", cp.get("net_income"), cn.get("net_income"), memo.get("yoy_ni_pct_table")),
            (f"EPS {_cU}", cp.get("eps"), cn.get("eps"), memo.get("yoy_eps_pct_table")),
        ]
    else:
        _rev_p, _rev_e = _ann_val("net_sales", _ann_fy_prior), _ann_val("net_sales", _ann_fy_est)
        _ni_p, _ni_e = _ann_val("net_income", _ann_fy_prior), _ann_val("net_income", _ann_fy_est)
        _ebitda_p, _ebitda_e = _ann_val("ebitda", _ann_fy_prior), _ann_val("ebitda", _ann_fy_est)
        _ebit_p, _ebit_e = _ann_val("ebit", _ann_fy_prior), _ann_val("ebit", _ann_fy_est)
        _eps_p, _eps_e = _ann_val("eps", _ann_fy_prior), _ann_val("eps", _ann_fy_est)
        rows = [
            (f"Revenue {_cM}", _rev_p, _rev_e, _yoy_pct(_rev_p, _rev_e)),
            (f"EBITDA {_cM}", _ebitda_p or _ebit_p, _ebitda_e or _ebit_e, _yoy_pct(_ebitda_p or _ebit_p, _ebitda_e or _ebit_e)),
            (f"Net Income {_cM}", _ni_p, _ni_e, _yoy_pct(_ni_p, _ni_e)),
            (f"EPS {_cU}", _eps_p, _eps_e, _yoy_pct(_eps_p, _eps_e)),
        ]
    # Yahoo fallback
    if all(r[1] is None and r[2] is None for r in rows):
        ya = sorted(getattr(payload, "annual_actuals", None) or [], key=lambda p: p.period_label, reverse=True)
        if len(ya) >= 2:
            _ya_cur, _ya_pri = ya[0], ya[1]
            def _toM(v):
                """Scale raw Yahoo values to millions. Yahoo returns full units (e.g. 5e8 for 500M)."""
                if v is None: return None
                try:
                    x = float(v)
                    return round(x / 1e6, 1) if abs(x) >= 1e6 else x
                except (TypeError, ValueError):
                    return v
            _is_bank = bool(_company_attr(c, "is_bank", False))
            _ebitda_row_p = (f"EBITDA {_cM}", _toM(_ya_pri.ebitda), _toM(_ya_cur.ebitda), _yoy_pct(_ya_pri.ebitda, _ya_cur.ebitda))
            if _is_bank and _ya_pri.ebitda is None:
                _ebitda_row_p = (f"EBIT {_cM}", _toM(getattr(_ya_pri, "ebit", None)), _toM(getattr(_ya_cur, "ebit", None)), _yoy_pct(getattr(_ya_pri, "ebit", None), getattr(_ya_cur, "ebit", None)))
            rows = [
                (f"Revenue {_cM}", _toM(_ya_pri.revenue), _toM(_ya_cur.revenue), _yoy_pct(_ya_pri.revenue, _ya_cur.revenue)),
                _ebitda_row_p,
                (f"Net Income {_cM}", _toM(_ya_pri.net_income), _toM(_ya_cur.net_income), _yoy_pct(_ya_pri.net_income, _ya_cur.net_income)),
                (f"EPS {_cU}", _ya_pri.eps, _ya_cur.eps, _yoy_pct(_ya_pri.eps, _ya_cur.eps)),
            ]

    rv = cn.get("net_sales") or cn.get("revenue") or _ann_val("net_sales", _ann_fy_est)
    ev_eps = cn.get("eps") or _ann_val("eps", _ann_fy_est)
    # If table has data but cards don't, pull from the table rows (estimate or actual)
    if not rv and rows:
        rv = rows[0][2] or rows[0][1]  # Revenue: estimate first, then actual
    if not ev_eps and len(rows) > 3:
        ev_eps = rows[3][2] or rows[3][1]  # EPS: estimate first, then actual

    pv = vm.get("periods") or []
    _cy = str(datetime.now().year)
    _ny = str(datetime.now().year + 1)
    i26 = next((i for i, p in enumerate(pv) if _cy in str(p) or _ny in str(p)), len(pv) - 1 if pv else -1)
    def pick(arr):
        if not arr:
            return None
        if 0 <= i26 < len(arr) and arr[i26] is not None:
            return arr[i26]
        for v in reversed(arr):
            if v is not None:
                return v
        return None
    pe = pick(vm.get("pe") or [])
    evv = pick(vm.get("ev_ebitda") or []) or pick(vm.get("ev_ebit") or [])
    pb = pick(vm.get("pbr") or [])
    dy = pick(vm.get("yield_pct") or [])

    # Yahoo fallback for valuation multiples
    if q:
        if pe is None:
            pe = getattr(q, "forward_pe", None) or getattr(q, "trailing_pe", None)
        if evv is None:
            evv = getattr(q, "ev_to_ebitda", None)
        if pb is None:
            pb = getattr(q, "price_to_book", None)
        if dy is None:
            _dy_raw = getattr(q, "dividend_yield", None)
            if _dy_raw is not None:
                dy = round(_dy_raw * 100, 2) if _dy_raw < 1 else _dy_raw  # Yahoo returns decimal

    DARK = RGBColor(0x0D, 0x11, 0x17)
    GOLD = RGBColor(0xC9, 0xA2, 0x27)
    LIGHT = RGBColor(0xE6, 0xED, 0xF3)
    MUTED = RGBColor(0x8B, 0x94, 0x9E)
    GREEN = RGBColor(0x3F, 0xB9, 0x50)
    WHITE = RGBColor(0xFF, 0xFF, 0xFF)
    BLACK = RGBColor(0x1F, 0x23, 0x28)
    W = Inches(7.5)

    prs = Presentation()
    prs.slide_width = W
    prs.slide_height = Inches(13.33)
    blank = prs.slide_layouts[6]

    # ── Slide 1: Cover (dark, portrait) ───────────────────────
    s1 = prs.slides.add_slide(blank)
    rect(s1, 0, 0, W, prs.slide_height, DARK)
    tx(s1, Inches(0.6), Inches(0.5), Inches(6), Inches(0.3), "EARNINGS PREVIEW NOTE", sz=10, bold=True, rgb=GOLD)
    rect(s1, Inches(0.6), Inches(0.85), Inches(1.8), Inches(0.04), GOLD)
    tx(s1, Inches(0.6), Inches(1.3), Inches(6.3), Inches(1.2), name or "—", sz=36, bold=True, rgb=LIGHT)
    tx(s1, Inches(0.6), Inches(2.6), Inches(6.3), Inches(0.5), _title_suffix, sz=18, rgb=LIGHT)

    my = 3.5
    for i, (lb, vl) in enumerate([("Sector:", sec), ("Ticker:", tk), ("Market Cap:", ms_val), ("Report Date:", exp)]):
        tx(s1, Inches(0.6), Inches(my + i * 0.4), Inches(1.8), Inches(0.3), lb, sz=11, rgb=MUTED)
        tx(s1, Inches(2.1), Inches(my + i * 0.4), Inches(5), Inches(0.3), str(vl), sz=11, bold=True, rgb=LIGHT)

    # Rating / Target / Upside — vertical cards
    by = Inches(5.5)
    bw = Inches(6.3)
    for j, (lb, vl, col) in enumerate([("RATING", rat(rec), LIGHT), ("TARGET PRICE", ts_val, LIGHT), ("UPSIDE", pp(spr, True) if spr is not None else "—", GREEN)]):
        y = by + Inches(j * 1.05)
        rect(s1, Inches(0.6), y, bw, Inches(0.9), RGBColor(0x10, 0x17, 0x22), GOLD)
        tx(s1, Inches(0.85), y + Inches(0.12), Inches(2), Inches(0.25), lb, sz=9, bold=True, rgb=MUTED)
        tx(s1, Inches(0.85), y + Inches(0.38), bw - Inches(0.5), Inches(0.4), str(vl), sz=22, bold=True, rgb=col)

    tx(s1, Inches(0), Inches(12.9), W, Inches(0.3), "CONFIDENTIAL | For Institutional Clients Only", sz=9, rgb=MUTED, al=PP_ALIGN.CENTER)

    # ── Slide 2: Executive Summary (white, portrait — EXPANDED thesis) ──
    s2 = prs.slides.add_slide(blank)
    rect(s2, 0, 0, W, prs.slide_height, WHITE)
    tx(s2, Inches(0.6), Inches(0.4), Inches(6.3), Inches(0.3), f"{name} | {q_label}", sz=12, bold=True, rgb=BLACK)
    tx(s2, Inches(0.6), Inches(0.85), Inches(6), Inches(0.5), "Executive Summary", sz=26, bold=True, rgb=BLACK)
    rect(s2, Inches(0.6), Inches(1.35), Inches(2), Inches(0.06), GOLD)

    # Investment Thesis — EXPANDED (6" tall block)
    tx(s2, Inches(0.6), Inches(1.6), Inches(1.5), Inches(0.3), "Investment Thesis", sz=12, bold=True, rgb=MUTED)
    rect(s2, Inches(0.6), Inches(1.95), Inches(6.3), Inches(5.5), RGBColor(0xFA, 0xF8, 0xF3), RGBColor(0xDB, 0xE0, 0xE6))
    rect(s2, Inches(0.6), Inches(1.95), Inches(0.06), Inches(5.5), GOLD)
    tx(s2, Inches(0.78), Inches(2.1), Inches(6.0), Inches(5.2), iv_text or "—", sz=12, rgb=BLACK, line_spacing=1.15)

    # Compute EBITDA margin for key expectations card
    _em_rev = cn.get("net_sales") or _ann_val("net_sales", _ann_fy_est)
    _em_ebitda = cn.get("ebitda") or _ann_val("ebitda", _ann_fy_est)
    _is_bank_card = bool(_company_attr(c, "is_bank", False))
    if _em_rev and _em_ebitda and _em_rev != 0:
        _em_display = pp(round(_em_ebitda / _em_rev * 100, 1))
    else:
        _em_display = "N/A*" if _is_bank_card else "—"

    # Key Expectations — 3 cards
    tx(s2, Inches(0.6), Inches(7.65), Inches(4), Inches(0.3), "Key Expectations", sz=14, bold=True, rgb=BLACK)
    cw2 = Inches(2.0)
    cg = Inches(0.15)
    for i, (lb, va, chg) in enumerate([
        ("Revenue", pn(rv), pp(rows[0][3] if rows and rows[0][3] is not None else memo.get("yoy_revenue_pct_table") or memo.get("qoq_revenue_pct"), True) if rv else "—"),
        ("EPS", pn(ev_eps), "—"),
        ("EBITDA Margin", _em_display, "—"),
    ][:3]):
        x = Inches(0.6) + i * (cw2 + cg)
        rect(s2, x, Inches(8.0), cw2, Inches(0.85), WHITE, RGBColor(0xDB, 0xE0, 0xE6))
        tx(s2, x + Inches(0.15), Inches(8.08), cw2 - Inches(0.3), Inches(0.2), lb, sz=9, rgb=MUTED)
        tx(s2, x + Inches(0.15), Inches(8.3), cw2 - Inches(0.3), Inches(0.3), va if isinstance(va, str) else pn(va), sz=18, bold=True, rgb=BLACK)
        if isinstance(chg, str) and chg != "—":
            tx(s2, x + Inches(0.15), Inches(8.62), cw2 - Inches(0.3), Inches(0.2), chg, sz=10, bold=True, rgb=GREEN)

    # What to Watch
    wl = (sections.get("what_to_watch") if isinstance(sections, dict) else None) or watch or ["Guidance", "Segment performance", "Macro / FX", "Capital allocation"]
    tx(s2, Inches(0.6), Inches(9.1), Inches(4), Inches(0.3), "What to Watch", sz=14, bold=True, rgb=BLACK)
    for i, item in enumerate(wl[:4]):
        y = Inches(9.45) + Inches(i * 0.38)
        tx(s2, Inches(0.6), y, Inches(0.3), Inches(0.3), str(i + 1), sz=11, bold=True, rgb=GOLD)
        tx(s2, Inches(0.95), y, Inches(5.9), Inches(0.3), item, sz=11, rgb=BLACK)

    # Catalysts & Risks
    tx(s2, Inches(0.6), Inches(11.1), Inches(4), Inches(0.3), "Catalysts & Risks", sz=14, bold=True, rgb=BLACK)
    cbw = Inches(3.05)
    c_list = (sections.get("catalysts") if isinstance(sections, dict) else None) or []
    catalysts = [str(x).strip() for x in c_list if str(x).strip()][:3] if isinstance(c_list, list) else []
    if not catalysts:
        catalysts = ["Product / volume upside", "Cost or mix tailwind", "Policy / regulatory development"]
    r_list = (sections.get("risks") if isinstance(sections, dict) else None) or []
    risks = [str(x).strip() for x in r_list if str(x).strip()][:3] if isinstance(r_list, list) else []
    if not risks:
        risks = ["Macro / demand downside", "Pricing / competition pressure", "Execution or guidance risk"]

    rect(s2, Inches(0.6), Inches(11.45), cbw, Inches(0.9), RGBColor(0xF0, 0xF9, 0xF0), RGBColor(0xDB, 0xE0, 0xE6))
    rect(s2, Inches(0.6), Inches(11.45), Inches(0.06), Inches(0.9), RGBColor(0x1A, 0x7F, 0x37))
    tx(s2, Inches(0.75), Inches(11.48), cbw - Inches(0.2), Inches(0.18), "CATALYSTS", sz=8, bold=True, rgb=RGBColor(0x1A, 0x7F, 0x37))
    tx(s2, Inches(0.75), Inches(11.65), cbw - Inches(0.2), Inches(0.65), "\u2191 " + "\n\u2191 ".join(catalysts), sz=8, rgb=BLACK, word_wrap=False, line_spacing=0.9)

    rx = Inches(3.85)
    rect(s2, rx, Inches(11.45), cbw, Inches(0.9), RGBColor(0xFE, 0xF0, 0xF0), RGBColor(0xDB, 0xE0, 0xE6))
    rect(s2, rx, Inches(11.45), Inches(0.06), Inches(0.9), RGBColor(0xCF, 0x22, 0x22))
    tx(s2, rx + Inches(0.15), Inches(11.48), cbw - Inches(0.2), Inches(0.18), "KEY RISKS", sz=8, bold=True, rgb=RGBColor(0xCF, 0x22, 0x22))
    tx(s2, rx + Inches(0.15), Inches(11.65), cbw - Inches(0.2), Inches(0.65), "\u2193 " + "\n\u2193 ".join(risks), sz=8, rgb=BLACK, word_wrap=False, line_spacing=0.9)

    # ── Slide 3: Financial Snapshot (white, portrait) ─────────
    s3 = prs.slides.add_slide(blank)
    rect(s3, 0, 0, W, prs.slide_height, WHITE)
    tx(s3, Inches(0.6), Inches(0.5), Inches(6), Inches(0.5), "Financial Snapshot", sz=26, bold=True, rgb=BLACK)
    rect(s3, Inches(0.6), Inches(1.0), Inches(2), Inches(0.06), GOLD)

    if _has_quarterly:
        hdrs = ["Metric", "Q prior (A)", "Q next (E)", "YoY %"]
    elif _first_est_period:
        _last_act = None
        for _i2, _d2 in enumerate(_ann_dates_early):
            if _d2 and str(_d2).strip() not in ("", "-", "None"):
                _last_act = _ann_periods_early[_i2] if _i2 < len(_ann_periods_early) else None
        hdrs = ["Metric", f"{_last_act or 'Prior'} (A)", f"{_first_est_period} (E)", "YoY %"]
    else:
        hdrs = ["Metric", "Prior (A)", "Current (E)", "YoY %"]
    cws = [Inches(2.0), Inches(1.5), Inches(1.5), Inches(1.3)]
    tbx = Inches(0.6)
    tby = Inches(1.3)
    rh = Inches(0.42)
    x = tbx
    for j, h in enumerate(hdrs):
        rect(s3, x, tby, cws[j], rh, BLACK, RGBColor(0xDB, 0xE0, 0xE6))
        tx(s3, x + Inches(0.1), tby + Inches(0.08), cws[j] - Inches(0.2), rh, h, sz=10, bold=True, rgb=WHITE)
        x += cws[j]
    rows = [(lb, pa, ce, yy) for lb, pa, ce, yy in rows if pa is not None or ce is not None]
    for i, (lb, pa, ce, yy) in enumerate(rows):
        y = tby + rh * (i + 1)
        x = tbx
        vals = [lb, pn(pa), pn(ce), pp(yy, True) if yy is not None else "—"]
        for j, v in enumerate(vals):
            fl = RGBColor(0xFA, 0xF8, 0xF3) if j == 2 else WHITE
            rect(s3, x, y, cws[j], rh, fl, RGBColor(0xDB, 0xE0, 0xE6))
            tx(s3, x + Inches(0.1), y + Inches(0.08), cws[j] - Inches(0.2), rh, str(v), sz=10, bold=(j == 0), rgb=BLACK)
            x += cws[j]

    # Valuation Summary
    tx(s3, Inches(0.6), Inches(4.2), Inches(6), Inches(0.4), "Valuation Summary", sz=22, bold=True, rgb=BLACK)
    rect(s3, Inches(0.6), Inches(4.6), Inches(2), Inches(0.05), GOLD)
    _is_bank_p = bool(_company_attr(c, "is_bank", False))
    boxes = [
        ("P/E (FY26E)", f"{pe:.1f}x" if pe is not None else "—"),
        ("EV/EBITDA", f"{evv:.1f}x" if evv is not None else ("N/A*" if _is_bank_p else "—")),
        ("P/B", f"{pb:.1f}x" if pb is not None else "—"),
        ("Div. Yield", f"{dy:.1f}%" if dy is not None else "—"),
    ]
    vbw = Inches(3.05)
    for i, (lbl, val) in enumerate(boxes):
        x = Inches(0.6) + (Inches(3.2) if i % 2 else 0)
        y = Inches(4.85) + (Inches(1.1) if i >= 2 else 0)
        rect(s3, x, y, vbw, Inches(0.95), WHITE, RGBColor(0xDB, 0xE0, 0xE6))
        rect(s3, x, y, Inches(0.06), Inches(0.95), GOLD)
        tx(s3, x + Inches(0.18), y + Inches(0.12), vbw - Inches(0.3), Inches(0.2), lbl, sz=10, rgb=MUTED)
        tx(s3, x + Inches(0.18), y + Inches(0.4), vbw - Inches(0.3), Inches(0.35), val, sz=22, bold=True, rgb=GOLD)

    tx(s3, Inches(0.6), Inches(7.2), Inches(6), Inches(0.3), f"Actuals: company filings via Yahoo Finance  |  Estimates: MarketScreener analyst consensus as of {datetime.now().strftime('%d %b %Y')}", sz=9, rgb=MUTED)
    if _is_bank_p:
        tx(s3, Inches(0.6), Inches(7.4), Inches(6), Inches(0.3), "* EBITDA / EV-EBITDA not applicable for banks and financial institutions", sz=8, rgb=MUTED)
    if quality_flags:
        tx(s3, Inches(0.6), Inches(7.45), Inches(6), Inches(0.3), "Data Quality: " + "; ".join(quality_flags[:4]), sz=9, rgb=MUTED)

    # ── Slide 4: Important Disclosures (dark, portrait) ───────
    s4 = prs.slides.add_slide(blank)
    rect(s4, 0, 0, W, prs.slide_height, DARK)
    tx(s4, Inches(0), Inches(1.0), W, Inches(0.6), "Important Disclosures", sz=28, bold=True, rgb=LIGHT, al=PP_ALIGN.CENTER)
    rect(s4, Inches(2.7), Inches(1.6), Inches(2.1), Inches(0.05), GOLD)
    disclosures = (
        "This document is provided for informational purposes only and does not constitute an offer, "
        "solicitation, or recommendation to buy or sell any security. The information contained herein "
        "is based on sources believed to be reliable, but no representation or warranty, express or "
        "implied, is made regarding its accuracy, completeness, or timeliness.\n\n"
        "All financial data, estimates, and projections are derived from publicly available sources "
        "including MarketScreener and Yahoo Finance, supplemented by AI-generated qualitative analysis. "
        "Past performance is not indicative of future results. Investors should conduct their own due "
        "diligence and consult with a qualified financial advisor before making investment decisions.\n\n"
        "This report does not take into account the specific investment objectives, financial situation, "
        "or particular needs of any individual investor. The securities discussed may not be suitable for "
        "all investors. Investing involves risks, including the possible loss of principal."
    )
    tx(s4, Inches(0.8), Inches(2.0), Inches(5.9), Inches(5.0), disclosures, sz=11, rgb=MUTED, line_spacing=1.3)
    gen_ts = datetime.now().strftime("%d %B %Y at %H:%M UTC")
    tx(s4, Inches(0.8), Inches(7.5), Inches(5.9), Inches(0.5),
       f"Data Sources: MarketScreener, Yahoo Finance, Google Gemini\nGenerated: {gen_ts}",
       sz=9, rgb=RGBColor(0x60, 0x66, 0x70), al=PP_ALIGN.CENTER)
    tx(s4, Inches(0), Inches(12.8), W, Inches(0.3),
       f"\u00a9 {datetime.now().year} Earnings Research  |  All rights reserved",
       sz=9, rgb=RGBColor(0x60, 0x66, 0x70), al=PP_ALIGN.CENTER)

    path.parent.mkdir(parents=True, exist_ok=True)
    prs.save(str(path))


def run(payload: ReportPayload, memo_data: dict | None = None, qa_audit: dict | None = None, data_warnings: list[str] | None = None) -> StepResult:
    with StepTimer() as t:
        try:
            iv_style = _iv_fallback_style()
            ticker = payload.company.ticker
            out_dir = report_output_dir()
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            suffix = f"{ticker}_{ts}_earnings_preview.pptx"
            out_path = out_dir / suffix
            iv_text, watch = _iv_text_and_watch(payload, memo_data, iv_style)
            quality_flags: list[str] = []
            if qa_audit:
                if not qa_audit.get("payload_entity_match", True):
                    quality_flags.append("MS entity mismatch suppressed")
                if qa_audit.get("ms_section_suppressed_due_to_missing_current_data"):
                    quality_flags.append("MS suppressed: missing current data")
                if qa_audit.get("ms_section_suppressed_due_to_entity_mismatch"):
                    quality_flags.append("MS suppressed: entity mismatch")
                if qa_audit.get("ms_section_suppressed_due_to_contamination"):
                    quality_flags.append("MS suppressed: contamination")
                if qa_audit.get("reused_default_payload_detected"):
                    quality_flags.append("Default payload reused")
            # Add automated data validation warnings
            if data_warnings:
                quality_flags.extend(data_warnings)
            _write_preview_pptx_portrait(payload, out_path, memo_data, iv_text, watch, quality_flags or None)
            return StepResult(step_name=STEP, status=Status.SUCCESS, source="pptx", message=f"Report saved → {out_path}", data=str(out_path), elapsed_seconds=t.elapsed)
        except Exception as exc:
            return StepResult(step_name=STEP, status=Status.FAILED, source="pptx", message="Report generation failed", error_detail=str(exc), elapsed_seconds=t.elapsed)
