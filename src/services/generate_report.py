"""
Service: generate_report

Institutional earnings preview memo. Polished layout; no debug/API text in output.
Structure: Title block → Summary strip → Investment View (2 paragraphs) → Key Preview
→ Operating Metrics → Street Snapshot → Recent Execution → What Matters → Appendices A, B, D.
Appendix C only if meaningful; Appendix E omitted from client-facing note.
"""

from __future__ import annotations
import json
import re
from datetime import datetime
from pathlib import Path

from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.opc.constants import RELATIONSHIP_TYPE
from docx.shared import Inches, Pt
from docx.dml.color import RGBColor

from src.config import cfg, root, report_output_dir
from src.models.report_payload import ReportPayload
from src.models.step_result import Status, StepResult, StepTimer
from src.services.report_styling import (
    ACCENT, BODY, GRAY, WHITE, HEADER_FILL,
    SPACE_NONE, SPACE_TINY, SPACE_SMALL, SPACE_MED,
    TITLE_PT, SECTION_PT, BODY_PT, SMALL_PT, SOURCE_PT,
    apply_section_margins, set_cell_shading, style_table_header_row, set_compact_row_height,
)

STEP = "generate_report"


def _run(paragraph, text: str, bold: bool = False, size_pt: float = 8.5, color=None):
    r = paragraph.add_run(text)
    r.font.name = "Arial"
    r.font.size = Pt(size_pt)
    r.font.color.rgb = color or BODY
    r.font.bold = bold
    return r


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


def _publisher_display_name(source: str) -> str:
    """Display name for publisher; no provider-specific logic (config or title-case)."""
    s = (source or "").strip().lower()
    # Optional config map for display names; else title-case
    try:
        names = cfg().get("news", {}).get("recent_context_publisher_display") or {}
        if isinstance(names, dict) and s in names:
            return str(names[s])
    except Exception:
        pass
    return (source or "").strip().title() or "—"


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


def _sector_operating_kpis_and_what_matters(company) -> tuple[list[str], list[str], str]:
    """
    Return (operating_metrics_kpis[4], what_matters_bullets[5], fallback_para2_snippet).
    fallback_para2 is publishable analyst prose only (no "Focus on...", "Do not use..." instructions).
    """
    sector = (getattr(company, "sector", None) or "").strip().lower()
    industry = (getattr(company, "industry", None) or "").strip().lower()
    ind = industry or sector
    is_bank = getattr(company, "is_bank", False)

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

    if "telecom" in ind or "communication" in sector:
        kpis = ["Subscribers", "ARPU", "Churn", "Capex intensity"]
        matters = ["Subscriber additions", "ARPU trend", "Churn", "Capex intensity", "India wireless competition" if "india" in (getattr(company, "country", "") or "").lower() else "Wireless competition", "Enterprise / data centre contribution"]
        p2 = "For telecoms, subscriber trends, ARPU, churn, and capex intensity are central; enterprise and data centre contribution matter where relevant."
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

    if "chem" in ind or "material" in ind:
        kpis = ["Volume", "Realized price", "Utilization", "Feedstock spread"]
        matters = ["Volume and realized price", "Utilization", "Feedstock spread", "Guidance", "Key metrics"]
        p2 = "Volume, realized price, utilization, and feedstock spread are the main levers; guidance and key metrics drive the story."
        return kpis, matters[:5], p2

    # Default: labeled rows for manual entry
    kpis = ["Key metric 1", "Key metric 2", "Key metric 3", "Key metric 4"]
    matters = ["Headline vs consensus", "Margin and pricing", "Guidance", "Key metrics"]
    p2 = "This quarter, sector operating metrics and headline results versus consensus matter most; earnings quality—whether a beat or miss is recurring or one-off—and guidance drive the narrative."
    return kpis, matters[:5], p2


def _add_recent_context_section(doc: Document, payload: ReportPayload) -> tuple[int, list]:
    """
    Add 'Recent Context' on page 1 only when we have valid articles (headline + publisher + URL).
    Render up to 5 bullets: headline (clickable) — Publisher, Date.
    Returns (render_count, list of displayed article headlines) for QA.
    """
    items = getattr(payload, "news_items", None) or []
    valid = [a for a in items if _is_valid_recent_context_article(a)][:5]
    if not valid:
        return 0, []

    p_sec = doc.add_paragraph()
    p_sec.paragraph_format.space_before = SPACE_SMALL
    p_sec.paragraph_format.space_after = SPACE_TINY
    _run(p_sec, "Recent Context", bold=True, size_pt=SECTION_PT, color=ACCENT)
    displayed_headlines: list[str] = []
    for art in valid:
        headline = (getattr(art, "headline", None) or "").strip()
        if not headline:
            continue
        headline_80 = headline[:80] + ("…" if len(headline) > 80 else "")
        dt = getattr(art, "published_at", None)
        date_str = ""
        if dt and hasattr(dt, "strftime"):
            date_str = dt.strftime("%Y-%m-%d")
        source = (getattr(art, "source", None) or "").strip()
        pub = _publisher_display_name(source)
        url = (getattr(art, "url", None) or "").strip()
        line = doc.add_paragraph(style="List Bullet")
        line.paragraph_format.space_before = SPACE_NONE
        line.paragraph_format.space_after = SPACE_TINY
        # Clickable headline; then " — Publisher, Date" as plain or link
        _add_hyperlink(line, headline_80, url, size_pt=BODY_PT, color=GRAY)
        _run(line, f" — {pub}, {date_str}", size_pt=BODY_PT, color=GRAY)
        displayed_headlines.append(headline)
    return len(valid), displayed_headlines


def _add_hyperlink(paragraph, text: str, url: str, size_pt: float = SOURCE_PT, color=GRAY):
    """Append a hyperlink run to the paragraph (smaller font, gray, clickable)."""
    if not url or not url.startswith("http"):
        _run(paragraph, text, size_pt=size_pt, color=color)
        return
    part = paragraph.part
    r_id = part.relate_to(url, RELATIONSHIP_TYPE.HYPERLINK, is_external=True)
    hyperlink = OxmlElement("w:hyperlink")
    hyperlink.set(qn("r:id"), r_id)
    r = OxmlElement("w:r")
    rPr = OxmlElement("w:rPr")
    fonts = OxmlElement("w:rFonts")
    fonts.set(qn("w:ascii"), "Arial")
    fonts.set(qn("w:hAnsi"), "Arial")
    rPr.append(fonts)
    c = OxmlElement("w:color")
    c.set(qn("w:val"), "5D6D7E")
    rPr.append(c)
    sz = OxmlElement("w:sz")
    sz.set(qn("w:val"), str(int(size_pt * 2)))
    rPr.append(sz)
    szCs = OxmlElement("w:szCs")
    szCs.set(qn("w:val"), str(int(size_pt * 2)))
    rPr.append(szCs)
    r.append(rPr)
    t = OxmlElement("w:t")
    t.set(qn("xml:space"), "preserve")
    t.text = text
    r.append(t)
    hyperlink.append(r)
    paragraph._p.append(hyperlink)


def _split_sentences(paragraph_text: str) -> list[str]:
    """Split into sentences (simple: on . ! ? followed by space or end)."""
    if not paragraph_text or not paragraph_text.strip():
        return []
    return re.split(r"(?<=[.!?])\s+", paragraph_text.strip())


def _build(payload: ReportPayload, path: Path, memo_data: dict | None = None, qa_audit: dict | None = None) -> None:
    doc = Document()
    c = payload.company
    memo = payload.memo_computed or {}
    # Section-level lineage: do not use MS-derived data when entity/ticker mismatch or contamination (better blank than wrong)
    payload_entity_match = getattr(payload, "payload_entity_match", True)
    payload_source_ticker = (getattr(payload, "payload_source_ticker", "") or "").strip()
    current_ticker = (getattr(c, "ticker", "") or "").strip()
    cross_contamination = getattr(payload, "cross_company_contamination_detected", False)
    use_ms_for_render = payload_entity_match and (payload_source_ticker == current_ticker) and not cross_contamination
    cs = (payload.consensus_summary or {}) if use_ms_for_render else {}
    q = payload.quote
    curr = (c.currency or "SAR").strip()
    # When memo_data is present, use validated header/display values only (memo_data already blanks MS when suppressed)
    if memo_data:
        header = memo_data.get("header") or {}
        preview_short = memo_data.get("preview_short") or memo.get("preview_quarter_short") or "1Q26"
        exp_date = _field_display(header.get("expected_report_date"), "—")
        mean_cons = _field_display(header.get("recommendation"), "—")
        n_analysts = _field_display(header.get("analyst_count"), "—")
        target = _field_display(header.get("average_target_price"))
        spread = _field_display(header.get("upside_pct"))
        price_val = _field_display(header.get("consensus_page_price")) or _field_display(header.get("quote_price"))
        price_yahoo = price_val if isinstance(price_val, (int, float)) else None
        price_ms = price_val if isinstance(price_val, (int, float)) else None
        price = price_val
        has_yahoo = price_yahoo is not None
        has_ms = price_ms is not None
        low = None
        high = None
    else:
        price_yahoo = (q and getattr(q, "price", None)) or None
        price_ms = cs.get("last_close_price")
        price = price_yahoo if price_yahoo is not None else price_ms
        preview_short = memo.get("preview_quarter_short") or "1Q26"
        exp_date = memo.get("next_earnings_date") or "—"
        mean_cons = cs.get("consensus_rating") or "—"
        n_analysts = cs.get("analyst_count") or "—"
        target = cs.get("average_target_price")
        low = cs.get("low_target_price")
        high = cs.get("high_target_price")
        spread = memo.get("spread_pct") or cs.get("upside_to_average_target_pct")
        has_yahoo = price_yahoo is not None and isinstance(price_yahoo, (int, float))
        has_ms = price_ms is not None and isinstance(price_ms, (int, float))

    for section in doc.sections:
        apply_section_margins(section)

    style = doc.styles["Normal"]
    style.font.name = "Arial"
    style.font.size = Pt(BODY_PT)
    style.font.color.rgb = BODY
    style.paragraph_format.space_after = SPACE_TINY
    style.paragraph_format.space_before = SPACE_NONE

    curr = (c.currency or "SAR").strip()

    # ─── 1. Title block ─────────────────────────────────────────────────
    p = doc.add_paragraph()
    p.paragraph_format.space_after = SPACE_TINY
    _run(p, c.company_name, bold=True, size_pt=TITLE_PT, color=ACCENT)
    _run(p, f"  ({c.ticker})  ·  ", size_pt=TITLE_PT, color=BODY)
    _run(p, f"Earnings Preview — {preview_short}", size_pt=TITLE_PT, color=BODY)
    p = doc.add_paragraph()
    p.paragraph_format.space_after = SPACE_SMALL
    _run(p, f"Expected report date: {exp_date}", size_pt=SMALL_PT, color=GRAY)

    # ─── 2. Top summary strip (compact dashboard) ────────────────────────
    strip_parts = [
        exp_date if exp_date != "—" else "—",
        mean_cons if mean_cons != "—" else "—",
        f"{n_analysts} analysts" if n_analysts != "—" else "—",
        f"{curr} {float(price_yahoo):,.2f}" if has_yahoo else (f"{curr} {float(price_ms):,.2f}" if has_ms else "—"),
        f"{curr} {target:,.2f}" if target is not None and isinstance(target, (int, float)) else "—",
        _fmt_pct(spread, signed=True) if spread is not None else "—",
    ]
    p = doc.add_paragraph()
    p.paragraph_format.space_before = SPACE_NONE
    p.paragraph_format.space_after = SPACE_SMALL
    _run(p, "  |  ".join(str(x) for x in strip_parts), size_pt=SMALL_PT, color=BODY)

    # MarketScreener: explicit warning when data suppressed (entity mismatch, contamination, or redirect)
    ms_avail = getattr(payload, "marketscreener_availability", "") or ""
    if not use_ms_for_render or ms_avail == "source_redirect":
        p = doc.add_paragraph()
        p.paragraph_format.space_after = SPACE_TINY
        if cross_contamination or getattr(payload, "reused_default_payload_detected", False):
            _run(p, "MarketScreener data suppressed (cross-company contamination or reused payload detected). Header, Key Preview, and Appendices A–D show blanks. Better blank than wrong.", size_pt=SMALL_PT, color=GRAY)
        elif not payload_entity_match or payload_source_ticker != current_ticker:
            _run(p, "MarketScreener data suppressed (entity mismatch or missing current-company data). Header, Key Preview, and Appendices A–D show blanks.", size_pt=SMALL_PT, color=GRAY)
        elif ms_avail == "source_redirect":
            _run(p, "MarketScreener consensus and appendices unavailable (source redirect). Figures from other sources.", size_pt=SMALL_PT, color=GRAY)

    # ─── 3. Investment View (2 substantial paragraphs; inline source links when available) ─
    p = doc.add_paragraph()
    _run(p, "Investment View", bold=True, size_pt=SECTION_PT, color=ACCENT)
    p.paragraph_format.space_before = SPACE_NONE
    p.paragraph_format.space_after = SPACE_TINY

    ns = getattr(payload, "news_summary", None)
    p1 = getattr(ns, "investment_view_paragraph_1", "") if ns else ""
    p2 = getattr(ns, "investment_view_paragraph_2", "") if ns else ""
    ref_articles = getattr(ns, "referenced_articles", None) or []
    citation_placements = getattr(ns, "citation_placements", None) or []
    if memo_data:
        from src.services.qa_engine import guardrail_paragraphs
        p1, p2 = guardrail_paragraphs(p1 or "", p2 or "")
    err_like = ("failed", "error", "exception", "traceback", "api")
    news_items_list = getattr(payload, "news_items", None) or []
    has_valid_rc = bool([a for a in news_items_list if _is_valid_recent_context_article(a)])
    effective_refs: list = []
    effective_placements: list = []
    final_iv_p1, final_iv_p2 = "", ""
    # When Recent Context is rendered, Investment View must use at least one fact from a selected article (mandatory)
    if has_valid_rc and news_items_list and (not ref_articles or not citation_placements):
        first_article = next((a for a in news_items_list if _is_valid_recent_context_article(a)), None)
        if first_article:
            fact = (getattr(first_article, "extracted_fact", None) or getattr(first_article, "snippet", None) or getattr(first_article, "headline", None) or "").strip()
            if len(fact) > 200:
                fact = fact[:197] + "…"
            pub = getattr(first_article, "source", None) or "Source"
            dt = getattr(first_article, "published_at", None)
            date_str = dt.strftime("%b %d") if dt and hasattr(dt, "strftime") else ""
            cite_suffix = f" ({pub}, {date_str})" if date_str else f" ({pub})"
            inject = f" Recent coverage noted: {fact}{cite_suffix}." if fact else f" Recent context is relevant ({pub}{', ' + date_str if date_str else ''})."
            p1 = (p1 or "").rstrip()
            if p1 and not p1.endswith("."):
                p1 = p1 + "."
            p1 = p1 + inject
            first_idx = next(i for i, a in enumerate(news_items_list) if _is_valid_recent_context_article(a))
            ref_articles = [first_article]
            sentences_p1 = _split_sentences(p1)
            citation_placements = [{"paragraph": 1, "after_sentence": len(sentences_p1) - 1, "article_index": first_idx}]
            if qa_audit is not None:
                qa_audit["investment_view_effective_ref_articles"] = ref_articles
    min_len = 20 if has_valid_rc else 40  # When recent context exists, prefer LLM output over fallback
    if p1 and p2 and len(p1) > min_len and len(p2) > min_len and not any(e in (p1 + p2).lower() for e in err_like):
        # Inline citations: article_index refers to payload.news_items (the selected list sent to Gemini)
        news_items_for_index = news_items_list

        def _render_paragraph_with_citations(para_text: str, para_num: int):
            para = doc.add_paragraph()
            para.paragraph_format.space_after = SPACE_SMALL if para_num == 1 else SPACE_NONE
            sentences = _split_sentences(para_text[:1200] + ("…" if len(para_text) > 1200 else ""))
            placements_for_para = [x for x in citation_placements if x.get("paragraph") == para_num]
            for i, sent in enumerate(sentences):
                if i > 0:
                    _run(para, " ", size_pt=BODY_PT)
                _run(para, sent, size_pt=BODY_PT)
                for pl in placements_for_para:
                    if pl.get("after_sentence") == i:
                        idx = pl.get("article_index", 0)
                        art = news_items_for_index[idx] if 0 <= idx < len(news_items_for_index) else None
                        if art:
                            pub = getattr(art, "source", None) or "Source"
                            dt = getattr(art, "published_at", None)
                            label = f" ({pub}, {dt.strftime('%b %d')})" if dt and hasattr(dt, "strftime") else f" ({pub})"
                            url = getattr(art, "url", None) or ""
                            _run(para, " ", size_pt=BODY_PT)
                            _add_hyperlink(para, label, url, size_pt=SMALL_PT, color=GRAY)
                        break

        if ref_articles and citation_placements and news_items_for_index:
            _render_paragraph_with_citations(p1, 1)
            _render_paragraph_with_citations(p2, 2)
        else:
            para1 = doc.add_paragraph()
            para1.paragraph_format.space_after = SPACE_SMALL
            _run(para1, p1[:1200] + ("…" if len(p1) > 1200 else ""), size_pt=BODY_PT)
            para2 = doc.add_paragraph()
            para2.paragraph_format.space_after = SPACE_NONE
            _run(para2, p2[:1200] + ("…" if len(p2) > 1200 else ""), size_pt=BODY_PT)
        effective_refs = ref_articles
        effective_placements = citation_placements
        final_iv_p1, final_iv_p2 = p1, p2

        # Referenced Articles (when LLM cited specific articles)
        if ref_articles:
            p_ref = doc.add_paragraph()
            p_ref.paragraph_format.space_before = SPACE_SMALL
            p_ref.paragraph_format.space_after = SPACE_TINY
            _run(p_ref, "Referenced Articles", bold=True, size_pt=SMALL_PT, color=ACCENT)
            for art in ref_articles[:4]:
                line = doc.add_paragraph()
                line.paragraph_format.space_before = SPACE_NONE
                line.paragraph_format.space_after = SPACE_TINY
                headline = (getattr(art, "headline", None) or "")[:80] + ("…" if len(getattr(art, "headline", "") or "") > 80 else "")
                dt = getattr(art, "published_at", None)
                date_str = dt.strftime("%b %d") if dt and hasattr(dt, "strftime") else ""
                pub = getattr(art, "source", None) or ""
                label = f"{headline}  ({pub}, {date_str})" if date_str else f"{headline}  ({pub})"
                url = getattr(art, "url", None) or ""
                _add_hyperlink(line, label, url, size_pt=SMALL_PT, color=GRAY)
    else:
        # Fuller 2-paragraph fallback from memo only (no generic filler)
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
        rev_surprise = memo.get("avg_revenue_surprise_pct")
        eps_surprise = memo.get("avg_eps_surprise_pct")
        qoq_rev = memo.get("qoq_revenue_pct")
        yoy_rev = memo.get("yoy_revenue_pct_table")
        qoq_eps = memo.get("qoq_eps_pct")
        yoy_eps = memo.get("yoy_eps_pct_table")
        # Para 1: two polished paragraphs with natural sentence flow and spacing
        rec_line = f"Consensus is {rec}"
        if an_str:
            rec_line += f" ({an_str})"
        rec_line += "."
        if price is not None:
            rec_line += f" The average target is {_fmt_num(price)}"
            if spread is not None:
                rec_line += f", implying upside of {_fmt_pct(spread, signed=True)}"
            rec_line += "."
        elif spread is not None:
            rec_line += f" Implied upside is {_fmt_pct(spread, signed=True)}."
        fallback_p1 = f"{c.company_name} reports {preview_short}. {rec_line}"
        if rev_surprise is not None or eps_surprise is not None:
            beat_parts = []
            if rev_surprise is not None:
                beat_parts.append(f"Revenue surprise versus consensus has averaged {_fmt_pct(rev_surprise, signed=True)}")
            if eps_surprise is not None:
                beat_parts.append(f"EPS surprise {_fmt_pct(eps_surprise, signed=True)}")
            if beat_parts:
                fallback_p1 += " " + "; ".join(beat_parts) + "."
        # Only mention QoQ/YoY when comparison bases exist (no "—" with a %)
        calendar_prior = memo.get("calendar_prior_quarter_released") or {}
        calendar_same_ly = memo.get("calendar_same_q_prior_yr_released") or {}
        has_prior = (calendar_prior.get("net_sales") is not None) or (memo.get("prior_quarter_actual_revenue") is not None)
        has_same_ly = (calendar_same_ly.get("net_sales") is not None) or (memo.get("same_quarter_prior_year_revenue") is not None)
        if (qoq_rev is not None and has_prior) or (yoy_rev is not None and has_same_ly):
            q_part = _fmt_pct(qoq_rev, signed=True) if (qoq_rev is not None and has_prior) else "—"
            y_part = _fmt_pct(yoy_rev, signed=True) if (yoy_rev is not None and has_same_ly) else "—"
            fallback_p1 += f" Key preview: quarter-on-quarter {q_part}, year-on-year {y_part}."
        if spread is not None:
            fallback_p1 += " Expectations into the print look " + ("supportive" if spread > 0 else "balanced" if spread == 0 else "demanding") + "."
        else:
            fallback_p1 += " Expectations into the print look balanced."
        # Para 2: sector-specific (no asset quality for non-banks, no backlog for oil & gas, etc.)
        _, _, fallback_p2 = _sector_operating_kpis_and_what_matters(c)
        # When Recent Context exists, Investment View must use at least one article (extracted_fact or snippet/headline)
        fallback_article_cite = None
        if has_valid_rc and news_items_list:
            first_article = next((a for a in news_items_list if _is_valid_recent_context_article(a)), None)
            if first_article:
                fact = (getattr(first_article, "extracted_fact", None) or getattr(first_article, "snippet", None) or getattr(first_article, "headline", None) or "").strip()
                if len(fact) > 200:
                    fact = fact[:197] + "…"
                if fact:
                    fallback_p1 += f" Recent coverage noted: {fact}"
                else:
                    fallback_p1 += " Recent context is relevant."
                pub = getattr(first_article, "source", None) or "Source"
                dt = getattr(first_article, "published_at", None)
                date_str = dt.strftime("%b %d") if dt and hasattr(dt, "strftime") else ""
                fallback_article_cite = (first_article, date_str, pub)
        effective_refs = [fallback_article_cite[0]] if fallback_article_cite else []
        fallback_sents_p1 = _split_sentences(fallback_p1)
        effective_placements = [{"paragraph": 1, "after_sentence": len(fallback_sents_p1) - 1, "article_index": 0}] if fallback_article_cite else []
        final_iv_p1, final_iv_p2 = fallback_p1, fallback_p2
        if qa_audit is not None and effective_refs:
            qa_audit["investment_view_effective_ref_articles"] = effective_refs
        para1 = doc.add_paragraph()
        para1.paragraph_format.space_after = SPACE_SMALL
        _run(para1, fallback_p1, size_pt=BODY_PT)
        if fallback_article_cite:
            art, date_str, pub = fallback_article_cite
            label = f" ({pub}, {date_str})" if date_str else f" ({pub})"
            url = getattr(art, "url", None) or ""
            _run(para1, " ", size_pt=BODY_PT)
            _add_hyperlink(para1, label, url, size_pt=SMALL_PT, color=GRAY)
        para2 = doc.add_paragraph()
        para2.paragraph_format.space_after = SPACE_NONE
        _run(para2, fallback_p2, size_pt=BODY_PT)

    # Article-to-sentence traceability for QA: sentence, source_type, article_headline, status
    if qa_audit is not None and (final_iv_p1 or final_iv_p2):
        inv_sentences_list: list[dict] = []
        for para_num, text in [(1, final_iv_p1), (2, final_iv_p2)]:
            if not text:
                continue
            sents = _split_sentences(text[:1200] + ("…" if len(text) > 1200 else ""))
            for i, sent in enumerate(sents):
                source_type = "memo_fact"
                article_headline = ""
                for pl in effective_placements:
                    if pl.get("paragraph") == para_num and pl.get("after_sentence") == i:
                        idx = pl.get("article_index", 0)
                        if effective_refs and 0 <= idx < len(effective_refs):
                            source_type = "recent_context_fact"
                            article_headline = (getattr(effective_refs[idx], "headline", None) or "")[:80]
                        break
                inv_sentences_list.append({
                    "sentence": (sent[:500] + ("…" if len(sent) > 500 else "")),
                    "source_type": source_type,
                    "article_headline": article_headline,
                    "status": "kept",
                })
        qa_audit["investment_view_sentences"] = inv_sentences_list
        qa_audit["investment_view_effective_ref_articles"] = effective_refs

    # ─── Recent Context (only when valid articles: headline + URL + Reuters/ZAWYA) ─
    rc_render_count, rc_displayed_headlines = _add_recent_context_section(doc, payload)
    if qa_audit is not None:
        qa_audit["recent_context_render_count"] = rc_render_count
        qa_audit["recent_context_displayed_headlines"] = rc_displayed_headlines

    # ─── 4. Key Preview (fixed structure: 7 columns × 4 rows; em dash for missing data) ─
    p = doc.add_paragraph()
    _run(p, "Key Preview", bold=True, size_pt=SECTION_PT, color=ACCENT)
    p.paragraph_format.space_before = SPACE_SMALL
    p.paragraph_format.space_after = SPACE_TINY

    calendar_next = memo.get("calendar_next_quarter") or {}
    calendar_prior = memo.get("calendar_prior_quarter_released") or {}
    calendar_same_ly = memo.get("calendar_same_q_prior_yr_released") or {}
    prev_q_short = memo.get("prior_quarter_short") or "4Q25"
    same_q_short = memo.get("prior_year_same_quarter_short") or "1Q25"

    # Fixed 7 columns: do not remove columns when data is missing
    base_cols = [
        "Metric",
        f"{preview_short}E (Our)",
        "Consensus",
        f"{prev_q_short}A",
        "QoQ",
        f"{same_q_short}A",
        "YoY",
    ]
    ncol = 7

    clean_labels = {
        "net_sales": f"Revenue ({curr} m)",
        "net_income": f"Net income ({curr} m)",
        "eps": f"EPS ({curr})",
        "ebit": f"EBIT ({curr} m)",
        "ebitda": f"EBITDA ({curr} m)",
    }
    # Fixed 4 rows: Revenue, EBIT or EBITDA (as applicable), Net income, EPS — always render all four
    if c.is_bank:
        key_order = ["net_sales", "ebit", "net_income", "eps"]
    else:
        key_order = ["net_sales", "ebitda", "net_income", "eps"]

    preview_rows = []
    for key in key_order:
        label = clean_labels.get(key, key.replace("_", " ").title())
        cons_val = calendar_next.get(key)
        cons_str = _fmt_num(cons_val) if cons_val is not None else "—"
        if key == "eps" and isinstance(cons_val, (int, float)):
            cons_str = f"{cons_val:,.2f}"
        prev_a_val = calendar_prior.get(key)
        prev_a = _fmt_num(prev_a_val) if prev_a_val is not None else "—"
        # Only show QoQ when prior-quarter base is present (never show "—" with a %)
        qoq = (
            memo.get("qoq_revenue_pct") if key == "net_sales"
            else memo.get("qoq_ni_pct") if key == "net_income"
            else memo.get("qoq_eps_pct") if key == "eps" else None
        )
        qoq_str = _fmt_pct(qoq, signed=True) if (qoq is not None and prev_a_val is not None) else "—"
        same_ly_val = calendar_same_ly.get(key)
        same_ly_str = _fmt_num(same_ly_val) if same_ly_val is not None else "—"
        # Only show YoY when same-quarter-last-year base is present (never show "—" with e.g. -100% YoY)
        yoy_val = (
            memo.get("yoy_revenue_pct_table") if key == "net_sales"
            else memo.get("yoy_ni_pct_table") if key == "net_income"
            else memo.get("yoy_eps_pct_table") if key == "eps" else None
        )
        yoy_str = _fmt_pct(yoy_val, signed=True) if (yoy_val is not None and same_ly_val is not None) else "—"
        # Our (manual) column always em dash unless we add a separate source later
        preview_rows.append((label, "—", cons_str, prev_a, qoq_str, same_ly_str, yoy_str))

    # Always 1 header + 4 data rows × 7 columns — proper horizontal header row with shading and bold
    tbl = doc.add_table(rows=1 + 4, cols=ncol)
    tbl.style = "Table Grid"
    style_table_header_row(tbl.rows[0], base_cols, _run)
    for r in tbl.rows:
        set_compact_row_height(r, 14)
    for ri, row_data in enumerate(preview_rows):
        row_cells = tbl.rows[ri + 1].cells
        for ci, val in enumerate(row_data):
            row_cells[ci].paragraphs[0].clear()
            _run(row_cells[ci].paragraphs[0], val, size_pt=SOURCE_PT, color=GRAY if val == "—" else BODY)

    # ─── 5. Operating Metrics (Sector-Specific) — always render, 4 rows ───
    p = doc.add_paragraph()
    p.paragraph_format.space_before = SPACE_SMALL
    _run(p, "Operating Metrics (Sector-Specific)", bold=True, size_pt=SECTION_PT, color=ACCENT)
    p.paragraph_format.space_after = SPACE_TINY

    op_headers = ["KPI", f"{preview_short}E", f"{prev_q_short}A", f"{same_q_short}A", "Commentary"]
    op_kpis, _, _ = _sector_operating_kpis_and_what_matters(c)
    if len(op_kpis) < 4:
        op_kpis = list(op_kpis) + [""] * (4 - len(op_kpis))
    op_kpis = op_kpis[:4]
    tbl_op = doc.add_table(rows=1 + 4, cols=5)
    tbl_op.style = "Table Grid"
    style_table_header_row(tbl_op.rows[0], op_headers, _run)
    set_cell_shading(tbl_op.rows[0].cells[0], HEADER_FILL)
    for i in range(1, 5):
        set_cell_shading(tbl_op.rows[0].cells[i], HEADER_FILL)
    for ri, kpi in enumerate(op_kpis):
        row_cells = tbl_op.rows[ri + 1].cells
        row_cells[0].paragraphs[0].clear()
        _run(row_cells[0].paragraphs[0], kpi if kpi else "", size_pt=SOURCE_PT, color=BODY if kpi else GRAY)
        for ci in range(1, 5):
            row_cells[ci].paragraphs[0].clear()
            _run(row_cells[ci].paragraphs[0], "", size_pt=SOURCE_PT, color=GRAY)
        set_compact_row_height(tbl_op.rows[ri + 1], 14)

    # ─── 6. Street Snapshot (P/E only if labeled and reconciled per QA) ───
    p = doc.add_paragraph()
    p.paragraph_format.space_before = SPACE_SMALL
    _run(p, "Street Snapshot", bold=True, size_pt=SECTION_PT, color=ACCENT)
    p.paragraph_format.space_after = SPACE_TINY
    snap = [f"Consensus: {mean_cons}", f"{n_analysts} analysts"]
    if target is not None and isinstance(target, (int, float)):
        snap.append(f"Target: {curr} {target:,.2f}")
    if spread is not None:
        snap.append(f"Upside: {_fmt_pct(spread, signed=True)}")
    show_pe = False
    pe_label = ""
    pe_val = None
    if memo_data:
        street = memo_data.get("street_snapshot") or {}
        pe_f = street.get("pe")
        if isinstance(pe_f, dict) and pe_f.get("status") in ("pass", "stale") and (pe_f.get("label") or "").strip():
            pe_val = _field_display(pe_f)
            pe_label = (pe_f.get("label") or "").strip()
            show_pe = pe_val is not None and isinstance(pe_val, (int, float))
    if not show_pe and not memo_data:
        vm = payload.ms_valuation_multiples
        if vm and (vm.get("pe") or []) and (vm.get("pe") or [])[0] is not None:
            pe_val = (vm.get("pe") or [])[0]
            pe_label = "P/E"
            show_pe = True
    if show_pe and pe_val is not None:
        snap.append(f"{pe_label}: {pe_val:.1f}x" if pe_label else f"P/E: {pe_val:.1f}x")
    p = doc.add_paragraph()
    _run(p, "  ·  ".join(snap), size_pt=SMALL_PT, color=GRAY)

    # ─── E. Recent Execution (compact callout box) ──────────────────────
    p = doc.add_paragraph()
    p.paragraph_format.space_before = SPACE_SMALL
    _run(p, "Recent Execution", bold=True, size_pt=SECTION_PT, color=ACCENT)
    p.paragraph_format.space_after = SPACE_TINY

    surprise_list = memo.get("revenue_surprise_history")
    eps_surprise = memo.get("eps_surprise_history")
    ni_surprise = memo.get("ni_surprise_history")
    avg_rev = memo.get("avg_revenue_surprise_pct")
    avg_eps = memo.get("avg_eps_surprise_pct")
    avg_ni = memo.get("avg_ni_surprise_pct")
    consec = memo.get("consecutive_revenue_beats")

    def _beat_line(entries, avg_pct, label):
        if not entries:
            return None
        total = len(entries)
        beat_count = len([e for e in entries if (e.get("surprise_pct") or 0) >= 0])
        s = f"{label} beat in {beat_count}/{total} visible quarters"
        if avg_pct is not None:
            s += f", avg. {_fmt_pct(avg_pct, signed=True)}"
        return s

    beat_lines = []
    if surprise_list:
        line = _beat_line(surprise_list, avg_rev, "Revenue")
        if line:
            if consec is not None:
                line += f" ({consec} consecutive)"
            beat_lines.append(line)
    if eps_surprise:
        line = _beat_line(eps_surprise, avg_eps, "EPS")
        if line:
            beat_lines.append(line)
    if ni_surprise:
        line = _beat_line(ni_surprise, avg_ni, "Net income")
        if line:
            beat_lines.append(line)

    if beat_lines:
        for line in beat_lines[:5]:
            para = doc.add_paragraph(style="List Bullet")
            para.paragraph_format.space_after = SPACE_TINY
            para.paragraph_format.space_before = SPACE_NONE
            _run(para, line, size_pt=BODY_PT)
    else:
        p = doc.add_paragraph()
        _run(p, "Beat/miss history not available from quarterly results.", size_pt=SMALL_PT, color=GRAY)
    # QA: flag when any surprise % is very large so it is reviewed rather than presented without context
    if memo_data:
        rec = memo_data.get("recent_execution") or {}
        if rec.get("extreme_surprise_flagged"):
            p = doc.add_paragraph()
            p.paragraph_format.space_after = SPACE_TINY
            _run(p, "Review: at least one surprise % is very large; verify before citing.", size_pt=SMALL_PT, color=GRAY)

    # ─── F. What Matters This Quarter (compact bullet block) ─────────────
    p = doc.add_paragraph()
    p.paragraph_format.space_before = SPACE_SMALL
    _run(p, "What Matters This Quarter", bold=True, size_pt=SECTION_PT, color=ACCENT)
    p.paragraph_format.space_after = SPACE_TINY

    _, matters, _ = _sector_operating_kpis_and_what_matters(c)
    for m in matters[:5]:
        para = doc.add_paragraph(style="List Bullet")
        para.paragraph_format.space_after = SPACE_TINY
        para.paragraph_format.space_before = SPACE_NONE
        _run(para, m, size_pt=BODY_PT)

    # End of page 1 — no full valuation table, no management guidance table here

    # ═══════════════════════════════════════════════════════════════════
    # APPENDIX (starts on new page so it is not split)
    # ═══════════════════════════════════════════════════════════════════

    appendix_sections = getattr(payload, "appendix_sections", None) or ["annual_forecasts", "quarterly_detail", "eps_dividend", "valuation", "audit"]
    appendix_suppressed_due_to_entity = not use_ms_for_render and (cross_contamination or not payload_entity_match or getattr(payload, "reused_default_payload_detected", False))

    doc.add_paragraph()
    p = doc.add_paragraph()
    p.paragraph_format.page_break_before = True
    _run(p, "Appendix", bold=True, size_pt=SECTION_PT, color=ACCENT)
    p.paragraph_format.space_after = SPACE_TINY
    if appendix_suppressed_due_to_entity:
        p = doc.add_paragraph()
        _run(p, "Appendices A–D suppressed (entity mismatch or contamination). No MarketScreener-derived figures shown.", size_pt=SMALL_PT, color=GRAY)
        p.paragraph_format.space_after = SPACE_SMALL
    else:
        p.paragraph_format.space_after = SPACE_SMALL

    # Appendix A — Annual Financial Forecasts
    if "annual_forecasts" in appendix_sections:
        p = doc.add_paragraph()
        _run(p, "Appendix A — Annual Financial Forecasts", bold=True, size_pt=SECTION_PT, color=ACCENT)
        p.paragraph_format.space_after = SPACE_TINY

        ann = (payload.ms_annual_forecasts or {}).get("annual", {})
        ed = (payload.ms_eps_dividend_forecasts or {})
        ann_p = ann.get("periods", [])
        ann_sales = ann.get("net_sales", [])
        ann_ni = ann.get("net_income", [])
        ed_p = ed.get("periods", [])
        ed_eps = ed.get("eps", [])
        ed_dps = ed.get("dividend_per_share", [])

        if ann_p or ed_p:
            def _idx(m, yr):
                ys = str(yr)
                y2 = ys[2:]  # e.g. "25"
                for k, i in m.items():
                    if ys in k:
                        return i
                    if y2 in k and ("FY" in k.upper() or "y" in k.lower() or len(k) <= 4):
                        return i
                return None
            ann_map = {str(x).strip(): i for i, x in enumerate(ann_p)}
            ed_map = {str(x).strip(): i for i, x in enumerate(ed_p)}
            i24 = _idx(ann_map, 2024)
            i25 = _idx(ann_map, 2025)
            i26 = _idx(ann_map, 2026)
            i27 = _idx(ann_map, 2027)
            e24 = _idx(ed_map, 2024)
            e25 = _idx(ed_map, 2025)
            e26 = _idx(ed_map, 2026)
            e27 = _idx(ed_map, 2027)
            if i25 is None and ann_map:
                n = len(ann_p)
                i24, i25, i26, i27 = (n - 4 if n >= 4 else 0), (n - 3 if n >= 3 else 0), (n - 2 if n >= 2 else 0), (n - 1)
            if e25 is None and ed_map and len(ed_p) >= 4:
                n = len(ed_p)
                e24, e25, e26, e27 = (n - 4 if n >= 4 else 0), (n - 3 if n >= 3 else 0), (n - 2 if n >= 2 else 0), (n - 1)

            def _ann_val(arr, idx):
                if arr and idx is not None and 0 <= idx < len(arr) and arr[idx] is not None:
                    return arr[idx] / 1e3
                return None

            def _ed_val(arr, idx):
                if arr and idx is not None and 0 <= idx < len(arr) and arr[idx] is not None:
                    return arr[idx]
                return None

            def _fmt_eps(v):
                if v is None:
                    return "—"
                if isinstance(v, (int, float)):
                    return f"{round(float(v), 2):,.2f}"
                return str(v)

            tbl_a = doc.add_table(rows=1, cols=6)
            tbl_a.style = "Table Grid"
            for i, h in enumerate(["Metric", "FY24A", "FY25E", "Chg", "FY26E", "FY27E"]):
                tbl_a.rows[0].cells[i].paragraphs[0].clear()
                set_cell_shading(tbl_a.rows[0].cells[i], "1A5276")
                _run(tbl_a.rows[0].cells[i].paragraphs[0], h, bold=True, size_pt=SOURCE_PT, color=RGBColor(0xFF, 0xFF, 0xFF))
            for row_name, get_vals, fmt in [
                (f"Net sales ({curr} bn)", lambda: (_ann_val(ann_sales, i24), _ann_val(ann_sales, i25), _ann_val(ann_sales, i26), _ann_val(ann_sales, i27)), _fmt_num),
                (f"Net income ({curr} bn)", lambda: (_ann_val(ann_ni, i24), _ann_val(ann_ni, i25), _ann_val(ann_ni, i26), _ann_val(ann_ni, i27)), _fmt_num),
                (f"EPS ({curr})", lambda: (_ed_val(ed_eps, e24), _ed_val(ed_eps, e25), _ed_val(ed_eps, e26), _ed_val(ed_eps, e27)), _fmt_eps),
                (f"Dividend / share ({curr})", lambda: (_ed_val(ed_dps, e24), _ed_val(ed_dps, e25), _ed_val(ed_dps, e26), _ed_val(ed_dps, e27)), _fmt_num),
            ]:
                v24, v25, v26, v27 = get_vals()
                chg = round((v25 - v24) / v24 * 100, 1) if v24 and v25 and v24 != 0 else None
                r = tbl_a.add_row().cells
                r[0].paragraphs[0].clear()
                _run(r[0].paragraphs[0], row_name, size_pt=SOURCE_PT)
                for j, v in enumerate([v24, v25, chg, v26, v27]):
                    r[j + 1].paragraphs[0].clear()
                    if j == 2:
                        _run(r[j + 1].paragraphs[0], _fmt_pct(chg, signed=True) if chg is not None else "—", size_pt=SOURCE_PT)
                    else:
                        _run(r[j + 1].paragraphs[0], fmt(v) if v is not None else "—", size_pt=SOURCE_PT)
        else:
            p = doc.add_paragraph()
            _run(p, "No annual consensus data available.", size_pt=SOURCE_PT, color=GRAY)
        doc.add_paragraph()

    # Appendix B — Quarterly Results Detail (Actual + Surprise only; no forecast columns)
    if "quarterly_detail" in appendix_sections:
        qr = (payload.ms_calendar_events or {}).get("quarterly_results", {}) or {}
        qr_quarters = qr.get("quarters", [])
        qr_rows = qr.get("rows", [])
        if qr_quarters and qr_rows:
            p = doc.add_paragraph()
            _run(p, "Appendix B — Quarterly Results Detail", bold=True, size_pt=SECTION_PT, color=ACCENT)
            p.paragraph_format.space_after = SPACE_TINY
            # Columns: Quarter + for each metric (Revenue, Net income, EPS): A, Surprise only
            key_to_labels = {"net_sales": ("Revenue A", "Rev Surprise"), "net_income": ("Net income A", "NI Surprise"), "eps": ("EPS A", "EPS Surprise")}
            metric_cols = [(mk, key_to_labels[mk]) for mk in ["net_sales", "net_income", "eps"] if any(r.get("metric_key") == mk for r in qr_rows)]
            headers = ["Quarter"] + [lab for _, (la, ls) in metric_cols for lab in (la, ls)]
            ncols = len(headers)
            tbl_b = doc.add_table(rows=1 + len(qr_quarters), cols=ncols)
            tbl_b.style = "Table Grid"
            for i, h in enumerate(headers):
                tbl_b.rows[0].cells[i].paragraphs[0].clear()
                set_cell_shading(tbl_b.rows[0].cells[i], "1A5276")
                _run(tbl_b.rows[0].cells[i].paragraphs[0], h, bold=True, size_pt=SOURCE_PT, color=RGBColor(0xFF, 0xFF, 0xFF))
            def _cell_val(rows_list, key, idx, which):
                r = next((x for x in rows_list if x.get("metric_key") == key), None)
                if not r:
                    return None
                by_q = r.get("by_quarter", [])
                if idx >= len(by_q):
                    return None
                c = by_q[idx]
                if which == "A":
                    return c.get("released")
                if which == "S":
                    return c.get("spread_pct")
                return None
            for qi, q_label in enumerate(qr_quarters):
                row_cells = tbl_b.rows[qi + 1].cells
                row_cells[0].paragraphs[0].clear()
                _run(row_cells[0].paragraphs[0], str(q_label), size_pt=SOURCE_PT)
                ci = 1
                for mk, (la, ls) in metric_cols:
                    for which in ("A", "S"):
                        v = _cell_val(qr_rows, mk, qi, which)
                        row_cells[ci].paragraphs[0].clear()
                        if v is None:
                            _run(row_cells[ci].paragraphs[0], "—", size_pt=SOURCE_PT, color=GRAY)
                        elif which == "S" and isinstance(v, (int, float)):
                            _run(row_cells[ci].paragraphs[0], _fmt_pct(v, signed=True), size_pt=SOURCE_PT)
                        elif isinstance(v, (int, float)):
                            _run(row_cells[ci].paragraphs[0], _fmt_num(v), size_pt=SOURCE_PT)
                        else:
                            _run(row_cells[ci].paragraphs[0], str(v), size_pt=SOURCE_PT)
                        ci += 1
            _run(doc.add_paragraph(), "Source: MarketScreener /calendar/ Quarterly results.", size_pt=SOURCE_PT, color=GRAY)
        else:
            p = doc.add_paragraph()
            _run(p, "Appendix B — Quarterly Results Detail", bold=True, size_pt=SECTION_PT, color=ACCENT)
            _run(doc.add_paragraph(), "No quarterly results table available.", size_pt=SOURCE_PT, color=GRAY)
        doc.add_paragraph()

    # Appendix C — Annual EPS / Dividend / Yield (only if meaningful content)
    ed = (payload.ms_eps_dividend_forecasts or {})
    ed_p = ed.get("periods", [])
    ed_eps = ed.get("eps", []) or []
    ed_dps = ed.get("dividend_per_share", []) or []
    has_eps = any(v is not None for v in ed_eps)
    has_dps = any(v is not None for v in ed_dps)
    if "eps_dividend" in appendix_sections and (ed_p and (has_eps or has_dps)):
        p = doc.add_paragraph()
        _run(p, "Appendix C — Annual EPS / Dividend / Yield", bold=True, size_pt=SECTION_PT, color=ACCENT)
        p.paragraph_format.space_after = SPACE_TINY
        # Compact table: Period | EPS | DPS (or similar)
        headers = ["Period"] + (["EPS"] if has_eps else []) + (["DPS"] if has_dps else [])
        if len(headers) > 1:
            n_per = len(ed_p)
            tbl_c = doc.add_table(rows=1 + n_per, cols=len(headers))
            tbl_c.style = "Table Grid"
            for i, h in enumerate(headers):
                tbl_c.rows[0].cells[i].paragraphs[0].clear()
                set_cell_shading(tbl_c.rows[0].cells[i], HEADER_FILL)
                _run(tbl_c.rows[0].cells[i].paragraphs[0], h, bold=True, size_pt=SOURCE_PT, color=ACCENT)
            for qi, period in enumerate(ed_p):
                set_compact_row_height(tbl_c.rows[qi + 1], 14)
                tbl_c.rows[qi + 1].cells[0].paragraphs[0].clear()
                _run(tbl_c.rows[qi + 1].cells[0].paragraphs[0], str(period), size_pt=SOURCE_PT)
                ci = 1
                if has_eps:
                    v = ed_eps[qi] if qi < len(ed_eps) else None
                    tbl_c.rows[qi + 1].cells[ci].paragraphs[0].clear()
                    _run(tbl_c.rows[qi + 1].cells[ci].paragraphs[0], _fmt_num(v) if v is not None else "—", size_pt=SOURCE_PT)
                    ci += 1
                if has_dps:
                    v = ed_dps[qi] if qi < len(ed_dps) else None
                    tbl_c.rows[qi + 1].cells[ci].paragraphs[0].clear()
                    _run(tbl_c.rows[qi + 1].cells[ci].paragraphs[0], _fmt_num(v) if v is not None else "—", size_pt=SOURCE_PT)
        _run(doc.add_paragraph(), "Source: MarketScreener /valuation-dividend/.", size_pt=SOURCE_PT, color=GRAY)
        doc.add_paragraph()

    # Appendix D — Valuation Multiples (full table here, not on page 1)
    if "valuation" in appendix_sections:
        p = doc.add_paragraph()
        _run(p, "Appendix D — Valuation Multiples", bold=True, size_pt=SECTION_PT, color=ACCENT)
        p.paragraph_format.space_after = SPACE_TINY

        vm = payload.ms_valuation_multiples
        if vm and vm.get("periods"):
            periods_v = vm.get("periods", [])
            pe = vm.get("pe", []) or []
            pbr = vm.get("pbr", []) or []
            yld = vm.get("yield_pct", []) or []
            ev_ebit = vm.get("ev_ebit", []) or []

            def _v_idx(yr):
                for i, p in enumerate(periods_v):
                    if str(yr) in str(p):
                        return i
                return None

            v25, v26, v27 = _v_idx(2025), _v_idx(2026), _v_idx(2027)
            if v25 is None and periods_v:
                n = len(periods_v)
                v25, v26, v27 = (n - 3 if n >= 3 else 0), (n - 2 if n >= 2 else 0), (n - 1)

            def _cell(arr, idx):
                if arr and idx is not None and 0 <= idx < len(arr) and arr[idx] is not None:
                    return arr[idx]
                return None

            tbl_d = doc.add_table(rows=1, cols=4)
            tbl_d.style = "Table Grid"
            for i, h in enumerate(["Multiple", "FY25E", "FY26E", "FY27E"]):
                tbl_d.rows[0].cells[i].paragraphs[0].clear()
                set_cell_shading(tbl_d.rows[0].cells[i], "1A5276")
                _run(tbl_d.rows[0].cells[i].paragraphs[0], h, bold=True, size_pt=SOURCE_PT, color=RGBColor(0xFF, 0xFF, 0xFF))
            for row_name, arr in [("P/E", pe), ("P/B", pbr), ("Div. Yield", yld), ("EV/EBIT", ev_ebit)]:
                r = tbl_d.add_row().cells
                r[0].paragraphs[0].clear()
                _run(r[0].paragraphs[0], row_name, size_pt=SOURCE_PT)
                for j, idx in enumerate([v25, v26, v27]):
                    v = _cell(arr, idx)
                    r[j + 1].paragraphs[0].clear()
                    if v is None:
                        _run(r[j + 1].paragraphs[0], "—", size_pt=SOURCE_PT)
                    else:
                        _run(r[j + 1].paragraphs[0], f"{v:.1f}x" if "Yield" not in row_name else f"{v}%", size_pt=SOURCE_PT)
        else:
            _run(doc.add_paragraph(), "Valuation multiples not available.", size_pt=SOURCE_PT, color=GRAY)
        doc.add_paragraph()

    # Appendix E omitted from client-facing memo (internal/audit only).

    # Footer
    p = doc.add_paragraph()
    p.paragraph_format.space_before = SPACE_SMALL
    _run(p, "This memo is for informational purposes only and does not constitute investment advice.", size_pt=SOURCE_PT, color=GRAY)

    path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(path))


def run(payload: ReportPayload, memo_data: dict | None = None, qa_audit: dict | None = None) -> StepResult:
    with StepTimer() as t:
        try:
            out_dir = report_output_dir()
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"{payload.company.ticker}_preview_{ts}.docx"
            out_path = out_dir / fname
            _build(payload, out_path, memo_data=memo_data, qa_audit=qa_audit)
            if qa_audit and memo_data:
                news_items = getattr(payload, "news_items", None) or []
                ns = getattr(payload, "news_summary", None)
                valid_articles = [a for a in news_items if _is_valid_recent_context_article(a)]
                # Use effective refs (injected or fallback when IV must cite an article) so QA counts correctly
                refs = qa_audit.get("investment_view_effective_ref_articles") or (getattr(ns, "referenced_articles", None) or [] if ns else [])
                placements = getattr(ns, "citation_placements", None) or [] if ns else []

                qa_audit["recent_context_query_log"] = getattr(payload, "recent_context_query_log", None) or []
                qa_audit["recent_context_candidate_count"] = getattr(payload, "recent_context_candidate_count", 0) or 0
                qa_audit["recent_context_valid_count"] = getattr(payload, "recent_context_valid_count", 0) or len(valid_articles)
                qa_audit["recent_context_rejected_reasons"] = getattr(payload, "recent_context_rejected_reasons", None) or []
                qa_audit["candidate_valid_basic"] = getattr(payload, "candidate_valid_basic", False)
                qa_audit["candidate_has_date_before_enrichment"] = getattr(payload, "candidate_has_date_before_enrichment", 0)
                qa_audit["candidate_has_extracted_fact"] = getattr(payload, "candidate_has_extracted_fact", 0)
                qa_audit["final_article_valid_count"] = getattr(payload, "final_article_valid_count", 0)
                qa_audit["date_parse_attempted"] = getattr(payload, "date_parse_attempted", 0)
                qa_audit["date_parse_source"] = getattr(payload, "date_parse_source", None) or []
                qa_audit["date_parse_success"] = getattr(payload, "date_parse_success", 0)
                qa_audit["candidates_rejected_for_missing_date"] = getattr(payload, "candidates_rejected_for_missing_date", 0)
                qa_audit["candidates_recovered_after_article_fetch"] = getattr(payload, "candidates_recovered_after_article_fetch", 0)
                qa_audit["recent_context_enrichment_log"] = getattr(payload, "recent_context_enrichment_log", None) or []
                qa_audit["rejected_candidates_top_10"] = getattr(payload, "rejected_candidates_top_10", None) or []
                qa_audit["recent_context_articles_qa"] = getattr(payload, "recent_context_articles_qa", None) or []
                qa_audit["recent_context_retrieved"] = len(valid_articles) > 0
                qa_audit["recent_context_has_valid_articles"] = len(valid_articles) > 0
                qa_audit["recent_context_article_count"] = len(valid_articles)
                qa_audit["recent_context_render_count"] = qa_audit.get("recent_context_render_count", 0)
                qa_audit["recent_context_rendered"] = qa_audit["recent_context_render_count"] > 0
                qa_audit["investment_view_used_article_count"] = len(refs)
                qa_audit["investment_view_used_article_headlines"] = [getattr(a, "headline", "") for a in refs]
                qa_audit["recent_context_used_headlines"] = qa_audit["investment_view_used_article_headlines"]
                qa_audit["investment_view_uses_recent_context"] = len(refs) > 0 or len(placements) > 0

                fail_reason = None
                cand = qa_audit.get("recent_context_candidate_count", 0)
                valid = qa_audit.get("recent_context_valid_count", 0)
                rc_render = qa_audit.get("recent_context_render_count", 0)
                iv_used = qa_audit.get("investment_view_used_article_count", 0)
                if cand == 0:
                    qa_audit["recent_context_failure_stage"] = "retrieval"
                elif valid == 0:
                    qa_audit["recent_context_failure_stage"] = "validation"
                elif valid > 0 and rc_render == 0:
                    qa_audit["recent_context_failure_stage"] = "render"
                    fail_reason = "Valid articles had metadata but none rendered (headline+URL+Reuters/ZAWYA required)."
                elif valid > 0 and iv_used == 0:
                    qa_audit["recent_context_failure_stage"] = "iv_citation"
                    fail_reason = (fail_reason or "") + " Investment View did not cite any recent-context article."
                else:
                    qa_audit["recent_context_failure_stage"] = ""
                if not fail_reason and valid > 0 and rc_render == 0:
                    fail_reason = "Valid articles had metadata but none rendered."
                if valid > 0 and iv_used == 0 and (fail_reason or "").find("Investment View") == -1:
                    fail_reason = (fail_reason or "") + " Investment View did not cite any recent-context article."
                qa_audit["recent_context_render_failed_reason"] = fail_reason
                if fail_reason:
                    qa_audit.setdefault("warnings", []).append(f"QA FAIL: {fail_reason}")
                # Internal QA output: primary format is .docx for human review
                qa_docx_path = out_path.parent / (out_path.stem + "_QA.docx")
                p1 = getattr(ns, "investment_view_paragraph_1", "") if ns else ""
                p2 = getattr(ns, "investment_view_paragraph_2", "") if ns else ""
                inv_sentences = []
                if p1 or p2:
                    from src.services.qa_engine import classify_sentences_for_qa
                    inv_sentences = classify_sentences_for_qa(p1 or "", p2 or "")
                from src.services.qa_audit_docx import write_qa_audit_docx
                write_qa_audit_docx(
                    qa_audit, memo_data,
                    path=qa_docx_path,
                    inv_sentences=inv_sentences if inv_sentences else None,
                    ticker=payload.company.ticker,
                    duplicate_screening_log=getattr(payload, "duplicate_screening_log", None) or [],
                )
                # Optional JSON for debugging (config: report.qa_audit_json)
                if cfg().get("report", {}).get("qa_audit_json", False):
                    qa_json_path = out_path.with_suffix(".qa.json")
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(qa_json_path, "w", encoding="utf-8") as f:
                        json.dump(qa_audit, f, indent=2, default=str)
            return StepResult(
                step_name=STEP, status=Status.SUCCESS, source="python-docx",
                message=f"Report saved → {out_path}",
                data=str(out_path), elapsed_seconds=t.elapsed,
            )
        except Exception as exc:
            return StepResult(
                step_name=STEP, status=Status.FAILED, source="python-docx",
                message="Report generation failed",
                error_detail=str(exc), elapsed_seconds=t.elapsed,
            )
