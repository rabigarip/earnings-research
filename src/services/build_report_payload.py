"""
Service: build_report_payload

Pure assembly step — no external calls. Packages all step outputs into
a single ReportPayload that the docx builder can consume.
Payload isolation: each run gets a brand-new payload; MS sections are rebuilt
explicitly from parsed data. Entity validation suppresses all MS data when
lineage/entity or URL checks fail.
"""

from __future__ import annotations
import logging
log = logging.getLogger(__name__)
from copy import deepcopy
from datetime import datetime, timezone

from src.models.company        import CompanyMaster
from src.models.financials     import DerivedMetrics, FinancialPeriod, QuoteSnapshot
from src.models.news           import NewsItem, NewsSummary
from src.models.report_payload import MSLineage, ReportPayload, SourcedValue
from src.models.step_result    import Status, StepResult, StepTimer
from src.services.entity_resolution import (
    get_effective_marketscreener_slug,
    get_marketscreener_availability,
    MS_AVAILABILITY_SOURCE_REDIRECT,
    MS_AVAILABILITY_WRONG_ENTITY,
)
from src.services.ms_payload_fingerprint import check_fingerprint, compute_fingerprint
from src.storage.db import load_company

STEP = "build_report_payload"


def _ms_lineage_from_dict(d: dict | None) -> MSLineage | None:
    """Convert dict to MSLineage; return None if missing or empty."""
    if not d or not isinstance(d, dict):
        return None
    return MSLineage(
        source_ticker=str(d.get("source_ticker") or "").strip(),
        source_company_name=str(d.get("source_company_name") or "").strip(),
        source_url=str(d.get("source_url") or "").strip(),
        source_page_type=str(d.get("source_page_type") or "").strip(),
        final_url=str(d.get("final_url") or "").strip() or str(d.get("source_url") or "").strip(),
    )


def _validate_ms_entity(
    company_ticker: str,
    ms_lineage: MSLineage | None,
    ms_availability: str,
    company_name: str = "",
) -> bool:
    """
    Return True if MarketScreener data is valid for this company.
    If False, caller must suppress all MS sections and MS-derived consensus_summary.
    Requires ticker match and, when source_company_name is present, name overlap to avoid wrong-entity contamination.
    """
    if not ms_lineage:
        return False
    if (ms_lineage.source_ticker or "").strip() != (company_ticker or "").strip():
        return False
    if ms_availability in ("wrong_entity", "source_redirect"):
        return False
    source_url = (ms_lineage.source_url or "").strip()
    if not source_url or "marketscreener" not in source_url.lower():
        return False
    final_url = (ms_lineage.final_url or source_url).strip()
    # Reject homepage or interstitial (e.g. no /quote/stock/... path)
    if not final_url or final_url.rstrip("/").endswith("marketscreener.com") or "/quote/stock/" not in final_url:
        return False
    # Stricter entity check: when MS returns a company name, require clear match to avoid wrong-entity data
    src_name = (ms_lineage.source_company_name or "").strip()
    if src_name and company_name:
        cn = company_name.strip().lower()
        sn = src_name.lower()
        # Significant tokens (skip common words)
        skip = {"the", "of", "and", "for", "in", "a", "an", "co", "inc", "ltd", "corp", "plc", "group", "company", "corporation", "limited", "holding", "holdings"}
        cn_tokens = {w for w in cn.replace(",", " ").split() if len(w) > 1 and w not in skip}
        sn_tokens = {w for w in sn.replace(",", " ").split() if len(w) > 1 and w not in skip}
        if cn_tokens and sn_tokens and not (cn_tokens & sn_tokens):
            return False
    return True


def _rebuild_ms_section(data: dict | None) -> dict | None:
    """Explicit rebuild: deep copy so no stale keys survive. Returns None if input is None."""
    if data is None:
        return None
    return deepcopy(data)


def _compute_memo(
    *,
    company,
    quote,
    quarterly: list,
    consensus: list,
    consensus_summary: dict | None,
    ms_annual_forecasts: dict | None,
    ms_quarterly_forecasts: dict | None,
    ms_eps_dividend_forecasts: dict | None,
    ms_calendar_events: dict | None,
    yahoo_earnings_date: str | None = None,
    derived,
) -> dict:
    """
    Compute memo-only fields: next earnings date, next-quarter consensus,
    next-quarter YoY growth (if prior-year quarter exists), implied upside.
    Used for front-page memo only; no review-style comparison metrics.
    """
    out: dict = {}

    # Company context (consumed by LLM evidence brief and sector-specific prompts)
    out["company_name"] = getattr(company, "company_name", "")
    out["ticker"] = getattr(company, "ticker", "")
    out["is_bank"] = getattr(company, "is_bank", False)
    out["industry"] = getattr(company, "industry", "")
    out["sector"] = getattr(company, "sector", "")
    out["currency"] = (getattr(company, "currency", None) or "").strip()
    out["country"] = getattr(company, "country", "")

    # Price sanity check: suppress MS consensus if price diverges >3x from Yahoo
    # (detects wrong-entity contamination, e.g. Riyad Bank data on Al Rajhi page)
    if consensus_summary and quote and getattr(quote, "price", None):
        _ms_close = consensus_summary.get("last_close_price")
        if _ms_close and quote.price > 0:
            _ratio = _ms_close / quote.price
            if _ratio < 0.3 or _ratio > 3.0:
                import logging
                logging.getLogger(__name__).warning(
                    "Suppressing MS consensus: price divergence %.1fx (MS close=%.2f vs Yahoo=%.2f) for %s",
                    _ratio, _ms_close, quote.price, getattr(company, "ticker", "?"),
                )
                consensus_summary = None

    # Consensus details (recommendation, analyst count, target)
    if consensus_summary:
        out["consensus_recommendation"] = consensus_summary.get("consensus_rating")
        out["consensus_analyst_count"] = consensus_summary.get("analyst_count")
        out["consensus_target_price"] = consensus_summary.get("average_target_price")
        out["consensus_last_close"] = consensus_summary.get("last_close_price")

    # Quote price
    if quote:
        out["quote_price"] = getattr(quote, "price", None)

    # Next earnings date (Source: /calendar/)
    if ms_calendar_events:
        out["next_earnings_date"] = ms_calendar_events.get("next_expected_earnings_date")
        out["next_earnings_label"] = ms_calendar_events.get("next_expected_earnings_label")
        out["next_earnings_time"] = ms_calendar_events.get("next_expected_earnings_time")
    # Yahoo fallback if MarketScreener calendar is blocked/unavailable.
    if not out.get("next_earnings_date") and yahoo_earnings_date:
        out["next_earnings_date"] = yahoo_earnings_date

    # Implied upside / spread (Source: /consensus/)
    if consensus_summary and consensus_summary.get("upside_to_average_target_pct") is not None:
        out["implied_upside_pct"] = consensus_summary["upside_to_average_target_pct"]
        out["spread_pct"] = consensus_summary["upside_to_average_target_pct"]
    elif consensus_summary and consensus_summary.get("average_target_price") is not None:
        t = consensus_summary["average_target_price"]
        p = (getattr(quote, "price", None) if quote else None) or consensus_summary.get("last_close_price")
        # Sanity check: if MS last_close differs from Yahoo price by >50%, MS data is likely wrong entity
        _ms_close = consensus_summary.get("last_close_price")
        _ya_price = getattr(quote, "price", None) if quote else None
        if _ms_close and _ya_price and _ya_price > 0:
            _price_ratio = _ms_close / _ya_price
            if _price_ratio < 0.3 or _price_ratio > 3.0:
                # Price divergence too large — likely wrong company in MS. Suppress MS consensus.
                consensus_summary = {}
                out.pop("consensus_recommendation", None)
                out.pop("implied_upside_pct", None)
                out.pop("spread_pct", None)
                p = None
        if p and p != 0:
            out["spread_pct"] = round((t - p) / p * 100, 1)
    if consensus_summary and consensus_summary.get("downside_to_low_target_pct") is not None:
        out["implied_downside_pct"] = consensus_summary["downside_to_low_target_pct"]

    # Single source of truth: derive preview quarter from expected REPORT date.
    # Report date in Apr–Jun = reporting Q1; Jul–Sep = Q2; Oct–Dec = Q3; Jan–Mar = Q4 (prior year).
    next_quarter_label = None
    next_earnings_date = (ms_calendar_events or {}).get("next_expected_earnings_date")
    if next_earnings_date and isinstance(next_earnings_date, str) and len(next_earnings_date) >= 7:
        import re as _re_date
        if _re_date.match(r"20\d{2}-\d{2}-\d{2}", next_earnings_date):
            try:
                y = int(next_earnings_date[:4])
                m = int(next_earnings_date[5:7])
                if m >= 4:
                    q = (m - 4) // 3 + 1
                    yr = y
                else:
                    q = 4
                    yr = y - 1
                next_quarter_label = f"{yr} Q{q}"
            except (ValueError, IndexError):
                pass
    if not next_quarter_label and ms_calendar_events and ms_calendar_events.get("next_expected_earnings_label"):
        import re
        m = re.search(r"Q(\d)\s*(\d{4})|(\d{4})\s*Q(\d)", ms_calendar_events["next_expected_earnings_label"], re.I)
        if m:
            if m.group(1):
                next_quarter_label = f"{m.group(2)} Q{m.group(1)}"
            else:
                next_quarter_label = f"{m.group(3)} Q{m.group(4)}"
    if not next_quarter_label and ms_quarterly_forecasts:
        qtr = ms_quarterly_forecasts.get("quarterly", {})
        periods = qtr.get("periods", [])
        if periods:
            next_quarter_label = periods[-1]
    out["next_quarter_label"] = next_quarter_label
    # Short form e.g. 1Q26
    if next_quarter_label:
        import re as _rq
        m = _rq.search(r"(\d{4})\s*Q(\d)|Q(\d)\s*(\d{4})", next_quarter_label, _rq.I)
        if m:
            yr = (m.group(1) or m.group(4)) or ""
            qn = (m.group(2) or m.group(3)) or ""
            out["preview_quarter_short"] = f"{qn}Q{yr[-2:]}" if yr and qn else None
    if not out.get("preview_quarter_short"):
        out["preview_quarter_short"] = "1Q26"
    out["preview_quarter_label"] = f"Earnings Preview — {out.get('preview_quarter_short', '1Q26')}"
    # Prior quarter and prior-year same quarter labels (for table headers and consistency)
    if next_quarter_label:
        import re as _r2
        m = _r2.search(r"(\d{4})\s*Q(\d)|Q(\d)\s*(\d{4})", next_quarter_label, _r2.I)
        if m:
            next_yr = int(m.group(1) or m.group(4))
            next_q = int(m.group(2) or m.group(3))
            prev_q = (next_q - 1) if next_q > 1 else 4
            prev_yr = next_yr if next_q > 1 else next_yr - 1
            same_yr = next_yr - 1
            out["prior_quarter_label"] = f"{prev_yr} Q{prev_q}"
            out["prior_quarter_short"] = f"{prev_q}Q{str(prev_yr)[-2:]}"
            out["prior_year_same_quarter_label"] = f"{same_yr} Q{next_q}"
            out["prior_year_same_quarter_short"] = f"{next_q}Q{str(same_yr)[-2:]}"

    # Next-quarter consensus revenue (Source: /finances/ quarterly)
    next_quarter_consensus_revenue = None
    next_quarter_consensus_eps = None
    if ms_quarterly_forecasts and next_quarter_label:
        qtr = ms_quarterly_forecasts.get("quarterly", {})
        periods = qtr.get("periods", [])
        net_sales = qtr.get("net_sales", [])
        for i, p in enumerate(periods):
            if p == next_quarter_label or (str(next_quarter_label).replace(" ", "") in str(p).replace(" ", "")):
                next_quarter_consensus_revenue = net_sales[i] if i < len(net_sales) else None
                break
        if next_quarter_consensus_revenue is None and periods and net_sales:
            next_quarter_consensus_revenue = net_sales[-1] if net_sales else None
    # Next-quarter EPS: from consensus_estimates (quarterly) if available
    if consensus and next_quarter_label:
        for est in consensus:
            if est.period_label and (next_quarter_label in est.period_label or est.period_label in (next_quarter_label or "")):
                next_quarter_consensus_eps = est.eps
                break
    out["next_quarter_consensus_revenue"] = next_quarter_consensus_revenue
    out["next_quarter_consensus_eps"] = next_quarter_consensus_eps

    # Calendar Quarterly results table: which metrics have data for next/prior/same-q-last-year
    qr = (ms_calendar_events or {}).get("quarterly_results", {}) or {}
    quarters = qr.get("quarters", [])
    rows_qr = qr.get("rows", [])
    if quarters and rows_qr and next_quarter_label:
        import re as _re2
        def _norm(q):
            return (q or "").replace(" ", " ").strip().upper()
        next_norm = _norm(next_quarter_label)
        idx_next = next(i for i, q in enumerate(quarters) if _norm(q) == next_norm) if any(_norm(q) == next_norm for q in quarters) else None
        # Prior quarter: Q4 for Q1, Q1 for Q2, etc.
        m = _re2.search(r"(\d{4})\s*Q(\d)|Q(\d)\s*(\d{4})", next_quarter_label or "", _re2.I)
        prev_yr, prev_q = None, None
        next_yr, next_q_num = None, None
        if m:
            next_yr = int(m.group(1) or m.group(4))
            next_q_num = int(m.group(2) or m.group(3))
            prev_q = (next_q_num - 1) if next_q_num > 1 else 4
            prev_yr = next_yr if next_q_num > 1 else next_yr - 1
        def _quarter_matches(period: str, y: int, qn: int) -> bool:
            n = _norm(period)
            return (f"{y} Q{qn}" in n or f"Q{qn} {y}" in n)
        idx_prior = next((i for i, p in enumerate(quarters) if _quarter_matches(p, prev_yr, prev_q)), None) if prev_yr is not None else None
        same_yr = (next_yr - 1) if next_yr is not None else None
        idx_same_ly = next((i for i, p in enumerate(quarters) if _quarter_matches(p, same_yr, next_q_num)), None) if same_yr is not None and next_q_num is not None else None

        calendar_next = {}
        calendar_prior = {}
        calendar_same_ly = {}
        available = []
        for r in rows_qr:
            key = r.get("metric_key", "")
            if key == "announcement_date":
                continue
            by_q = r.get("by_quarter", [])
            v_next = None
            if idx_next is not None and idx_next < len(by_q):
                cell = by_q[idx_next]
                v_next = cell.get("forecast") if cell.get("forecast") is not None else cell.get("released")
            if v_next is not None:
                calendar_next[key] = v_next
                available.append(key)
            if idx_prior is not None and idx_prior < len(by_q):
                vp = by_q[idx_prior].get("released")
                if vp is not None:
                    calendar_prior[key] = vp
            if idx_same_ly is not None and idx_same_ly < len(by_q):
                vs = by_q[idx_same_ly].get("released")
                if vs is not None:
                    calendar_same_ly[key] = vs
        out["calendar_quarterly_available_metrics"] = available
        out["calendar_next_quarter"] = calendar_next
        out["calendar_prior_quarter_released"] = calendar_prior
        out["calendar_same_q_prior_yr_released"] = calendar_same_ly
        out["calendar_quarterly_rows"] = [{"metric_key": r.get("metric_key"), "metric_label": r.get("metric_label"), "unit": r.get("unit")} for r in rows_qr if r.get("metric_key") != "announcement_date"]
        # Override consensus from calendar when available (calendar is from same page as earnings date)
        if calendar_next.get("net_sales") is not None:
            out["next_quarter_consensus_revenue"] = calendar_next["net_sales"]
        if calendar_next.get("eps") is not None:
            out["next_quarter_consensus_eps"] = calendar_next["eps"]

    # Next-quarter YoY revenue / NI growth: prior-year same quarter from quarterly_actuals
    # e.g. next = 2026 Q1 → prior year = 2025 Q1; growth = (Q1'26 - Q1'25)/Q1'25 (we may not have Q1'26 actuals, so we use consensus for context or leave blank)
    next_quarter_yoy_revenue_growth_pct = None
    next_quarter_yoy_ni_growth_pct = None
    if next_quarter_label and quarterly:
        import re
        m = re.search(r"(\d{4})\s*Q(\d)|Q(\d)\s*(\d{4})", str(next_quarter_label), re.I)
        if m:
            yr = int(m.group(1) or m.group(4))
            q = int(m.group(2) or m.group(3))
            prior_yr = yr - 1
            # Find prior year same quarter in quarterly_actuals
            prior_label = f"{prior_yr}Q{q}" if prior_yr >= 2000 else f"FY{prior_yr} Q{q}"
            curr_label = f"{yr}Q{q}"
            qmap = {fp.period_label.replace(" ", "").upper(): fp for fp in quarterly}
            pkey = prior_label.replace(" ", "").upper()
            ckey = curr_label.replace(" ", "").upper()
            for k, v in qmap.items():
                if f"Q{q}" in k and str(prior_yr) in k:
                    pkey = k
                    break
            prior_fp = qmap.get(pkey) or next((fp for fp in quarterly if str(prior_yr) in fp.period_label and f"Q{q}" in fp.period_label.upper()), None)
            curr_fp = qmap.get(ckey) or next((fp for fp in quarterly if str(yr) in fp.period_label and f"Q{q}" in fp.period_label.upper()), None)
            if prior_fp and prior_fp.revenue and prior_fp.revenue != 0:
                if curr_fp and curr_fp.revenue is not None:
                    next_quarter_yoy_revenue_growth_pct = round((curr_fp.revenue - prior_fp.revenue) / prior_fp.revenue * 100, 2)
                # For "key context" show prior-year growth from derived if available
                if next_quarter_yoy_revenue_growth_pct is None and derived and getattr(derived, "quarterly_revenue_growth", None):
                    for g in derived.quarterly_revenue_growth:
                        if str(prior_yr) in g.get("period", "") and f"Q{q}" in g.get("period", "").upper():
                            next_quarter_yoy_revenue_growth_pct = g.get("pct")
                            break
            if prior_fp and prior_fp.net_income and prior_fp.net_income != 0 and curr_fp and curr_fp.net_income is not None:
                next_quarter_yoy_ni_growth_pct = round((curr_fp.net_income - prior_fp.net_income) / abs(prior_fp.net_income) * 100, 2)
    out["next_quarter_yoy_revenue_growth_pct"] = next_quarter_yoy_revenue_growth_pct
    out["next_quarter_yoy_ni_growth_pct"] = next_quarter_yoy_ni_growth_pct

    # Prior quarter (e.g. Q4 25 for Q1 26) and same quarter last year (Q1 25) — for Q1 Preview table
    import re as _re
    prior_q_revenue = None
    prior_q_ni = None
    same_q_prior_yr_revenue = None
    same_q_prior_yr_ni = None
    if next_quarter_label and quarterly:
        m = _re.search(r"(\d{4})\s*Q(\d)|Q(\d)\s*(\d{4})", str(next_quarter_label), _re.I)
        if m:
            yr = int(m.group(1) or m.group(4))
            q = int(m.group(2) or m.group(3))
            prior_yr = yr - 1
            prev_q = (q - 1) if q > 1 else 4
            prev_yr = yr if q > 1 else yr - 1
            qmap = {fp.period_label.replace(" ", "").upper(): fp for fp in quarterly}
            for k, v in qmap.items():
                if f"Q{q}" in k and str(prior_yr) in k:
                    same_q_prior_yr_revenue = v.revenue
                    same_q_prior_yr_ni = v.net_income
                    break
            for k, v in qmap.items():
                if f"Q{prev_q}" in k and str(prev_yr) in k:
                    prior_q_revenue = v.revenue
                    prior_q_ni = v.net_income
                    break
    out["prior_quarter_actual_revenue"] = prior_q_revenue
    out["prior_quarter_actual_ni"] = prior_q_ni
    out["same_quarter_prior_year_revenue"] = same_q_prior_yr_revenue
    out["same_quarter_prior_year_ni"] = same_q_prior_yr_ni

    # QoQ / YoY for table: only when we have both consensus and comparison value (from calendar or quarterly actuals)
    cal_prior = out.get("calendar_prior_quarter_released") or {}
    cal_same_ly = out.get("calendar_same_q_prior_yr_released") or {}
    cal_next = out.get("calendar_next_quarter") or {}
    rev_cons = out.get("next_quarter_consensus_revenue")
    prior_rev = cal_prior.get("net_sales") or prior_q_revenue
    same_ly_rev = cal_same_ly.get("net_sales") or same_q_prior_yr_revenue
    if rev_cons is not None and prior_rev and prior_rev != 0:
        out["qoq_revenue_pct"] = round((rev_cons - prior_rev) / prior_rev * 100, 1)
    if rev_cons is not None and same_ly_rev and same_ly_rev != 0:
        out["yoy_revenue_pct_table"] = round((rev_cons - same_ly_rev) / same_ly_rev * 100, 1)
    ni_cons = cal_next.get("net_income")
    prior_ni = cal_prior.get("net_income") or prior_q_ni
    same_ly_ni = cal_same_ly.get("net_income") or same_q_prior_yr_ni
    if ni_cons is not None and prior_ni and prior_ni != 0:
        out["qoq_ni_pct"] = round((ni_cons - prior_ni) / prior_ni * 100, 1)
    if ni_cons is not None and same_ly_ni and same_ly_ni != 0:
        out["yoy_ni_pct_table"] = round((ni_cons - same_ly_ni) / abs(same_ly_ni) * 100, 1)

    # EBITDA QoQ / YoY
    ebitda_cons = cal_next.get("ebitda")
    prior_ebitda = cal_prior.get("ebitda")
    same_ly_ebitda = cal_same_ly.get("ebitda")
    if ebitda_cons is not None and prior_ebitda and prior_ebitda != 0:
        out["qoq_ebitda_pct"] = round((ebitda_cons - prior_ebitda) / abs(prior_ebitda) * 100, 1)
    if ebitda_cons is not None and same_ly_ebitda and same_ly_ebitda != 0:
        out["yoy_ebitda_pct_table"] = round((ebitda_cons - same_ly_ebitda) / abs(same_ly_ebitda) * 100, 1)

    # EPS QoQ / YoY when preview consensus and comparison actuals exist
    eps_cons = cal_next.get("eps")
    prior_eps = cal_prior.get("eps")
    same_ly_eps = cal_same_ly.get("eps")
    if eps_cons is not None and prior_eps is not None and prior_eps != 0:
        out["qoq_eps_pct"] = round((eps_cons / prior_eps - 1) * 100, 1)
    if eps_cons is not None and same_ly_eps is not None and same_ly_eps != 0:
        out["yoy_eps_pct_table"] = round((eps_cons / same_ly_eps - 1) * 100, 1)

    # Do not derive quarterly NI/EPS from annual consensus; use only calendar quarterly results.

    # Beat/miss: only count quarters with BOTH released AND forecast (valid comparison)
    qr = (ms_calendar_events or {}).get("quarterly_results", {}) or {}
    qr_quarters = qr.get("quarters", [])
    qr_rows = qr.get("rows", [])
    if qr_quarters and qr_rows:
        rev_surprise = []
        eps_surprise = []
        ni_surprise = []
        for r in qr_rows:
            key = r.get("metric_key")
            by_q = r.get("by_quarter", [])
            for i, cell in enumerate(by_q):
                if i >= len(qr_quarters):
                    break
                released = cell.get("released")
                forecast = cell.get("forecast")
                spread = cell.get("spread_pct")
                if released is None or forecast is None:
                    continue
                pct = spread
                if pct is None and forecast and forecast != 0:
                    pct = round((released - forecast) / abs(forecast) * 100, 1)
                if pct is None:
                    continue
                period = qr_quarters[i]
                entry = {"period": period, "surprise_pct": pct}
                if key == "net_sales":
                    rev_surprise.append(entry)
                elif key == "eps":
                    eps_surprise.append(entry)
                elif key == "net_income":
                    ni_surprise.append(entry)
        if rev_surprise:
            out["revenue_surprise_history"] = rev_surprise
            pcts = [e["surprise_pct"] for e in rev_surprise]
            out["avg_revenue_surprise_pct"] = round(sum(pcts) / len(pcts), 1) if pcts else None
            out["consecutive_revenue_beats"] = len([p for p in pcts if (p or 0) >= 0])
        if eps_surprise:
            out["eps_surprise_history"] = eps_surprise
            out["avg_eps_surprise_pct"] = round(sum(e["surprise_pct"] for e in eps_surprise) / len(eps_surprise), 1) if eps_surprise else None
        if ni_surprise:
            out["ni_surprise_history"] = ni_surprise
            out["avg_ni_surprise_pct"] = round(sum(e["surprise_pct"] for e in ni_surprise) / len(ni_surprise), 1) if ni_surprise else None

    # Keep avg 4Q growth for appendix/context
    if derived:
        out["avg_4q_revenue_growth"] = getattr(derived, "avg_4q_revenue_growth", None)
        out["avg_4q_ni_growth"] = getattr(derived, "avg_4q_ni_growth", None)

    return out


def get_memo_computed_for_preview(
    *,
    company,
    quote,
    quarterly: list,
    consensus: list,
    consensus_summary: dict | None,
    ms_annual_forecasts: dict | None,
    ms_quarterly_forecasts: dict | None,
    ms_eps_dividend_forecasts: dict | None,
    ms_calendar_events: dict | None,
    derived,
) -> dict:
    """Compute memo fields for use before full payload (e.g. Investment View fact pack)."""
    return _compute_memo(
        company=company,
        quote=quote,
        quarterly=quarterly,
        consensus=consensus,
        consensus_summary=consensus_summary,
        ms_annual_forecasts=ms_annual_forecasts,
        ms_quarterly_forecasts=ms_quarterly_forecasts,
        ms_eps_dividend_forecasts=ms_eps_dividend_forecasts,
        ms_calendar_events=ms_calendar_events,
        yahoo_earnings_date=None,
        derived=derived,
    )


def run(
    *,
    run_id:                     str,
    company:                    CompanyMaster,
    quote:                      QuoteSnapshot | None,
    quarterly:                  list[FinancialPeriod],
    annual:                     list[FinancialPeriod],
    consensus:                  list[FinancialPeriod],
    consensus_summary:          dict | None = None,
    ms_lineage:                 dict | MSLineage | None = None,
    ms_summary:                 dict | None = None,
    ms_annual_forecasts:        dict | None = None,
    ms_quarterly_forecasts:     dict | None = None,
    ms_eps_dividend_forecasts:  dict | None = None,
    ms_income_statement_actuals: dict | None = None,
    ms_valuation_multiples:     dict | None = None,
    ms_calendar_events:         dict | None = None,
    ms_quarterly_results_table: dict | None = None,
    derived:                    DerivedMetrics | None,
    news_items:                 list[NewsItem],
    news_summary:               NewsSummary | None,
    duplicate_screening_log:    list[dict] = (),
    step_log:                   list[dict],
    recent_context_query_log:   list[dict] = (),
    recent_context_candidate_count: int = 0,
    recent_context_valid_count: int = 0,
    recent_context_rejected_reasons: list[str] = (),
    candidate_valid_basic:      bool = False,
    candidate_has_date_before_enrichment: int = 0,
    candidate_has_extracted_fact: int = 0,
    final_article_valid_count:  int = 0,
    date_parse_attempted:       int = 0,
    date_parse_source:          list[str] = (),
    date_parse_success:        int = 0,
    candidates_rejected_for_missing_date: int = 0,
    candidates_recovered_after_article_fetch: int = 0,
    recent_context_enrichment_log: list[dict] = (),
    rejected_candidates_top_10: list[dict] = (),
    recent_context_articles_qa: list[dict] = (),
    cross_company_contamination_detected: bool = False,
    identical_to_previous_ticker_payload: bool = False,
    yahoo_earnings_date: str | None = None,
) -> StepResult:
    with StepTimer() as t:
        warnings: list[str] = []
        if quote is None:       warnings.append("quote missing")
        if not quarterly:       warnings.append("no quarterly financials")
        if not consensus:       warnings.append("no consensus estimates")
        # Use latest DB status (fetch_consensus may have set source_redirect this run)
        row_ms = load_company(company.ticker) if getattr(company, "ticker", None) else None
        ms_avail = get_marketscreener_availability(row_ms or company.model_dump())
        if get_effective_marketscreener_slug(company.model_dump()) and consensus_summary is None:
            if ms_avail == MS_AVAILABILITY_SOURCE_REDIRECT:
                warnings.append("MarketScreener consensus unavailable (source redirect)")
            else:
                warnings.append("no consensus summary (MarketScreener)")
        if not news_items:      warnings.append("no news")

        ms_fingerprint = ""
        # ── Lineage and entity validation (suppress all MS data on failure) ───
        lineage = ms_lineage if isinstance(ms_lineage, MSLineage) else _ms_lineage_from_dict(ms_lineage)
        entity_ok = _validate_ms_entity(
            getattr(company, "ticker", "") or "",
            lineage,
            ms_avail,
            company_name=getattr(company, "company_name", "") or "",
        )
        # Use MS data from this run when lineage matches but DB has stale "needs_review"
        # (e.g. slug was discovered via resolve_slug_from_search; ISIN validation failed earlier)
        company_ticker = (getattr(company, "ticker", "") or "").strip()
        if (
            not entity_ok
            and ms_avail == MS_AVAILABILITY_WRONG_ENTITY
            and lineage
            and (lineage.source_ticker or "").strip() == company_ticker
            and (consensus_summary or ms_annual_forecasts or ms_calendar_events)
        ):
            entity_ok = True
        # Suppression reasons for QA and rendering (better blank than wrong)
        had_ms_data = bool(
            consensus_summary or ms_annual_forecasts or ms_calendar_events
            or ms_quarterly_forecasts or ms_eps_dividend_forecasts
            or ms_valuation_multiples or ms_quarterly_results_table or ms_summary
        )
        ms_suppressed_missing = not entity_ok and not lineage
        ms_suppressed_entity_mismatch = not entity_ok
        ms_suppressed_contamination = cross_company_contamination_detected
        reused_default_detected = (not entity_ok and had_ms_data) or cross_company_contamination_detected

        if not entity_ok or cross_company_contamination_detected:
            # Hard fail-safe: do not preserve any previous MS values
            consensus_summary = None
            ms_summary = None
            ms_annual_forecasts = None
            ms_quarterly_forecasts = None
            ms_eps_dividend_forecasts = None
            ms_income_statement_actuals = None
            ms_valuation_multiples = None
            ms_calendar_events = None
            ms_quarterly_results_table = None
        else:
            # Explicit rebuild: no shared mutable refs; each section is a fresh copy
            ms_summary = _rebuild_ms_section(ms_summary)
            ms_annual_forecasts = _rebuild_ms_section(ms_annual_forecasts)
            ms_quarterly_forecasts = _rebuild_ms_section(ms_quarterly_forecasts)
            ms_eps_dividend_forecasts = _rebuild_ms_section(ms_eps_dividend_forecasts)
            ms_income_statement_actuals = _rebuild_ms_section(ms_income_statement_actuals)
            ms_valuation_multiples = _rebuild_ms_section(ms_valuation_multiples)
            ms_calendar_events = _rebuild_ms_section(ms_calendar_events)
            ms_quarterly_results_table = _rebuild_ms_section(ms_quarterly_results_table)
            consensus_summary = _rebuild_ms_section(consensus_summary) if consensus_summary else None

            # Automatic fingerprint check: flag identical MS payloads across unrelated tickers
            ms_fingerprint = compute_fingerprint(
                consensus_summary=consensus_summary,
                ms_summary=ms_summary,
                ms_annual_forecasts=ms_annual_forecasts,
                ms_quarterly_forecasts=ms_quarterly_forecasts,
                ms_eps_dividend_forecasts=ms_eps_dividend_forecasts,
                ms_income_statement_actuals=ms_income_statement_actuals,
                ms_valuation_multiples=ms_valuation_multiples,
                ms_calendar_events=ms_calendar_events,
                ms_quarterly_results_table=ms_quarterly_results_table,
            )
            if ms_fingerprint:
                fp_cross, fp_identical = check_fingerprint(
                    (getattr(company, "ticker", "") or "").strip(), ms_fingerprint
                )
                if fp_cross:
                    cross_company_contamination_detected = True
                    ms_suppressed_contamination = True
                    reused_default_detected = True
                    # No stale carry-forward: null every MS section
                    consensus_summary = None
                    ms_summary = None
                    ms_annual_forecasts = None
                    ms_quarterly_forecasts = None
                    ms_eps_dividend_forecasts = None
                    ms_income_statement_actuals = None
                    ms_valuation_multiples = None
                    ms_calendar_events = None
                    ms_quarterly_results_table = None
                    ms_fingerprint = ""
                else:
                    identical_to_previous_ticker_payload = fp_identical

        payload_source_ticker = (lineage.source_ticker if lineage else "") or (getattr(company, "ticker", "") or "")
        payload_entity_match = entity_ok
        if not entity_ok:
            ms_fingerprint = ""

        # Final defensive null: ensure no MS data survives when entity/contamination invalid
        if not payload_entity_match or cross_company_contamination_detected:
            consensus_summary = None
            ms_summary = None
            ms_annual_forecasts = None
            ms_quarterly_forecasts = None
            ms_eps_dividend_forecasts = None
            ms_income_statement_actuals = None
            ms_valuation_multiples = None
            ms_calendar_events = None
            ms_quarterly_results_table = None
            ms_fingerprint = ""

        # Price sanity: suppress consensus_summary if MS price diverges >3x from Yahoo
        if consensus_summary and quote and getattr(quote, "price", None):
            _ms_cl = consensus_summary.get("last_close_price") if isinstance(consensus_summary, dict) else None
            if _ms_cl and quote.price > 0:
                _r = _ms_cl / quote.price
                if _r < 0.3 or _r > 3.0:
                    log.warning("Suppressed consensus_summary: price divergence %.1fx for %s", _r, getattr(company, "ticker", "?"))
                    consensus_summary = None

        # ── Memo-specific computed (for front-page memo) ─────────────────────
        memo_computed = _compute_memo(
            company=company,
            quote=quote,
            quarterly=quarterly,
            consensus=consensus,
            consensus_summary=consensus_summary,
            ms_annual_forecasts=ms_annual_forecasts,
            ms_quarterly_forecasts=ms_quarterly_forecasts,
            ms_eps_dividend_forecasts=ms_eps_dividend_forecasts,
            ms_calendar_events=ms_calendar_events,
            yahoo_earnings_date=yahoo_earnings_date,
            derived=derived,
        )

        # Field-level provenance (do not mix MS and Yahoo silently)
        field_provenance: dict[str, dict] = {}
        if consensus_summary and consensus_summary.get("last_close_price") is not None:
            field_provenance["last_close"] = SourcedValue(
                value=consensus_summary["last_close_price"],
                source="marketscreener",
                source_page="/consensus/",
                frequency="point",
                period_label="",
                status="ok",
                fallback_used=False,
            ).model_dump()
        if quote and getattr(quote, "price", None) is not None:
            field_provenance["current_price"] = SourcedValue(
                value=quote.price,
                source="yahoo",
                source_page="quote",
                frequency="point",
                period_label="",
                status="ok",
                fallback_used=consensus_summary is None,
                warning="Yahoo fallback" if consensus_summary is None else "",
            ).model_dump()
        if memo_computed.get("next_quarter_consensus_revenue") is not None:
            field_provenance["next_quarter_revenue"] = SourcedValue(
                value=memo_computed["next_quarter_consensus_revenue"],
                source="marketscreener",
                source_page="/calendar/",
                frequency="quarterly",
                period_label=memo_computed.get("next_quarter_label") or "",
                status="ok",
                fallback_used=False,
            ).model_dump()
        if memo_computed.get("next_quarter_consensus_eps") is not None:
            field_provenance["next_quarter_eps"] = SourcedValue(
                value=memo_computed["next_quarter_consensus_eps"],
                source="marketscreener",
                source_page="/calendar/",
                frequency="quarterly",
                period_label=memo_computed.get("next_quarter_label") or "",
                status="ok",
                fallback_used=False,
            ).model_dump()

        # Suppress appendix sections when data is thin or when MS data is invalid (better blank than wrong)
        if not payload_entity_match or cross_company_contamination_detected:
            appendix_sections = ["audit"]
        else:
            appendix_sections = ["annual_forecasts", "quarterly_detail", "eps_dividend", "valuation", "audit"]
            if not ms_annual_forecasts or not (ms_annual_forecasts.get("annual") or {}).get("periods"):
                appendix_sections = [s for s in appendix_sections if s != "annual_forecasts"]
            qr = (ms_calendar_events or {}).get("quarterly_results", {}) or {}
            if not ms_quarterly_results_table and not qr.get("rows"):
                appendix_sections = [s for s in appendix_sections if s != "quarterly_detail"]
            if not ms_eps_dividend_forecasts or not (ms_eps_dividend_forecasts.get("periods") and ms_eps_dividend_forecasts.get("eps")):
                appendix_sections = [s for s in appendix_sections if s != "eps_dividend"]
            if not ms_valuation_multiples or not ms_valuation_multiples.get("periods"):
                appendix_sections = [s for s in appendix_sections if s != "valuation"]

        payload = ReportPayload(
            run_id=run_id,
            generated_at=datetime.now(timezone.utc),
            mode="preview",
            company=company,
            quote=quote,
            quarterly_actuals=list(quarterly),
            annual_actuals=list(annual),
            consensus_estimates=list(consensus),
            consensus_summary=consensus_summary,
            ms_lineage=lineage,
            ms_summary=ms_summary,
            ms_annual_forecasts=ms_annual_forecasts,
            ms_quarterly_forecasts=ms_quarterly_forecasts,
            ms_eps_dividend_forecasts=ms_eps_dividend_forecasts,
            ms_income_statement_actuals=ms_income_statement_actuals,
            ms_valuation_multiples=ms_valuation_multiples,
            ms_calendar_events=ms_calendar_events,
            ms_quarterly_results_table=ms_quarterly_results_table,
            derived=derived,
            news_items=list(news_items),
            news_summary=news_summary,
            step_results=list(step_log),
            duplicate_screening_log=list(duplicate_screening_log),
            recent_context_query_log=list(recent_context_query_log),
            recent_context_candidate_count=recent_context_candidate_count,
            recent_context_valid_count=recent_context_valid_count,
            recent_context_rejected_reasons=list(recent_context_rejected_reasons),
            candidate_valid_basic=candidate_valid_basic,
            candidate_has_date_before_enrichment=candidate_has_date_before_enrichment,
            candidate_has_extracted_fact=candidate_has_extracted_fact,
            final_article_valid_count=final_article_valid_count,
            date_parse_attempted=date_parse_attempted,
            date_parse_source=list(date_parse_source) if date_parse_source else [],
            date_parse_success=date_parse_success,
            candidates_rejected_for_missing_date=candidates_rejected_for_missing_date,
            candidates_recovered_after_article_fetch=candidates_recovered_after_article_fetch,
            recent_context_enrichment_log=list(recent_context_enrichment_log or []),
            rejected_candidates_top_10=list(rejected_candidates_top_10 or []),
            recent_context_articles_qa=list(recent_context_articles_qa or []),
            memo_computed=memo_computed,
            field_provenance=dict(field_provenance) if field_provenance else None,
            appendix_sections=list(appendix_sections),
            marketscreener_availability=ms_avail,
            payload_source_ticker=payload_source_ticker,
            payload_entity_match=payload_entity_match,
            cross_company_contamination_detected=cross_company_contamination_detected,
            identical_to_previous_ticker_payload=identical_to_previous_ticker_payload,
            ms_payload_fingerprint=ms_fingerprint,
            ms_section_suppressed_due_to_missing_current_data=ms_suppressed_missing,
            ms_section_suppressed_due_to_entity_mismatch=ms_suppressed_entity_mismatch,
            ms_section_suppressed_due_to_contamination=ms_suppressed_contamination,
            reused_default_payload_detected=reused_default_detected,
            has_consensus=bool(consensus),
            has_news=bool(news_items),
            warnings=list(warnings),
        )

        filled = sum([
            quote is not None,
            bool(quarterly),
            bool(annual),
            bool(consensus),
            derived is not None,
            bool(news_items),
            news_summary is not None,
        ])

        return StepResult(
            step_name=STEP,
            status=Status.SUCCESS if not warnings else Status.PARTIAL,
            source="assembled",
            message=(
                f"Payload: {filled}/7 sections"
                + (f" | gaps: {', '.join(warnings)}" if warnings else "")
            ),
            data=payload, elapsed_seconds=t.elapsed,
        )
