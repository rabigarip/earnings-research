"""
LLM-driven executive summary for the Jabal deck.

The auto-templated paragraph on slide 2 (the prior approach) was generic
because it pulled the same boilerplate phrases for every ticker. With
Bloomberg consensus + IR-PDF + free-source backstops feeding the canonical
store, we now have enough structured data per ticker to generate a real
analyst-grade paragraph.

This module builds a rich context dict per ticker, calls Gemini with a
disciplined prompt, and returns:
    {
        thesis_paragraph: str   (4-6 sentences),
        catalysts:        list[str],   (3 items)
        risks:            list[str],   (3 items)
        watch_list:       list[str],   (3 items, framed as questions)
        provider:         "gemini",
        model:            "<actual model name>",
        as_of:            ISO timestamp,
    }

Cached to disk by (ticker, context_hash) so a re-render is free unless
underlying data changes. Falls back gracefully to the prior template when:
  - GEMINI_API_KEY is not set
  - the model call fails / times out
  - returned text fails JSON validation
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from src.services.canonical_store import (
    get_all_fields, get_observations_by_provider,
)


log = logging.getLogger(__name__)

_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "cache" / "llm_summary"


# ── Context builder ──────────────────────────────────────────────

def _pretty_rating_label(raw) -> Optional[str]:
    """Normalise rating enums like 'STRONG_BUY' -> 'Strong Buy' so the LLM
    prompt and the slide labels agree. Returns None for empty input."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    return " ".join(p.capitalize() for p in s.replace("_", " ").replace("-", " ").split())


def _fmt_num(v: Any, *, suffix: str = "") -> str:
    if v is None:
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if abs(f) >= 1e12: return f"{f/1e12:.2f}T{suffix}"
    if abs(f) >= 1e9:  return f"{f/1e9:.1f}B{suffix}"
    if abs(f) >= 1e6:  return f"{f/1e6:.0f}M{suffix}"
    if abs(f) < 1:     return f"{f:.3f}{suffix}"
    return f"{f:,.2f}{suffix}"


def build_context(ticker: str) -> dict:
    """Pull every datum the LLM might reasonably need from canonical_store
    + observation backfills, into a single flat-ish dict. The prompt
    references this verbatim — keys are deliberately readable."""
    cv = get_all_fields(ticker)
    investing_obs   = get_observations_by_provider(ticker, "investing")
    commodities_obs = get_observations_by_provider(ticker, "commodities")
    macro_obs       = get_observations_by_provider(ticker, "macro")
    bloomberg_obs   = get_observations_by_provider(ticker, "bloomberg")

    def _val(field):
        c = cv.get(field)
        return c.value if c else None

    profile = _val("company_profile") or {}
    if not isinstance(profile, dict):
        profile = {}

    target = _val("target_price") or {}
    if not isinstance(target, dict):
        target = {}

    rating = _val("rating_split") or {}
    if not isinstance(rating, dict):
        rating = {}

    val_fwd = _val("valuation_forward") or {}
    if not isinstance(val_fwd, dict):
        val_fwd = {}

    val_hist = _val("valuation_historical") or {}
    if not isinstance(val_hist, dict):
        val_hist = {}

    hist_prices = _val("historical_prices") or {}
    if not isinstance(hist_prices, dict):
        hist_prices = {}

    # P/E recent vs 5y average
    pe_recent = pe_avg = None
    pe_list = val_hist.get("pe") if isinstance(val_hist, dict) else []
    if isinstance(pe_list, list):
        nums = [v for v in pe_list if isinstance(v, (int, float))]
        if nums:
            pe_recent = nums[-1]
            pe_avg = sum(nums) / len(nums)

    # Surprise history (from Investing) — most useful for the "track record"
    # angle. CRITICAL: count beats ONLY across rows with a real surprise_pct
    # value. Without this guard, names where Investing never published an
    # epsForecast (BKMB-class thinly-covered banks) produce "0 of last 4
    # quarters above consensus EPS" — a false claim, not a missing-data note.
    raw_surprise = ((investing_obs.get("income_statement_quarterly") or {})
                     .get("surprise_history", []))
    surprise = [r for r in raw_surprise if isinstance(r, dict)]
    valid_surprises = [r for r in surprise
                         if isinstance(r.get("eps_surprise_pct"), (int, float))]
    if len(valid_surprises) >= 3:
        beats = sum(1 for r in valid_surprises[:4] if r["eps_surprise_pct"] > 0)
        n_recent = min(4, len(valid_surprises))
        last_surprise = valid_surprises[0]
    else:
        # Insufficient surprise data — pass empty values so the prompt's
        # template doesn't generate "0 of last 4 above consensus".
        beats = None
        n_recent = 0
        last_surprise = None

    # Broker actions (from MS canonical)
    ba_val = _val("broker_actions") or {}
    broker_items = ba_val.get("items", []) if isinstance(ba_val, dict) else []

    # Commodity context (from commodities provider observations)
    commodities = ((commodities_obs.get("company_profile") or {})
                   .get("industry_commodities", {}))

    # Macro context
    macro = macro_obs.get("company_profile") or {}

    current_price = _val("current_price")
    target_mean = target.get("mean")
    upside_pct = None
    try:
        if target_mean and current_price and float(current_price) > 0:
            upside_pct = (float(target_mean) / float(current_price) - 1.0) * 100
    except (TypeError, ValueError):
        pass

    # MarketScreener reports market_cap in millions of local currency; the
    # other providers report raw. Normalize to raw before passing to prompt.
    raw_mcap = _val("market_cap")
    mcap_source = (cv.get("market_cap").canonical_source
                    if cv.get("market_cap") else None)
    if mcap_source == "marketscreener" and isinstance(raw_mcap, (int, float)):
        raw_mcap = raw_mcap * 1_000_000

    # Peer / industry P/E median — the comparator the VALUATION pill
    # should cite ("vs the GCC bank peer median near 9.0x"). Computed from
    # the SAME registry peer set the slide-3 table uses, so the deck and
    # the prose agree. Best-effort: thin coverage / network failure leaves
    # it None and the prompt degrades to the own-history comparator rather
    # than falling back to the bare template.
    peer_pe_median = None
    try:
        from src.services.ticker_registry import registry_peer_set
        from src.services.fetch_peers import fetch_peer_rows
        import statistics as _stats
        _peers = registry_peer_set(ticker) or []
        if _peers:
            _rows = fetch_peer_rows(_peers) or []
            _pes = [r.get("pe") for r in _rows
                    if isinstance(r.get("pe"), (int, float)) and r.get("pe") > 0]
            if len(_pes) >= 3:
                peer_pe_median = round(_stats.median(_pes), 1)
    except Exception:
        peer_pe_median = None

    # Normalise FY year labels (strip an existing "FY" prefix so the prompt
    # doesn't end up with "FYFY2026").
    def _norm_fy(v):
        s = str(v or "").strip()
        if s.upper().startswith("FY"):
            s = s[2:].lstrip()
        return s

    return {
        "peer_pe_median": peer_pe_median,
        "ticker": ticker,
        "company_name": profile.get("name") or ticker,
        "sector": profile.get("sector") or "—",
        "industry": profile.get("industry") or "—",
        "country": profile.get("country") or "—",
        "currency": profile.get("currency") or "",
        # Live snapshot
        "current_price": current_price,
        "market_cap": raw_mcap,
        "dividend_yield_pct": _val("dividend_yield"),
        # Bloomberg / consensus (canonical source where present)
        "consensus_source": (cv.get("valuation_forward").canonical_source
                              if cv.get("valuation_forward") else None),
        "next_q_period": val_fwd.get("next_q_period"),
        "next_q_report_date": val_fwd.get("next_q_report_date"),
        "eps_next_q": val_fwd.get("eps_next_q"),
        "revenue_next_q": val_fwd.get("revenue_next_q"),
        "fy1_year": _norm_fy(val_fwd.get("fy1_year")),
        "eps_fy1": val_fwd.get("eps_fy1"),
        "revenue_fy1": val_fwd.get("revenue_fy1"),
        "fy2_year": _norm_fy(val_fwd.get("fy2_year")),
        "eps_fy2": val_fwd.get("eps_fy2"),
        "revenue_fy2": val_fwd.get("revenue_fy2"),
        # Target + rating
        "target_mean": target_mean,
        "target_high": target.get("high"),
        "target_low":  target.get("low"),
        "n_analysts":  rating.get("total") or target.get("n_analysts"),
        "rating_consensus": _pretty_rating_label(rating.get("consensus")),
        "buy_count":  rating.get("buy"),
        "hold_count": rating.get("hold"),
        "sell_count": rating.get("sell"),
        "upside_pct": upside_pct,
        # Valuation history (MS multi-year P/E series, last + avg)
        "pe_recent": pe_recent,
        "pe_5y_avg": pe_avg,
        # Forward P/E from Investing — the value that drives slide 1's
        # FY P/E chip. Including it here so the LLM's thesis paragraph
        # can cite the SAME number ("12.4x FY26") that the rest of the
        # deck displays, not the MS historical reversion-mean.
        "pe_fy1": val_fwd.get("pe_fy1"),
        "pe_fy2": val_fwd.get("pe_fy2"),
        # Surprise track record
        "surprise_beats_last4": beats,
        "surprise_n_recent": n_recent,
        "last_surprise": last_surprise,
        # Broker actions (3 most recent)
        "recent_broker_actions": broker_items[:3],
        # Commodity / macro overlays (optional)
        "commodities": commodities,
        # Macro context for the LLM prompt. We prefer IMF WEO forecasts
        # over World Bank historical actuals because the deck is forward-
        # looking — feeding Gemini the WB 2024 actual when the print is
        # Q2 2026 anchors the thesis on stale conditions (e.g. Oman GDP
        # growth was 1.6% in 2024 but IMF expects 3.5% in 2026, and
        # inflation 0.6%→1.7%). World Bank stays as a fallback when IMF
        # has no series for the country.
        "macro": {
            # Preferred: IMF current-year forecast (matches/leads the
            # company's reporting cycle).
            "gdp_growth_pct":      macro.get("gdp_growth_fcst_pct")
                                       or macro.get("gdp_growth_pct"),
            "gdp_growth_year":     macro.get("gdp_growth_fcst_year")
                                       or macro.get("macro_year"),
            "gdp_growth_source":   ("IMF" if macro.get("gdp_growth_fcst_pct") is not None
                                     else "WB"),
            # Forward year (IMF next-year forecast) — useful for swing-factor sentences.
            "gdp_growth_fcst_next_pct":  macro.get("gdp_growth_fcst_next_pct"),
            "gdp_growth_fcst_next_year": macro.get("gdp_growth_fcst_next_year"),
            "inflation_pct":       macro.get("inflation_fcst_pct")
                                       or macro.get("inflation_pct"),
            "inflation_year":      macro.get("inflation_fcst_year")
                                       or macro.get("macro_year"),
            "inflation_source":    ("IMF" if macro.get("inflation_fcst_pct") is not None
                                     else "WB"),
            "inflation_fcst_next_pct":  macro.get("inflation_fcst_next_pct"),
            "inflation_fcst_next_year": macro.get("inflation_fcst_next_year"),
        },
    }


# ── Prompt ───────────────────────────────────────────────────────

_SYSTEM = (
    "You are a senior buy-side analyst at Jabal Asset Management writing the "
    "Investment Thesis paragraph of an institutional earnings-preview note. "
    "Your voice is declarative and direct, like a sell-side morning note: "
    "short sentences, no hedging adverbs, no marketing language. "
    "Every numeric claim MUST cite a value from the data block — never invent, "
    "round inconsistently, or extrapolate beyond what is supplied. "
    "Forbidden phrasing: 'we believe', 'appears to', 'suggests', 'could "
    "potentially', 'may benefit', 'remains well-positioned', 'attractive "
    "entry point', 'compelling valuation'. If a fact is not in the data, do "
    "not state it. If you have no number for a claim, drop the claim."
)


def _prompt(ctx: dict) -> str:
    # Pre-format numeric anchors so the model sees clean strings, not raw floats.
    cur = ctx.get("currency") or ""
    fmt_money = lambda v: f"{cur} {_fmt_num(v)}".strip()
    cur_price = fmt_money(ctx.get("current_price"))
    target = fmt_money(ctx.get("target_mean"))
    upside = (f"{ctx['upside_pct']:+.1f}%" if isinstance(ctx.get("upside_pct"), (int, float))
              else "—")
    next_q_eps = _fmt_num(ctx.get("eps_next_q"))
    next_q_rev = fmt_money(ctx.get("revenue_next_q"))
    fy1_eps = _fmt_num(ctx.get("eps_fy1"))
    fy1_rev = fmt_money(ctx.get("revenue_fy1"))

    surprise_lines = []
    for r in (ctx.get("last_surprise") and [ctx["last_surprise"]] or []):
        if r:
            s = r.get("eps_surprise_pct")
            if isinstance(s, (int, float)):
                surprise_lines.append(
                    f"Last quarter ({r.get('period','?')}): EPS "
                    f"{r.get('eps_actual')} vs estimate {r.get('eps_estimate')} "
                    f"({s:+.1f}%)."
                )
    beats_line = (
        f"{ctx['surprise_beats_last4']} of last {ctx['surprise_n_recent']} "
        f"quarters above consensus EPS."
    ) if ctx.get("surprise_n_recent") else ""

    pe_parts = []
    if isinstance(ctx.get("pe_fy1"), (int, float)):
        fy1 = ctx["pe_fy1"]
        yr = ctx.get("fy1_year") or ""
        pe_parts.append(f"Forward P/E {fy1:.1f}x (FY{yr})" if yr else f"Forward P/E {fy1:.1f}x")
    if isinstance(ctx.get("pe_recent"), (int, float)) and isinstance(ctx.get("pe_5y_avg"), (int, float)):
        avg = ctx["pe_5y_avg"]
        rec = ctx["pe_recent"]
        delta_pct = (rec / avg - 1.0) * 100 if avg else 0
        pe_parts.append(
            f"trailing P/E {rec:.1f}x vs 5-year average {avg:.1f}x "
            f"({delta_pct:+.0f}% relative)"
        )
    # Peer/industry comparator — the preferred anchor for the VALUATION pill.
    if isinstance(ctx.get("peer_pe_median"), (int, float)):
        ppm = ctx["peer_pe_median"]
        rec = ctx.get("pe_recent")
        if isinstance(rec, (int, float)) and ppm:
            prem = (rec / ppm - 1.0) * 100
            pe_parts.append(
                f"peer/industry median P/E {ppm:.1f}x "
                f"(subject {prem:+.0f}% vs peers)")
        else:
            pe_parts.append(f"peer/industry median P/E {ppm:.1f}x")
    pe_line = "; ".join(pe_parts) + "." if pe_parts else ""

    # Short commodity tags (e.g. "hh" for Henry Hub) get truncated by the
    # LLM into garbage strings — pre-expand to human-readable labels so
    # the prompt block reads cleanly and the model never invents a
    # short-form.
    _COMMODITY_LABELS = {
        "hh":          "Henry Hub natural gas",
        "wti":         "WTI crude",
        "brent":       "Brent crude",
        "urea":        "Urea",
        "ammonia":     "Ammonia",
        "copper":      "Copper",
        "gold":        "Gold",
        "coking_coal": "Coking coal",
        "iron_ore":    "Iron ore",
    }
    commodity_lines = []
    for tag, info in (ctx.get("commodities") or {}).items():
        if not isinstance(info, dict):
            continue
        val = info.get("value")
        yoy = info.get("yoy_pct")
        unit = info.get("unit", "")
        if val is None:
            continue
        label = _COMMODITY_LABELS.get(tag, tag.replace("_", " ").title())
        yoy_str = f" ({yoy:+.1f}% YoY)" if isinstance(yoy, (int, float)) else ""
        commodity_lines.append(f"{label}: {val} {unit}{yoy_str}")
    commodity_block = ("Commodity context: " + "; ".join(commodity_lines) + ".") if commodity_lines else ""

    # Macro block. Every figure is year + source stamped so Gemini cannot
    # confuse a 2024 World Bank actual with a 2026 IMF forecast. We lead
    # with the IMF forecast for the company's reporting year because the
    # deck is forward-looking — WB actuals fall back only when IMF lacks a
    # series for the country.
    macro = ctx.get("macro") or {}
    macro_parts = []
    if isinstance(macro.get("gdp_growth_pct"), (int, float)):
        src = macro.get("gdp_growth_source") or "IMF"
        yr  = macro.get("gdp_growth_year") or "?"
        macro_parts.append(f"GDP growth {macro['gdp_growth_pct']:.1f}% ({src} {yr})")
    if isinstance(macro.get("gdp_growth_fcst_next_pct"), (int, float)):
        ny = macro.get("gdp_growth_fcst_next_year") or "next year"
        macro_parts.append(
            f"GDP growth forecast {macro['gdp_growth_fcst_next_pct']:.1f}% (IMF {ny})"
        )
    if isinstance(macro.get("inflation_pct"), (int, float)):
        src = macro.get("inflation_source") or "IMF"
        yr  = macro.get("inflation_year") or "?"
        macro_parts.append(f"inflation {macro['inflation_pct']:.1f}% ({src} {yr})")
    if isinstance(macro.get("inflation_fcst_next_pct"), (int, float)):
        ny = macro.get("inflation_fcst_next_year") or "next year"
        macro_parts.append(
            f"inflation forecast {macro['inflation_fcst_next_pct']:.1f}% (IMF {ny})"
        )
    macro_block = ("Country macro: " + "; ".join(macro_parts) + ".") if macro_parts else ""

    broker_block = ""
    if ctx.get("recent_broker_actions"):
        rows = [
            f"  - {b.get('date','?')}: {b.get('headline','')[:120]}"
            for b in ctx["recent_broker_actions"][:3]
        ]
        broker_block = "Recent broker actions:\n" + "\n".join(rows)

    rating = ctx.get("rating_consensus") or "—"
    n_an = ctx.get("n_analysts") or 0
    b = ctx.get("buy_count") or 0
    h = ctx.get("hold_count") or 0
    s = ctx.get("sell_count") or 0

    return f"""{_SYSTEM}

COMPANY: {ctx['company_name']} ({ctx['ticker']})
SECTOR / INDUSTRY: {ctx['sector']} / {ctx['industry']} ({ctx['country']})

MARKET SNAPSHOT
  Last close: {cur_price}
  Market cap: {fmt_money(ctx.get('market_cap'))}
  Dividend yield: {_fmt_num(ctx.get('dividend_yield_pct'))}%

CONSENSUS (source: {ctx.get('consensus_source') or '—'}, {n_an} analysts)
  Next print: {ctx.get('next_q_report_date') or '—'} (period {ctx.get('next_q_period') or '—'})
  Next-Q  EPS: {next_q_eps} · Revenue: {next_q_rev}
  FY{ctx.get('fy1_year') or '—'}  EPS: {fy1_eps} · Revenue: {fy1_rev}
  Average target: {target} ({upside} vs last close), range {fmt_money(ctx.get('target_low'))}-{fmt_money(ctx.get('target_high'))}
  Rating: {rating} ({b}/{h}/{s} Buy/Hold/Sell)

VALUATION CONTEXT
  {pe_line}

EARNINGS TRACK RECORD
  {beats_line}
  {' '.join(surprise_lines)}

{commodity_block}

{macro_block}

{broker_block}

TASK
Write an institutional earnings-preview note for {ctx['company_name']}.
The audience already sees the raw numbers on the slide. Your job is
to INTERPRET — give the analyst a qualitative read on what matters
heading into the print.

Deliver a JSON object with these keys. No markdown, no preface, no
trailing prose. JSON only.

═══════════════════════════════════════════════════════════════════
"thesis_paragraph" — A 4-sentence executive summary. ~80–120 words.
═══════════════════════════════════════════════════════════════════

   THE VOICE WE WANT. Three reference examples — match this rhythm,
   word count, and qualitative register. Notice: almost no numbers,
   no Mohamed-pleasing acronyms, no macro filler. Just a clean
   interpretive read.

   ┌─ Apple ───────────────────────────────────────────────────────┐
   │ Apple enters earnings with focus on iPhone demand, Services   │
   │ growth, gross margin, and next-quarter guidance. Recent       │
   │ performance has been supported by AI upgrade expectations,    │
   │ while China demand and hardware replacement cycles remain     │
   │ key concerns. Investors should watch iPhone revenue,          │
   │ Services growth, gross margin, Greater China performance,    │
   │ and commentary on AI integration and capital returns. The     │
   │ setup appears balanced, as valuation remains elevated and     │
   │ expectations are already fairly high.                         │
   └───────────────────────────────────────────────────────────────┘

   ┌─ JPMorgan ────────────────────────────────────────────────────┐
   │ JPMorgan enters earnings with focus on net interest income,   │
   │ loan growth, credit quality, and capital returns. Recent      │
   │ performance has been supported by resilient U.S. economic    │
   │ data and stronger banking sentiment, while investors remain   │
   │ focused on whether net interest income has peaked. Key        │
   │ metrics to watch include NII, provision expense, loan         │
   │ growth, deposit trends, investment banking fees, and          │
   │ management commentary on credit and capital deployment. The   │
   │ setup appears cautiously attractive, supported by strong      │
   │ profitability and balance sheet strength.                     │
   └───────────────────────────────────────────────────────────────┘

   ┌─ Tesla ───────────────────────────────────────────────────────┐
   │ Tesla enters earnings with focus on vehicle deliveries,       │
   │ automotive gross margin, pricing strategy, and demand         │
   │ outlook. Recent performance has been driven by expectations   │
   │ around autonomous driving, robotaxi developments, and future  │
   │ product launches, while margin pressure and softer EV demand  │
   │ remain concerns. Investors should watch automotive revenue,   │
   │ gross margin excluding credits, free cash flow, delivery      │
   │ outlook, energy storage growth, and commentary on pricing     │
   │ and autonomy. The setup appears high-risk, high-reward       │
   │ given the stock's reliance on future growth narratives.       │
   └───────────────────────────────────────────────────────────────┘

   STRUCTURE — THREE sentences total (the reference boxes above show a
   legacy 4th "watch" sentence; OMIT it — see note):
     S1. "{{Company}} enters earnings with focus on [4 operational
         levers most central to the print]." Pick the levers that
         define how investors model THIS business. For banks:
         net interest income, loan growth, credit quality, capital
         returns. For oil & gas: production volumes, realized
         prices, capex, free cash flow. For tech: revenue growth,
         gross margin, AI/product momentum, guidance.
     S2. "Recent performance has been supported by [drivers],
         while [pressures] remain key concerns." REQUIRED — this is
         the meatiest interpretive sentence; never skip it. Qualitative
         narrative on what the Street has been rewarding and worrying
         about. Do not collapse the paragraph to two sentences.
     S3. "The setup appears [balanced / cautiously attractive /
         asymmetric / high-risk-high-reward], [one-clause
         reason]." Stop here. No scenario coda.

   Exactly THREE full sentences (~70-110 words). DO NOT write an
   "Investors should watch …" / "Key metrics to watch …" sentence —
   those questions live in their own "What to Watch" card on the same
   slide; repeating them here is redundant.

   You may anchor 1-2 numbers in S2 if they sharpen the read (e.g. a
   specific NIM percent, a current multiple). Do not stuff numbers. The
   references each cite zero or one number total.

═══════════════════════════════════════════════════════════════════
"highlights" — 5 short interpretive sentences (one per category).
═══════════════════════════════════════════════════════════════════

   Schema: list of {{"category": str, "body": str}}. Categories in
   order: EARNINGS, VALUATION, POSITIONING, WATCH, RISK. Each body
   ≤18 words. One single thought per pill.

   EARNINGS — the company-specific operational lever this print will
   test. NEVER frame it around GDP / inflation / "macro backdrop" —
   that reads as filler and isn't a print-level driver. Name the lever.
     Good: "Q2 print will test whether loan growth holds the prior
            5-7% pace as deposit competition bites."
     Weak: "Loan growth amid 1.6% GDP growth." (macro is backdrop,
            not the driver; says nothing about the print)

   VALUATION — ALWAYS lead with the LIVE multiple (the number on the
   slide). Then add a comparator: prefer the peer/industry median when
   the data block gives one ("peer/industry median P/E"); if it doesn't,
   the 5-year own-average is an acceptable comparator. Hard rule: a
   comparator NEVER stands alone — the live multiple must be present.
     Good: "Trades at 11.1x trailing earnings, a ~20% premium to the
            GCC bank peer median near 9.0x."
     Also good (no peer data): "Forward P/E 12.1x, a 4% premium to its
            own 5-year average of 10.7x."
     Weak: "Trades at a premium to its 5-year average." (no live
            multiple — comparator standing alone)

   POSITIONING — the Street's posture. Rating split, target
   dispersion, beat history. Interpretive, not restated.
     Good: "Street is constructive with 2 of 3 analysts at Buy,
            though target range OMR 0.39-0.52 signals thin
            conviction."

   WATCH — the single highest-conviction thing to listen for. One.
     Good: "Whether management raises mid-cycle NIM guidance from
            3.2% on the call."

   RISK — a SPECIFIC downside MECHANISM with a magnitude or baseline,
   not a generic worry. Bad risks are vanilla ("slower activity could
   pressure growth"); good risks name the channel AND a number/threshold.
     Good: "A 25-30bp NIM slip as deposits reprice faster than the
            ~60% CASA book would cut pre-provision profit mid-single
            digits."
     Weak: "Slower economic activity could pressure loan growth and
            asset quality." (no mechanism, no magnitude, says nothing
            a peer's risk pill couldn't)

═══════════════════════════════════════════════════════════════════
"catalysts" — 3 forward-looking drivers specific to the company.
═══════════════════════════════════════════════════════════════════

   Each catalyst names a specific lever (management action,
   operational milestone, named guidance range, sector flow) AND, where
   the data block supports it, anchors to a number. If swapping the
   company name with a peer would still make the bullet read true, it's
   too generic — rewrite.

   Rules:
     - Do NOT mention "macro backdrop", GDP, or inflation in a catalyst
       at all — not as a driver, not as a benchmark. Anchor entirely on
       a company lever (e.g. "loan growth that outpaces the prior 5-7%
       guidance", NOT "outpaces the macro backdrop").
     - Carry a baseline figure when one exists (a yield, a guidance
       range, a ratio) so the reader can size the move.
     - No vague nouns like "updates", "developments", "progress" — say
       WHAT specifically (a buyback authorization, a guidance raise, a
       fee-income mix shift).

   Examples of the right shape:
     - "A buyback authorization on the call that lifts the payout
        beyond the current 4.22% dividend yield."
     - "Loan-growth guidance confirmed at the prior 5-7% range despite
        deposit-cost pressure."
     - "Fee income climbing toward 25% of revenue, cutting
        spread-business dependence."

═══════════════════════════════════════════════════════════════════
"risks" — 3 company-specific downside paths.
═══════════════════════════════════════════════════════════════════

   The 3 risks must each be DISTINCT — different channels, no overlap
   with one another or with the RISK highlight pill on slide 1. Each
   ties to a baseline number or named trigger. Do NOT restate valuation
   if it already lives in the VALUATION pill.

   Rules:
     - BANNED filler phrasings: "unexpected", "unforeseen",
       "higher-than-expected", "lower-than-expected",
       "slower-than-expected", "better-than-expected". They say nothing.
       Replace with a concrete magnitude/threshold (e.g. "cost-of-risk
       rising above 50bp") or a named trigger (a specific exposure, a
       repricing channel, a one-off charge).
     - Don't repeat the same worry (e.g. "slower loan growth") across
       multiple bullets or across slides; if it's already on the deck,
       pick a different downside path.

   Examples of the right shape:
     - "Asset-quality drift from the current sub-2% NPL ratio as the
        rate-cut cycle pressures provisioning expense."
     - "A larger one-off provision charge — Oman corporate exposure is
        concentrated — that resets the cost-of-risk run-rate higher."
     - "Share loss to digitally-native challengers accelerating
        cost-to-income deterioration from current levels."

═══════════════════════════════════════════════════════════════════
"watch_list" — 3 questions to put to management on the call.
═══════════════════════════════════════════════════════════════════

   Sharp, specific questions ending in "?". These live in their own
   "What to Watch" card and are NOT echoed in the thesis paragraph
   (the exec summary no longer carries a watch sentence). Because this
   card now stands alone, make each question substantive — name the
   metric AND the threshold/context that makes the answer actionable,
   not just a one-line ask.

   Examples:
     - "What is the full-year NIM guidance range, and how much deposit
        repricing is already assumed in it?"
     - "Where does the loan-growth pipeline sit versus the prior 5-7%
        guidance, and which segments are driving it?"
     - "Is the dividend payout sustainable at the current ~4.2% yield
        if cost-of-risk normalizes from today's low base?"

═══════════════════════════════════════════════════════════════════
NUMERIC DISCIPLINE
═══════════════════════════════════════════════════════════════════

  Every number you cite must come from the DATA block above. No
  rounding inconsistencies, no hallucinated comparators.

  When citing a P/E, keep the qualifier ("forward", "trailing",
  "FY26E") so the reader doesn't conflate them.

  When citing macro, keep the "(IMF YYYY)" / "(WB YYYY)" tag.

MACRO DISCIPLINE
  The macro block is BACKGROUND only. Do NOT make GDP or inflation the
  driver of an EARNINGS pill, a catalyst, or a risk — those must anchor
  on company-specific operational levers (loan growth, NIM, provisions,
  fees, capital). Macro may appear at most once, in the thesis S2
  "drivers/pressures" clause, and only if it genuinely moved the stock.
"""


# ── Numeric-trace validator ───────────────────────────────────────
#
# Even with a tight grounding contract, the LLM occasionally invents
# numbers (rounds inconsistently, hallucinates a "5-year average" the
# context never carried, etc.). The validator extracts every numeric
# token from each LLM-generated sentence and confirms it matches a value
# we actually have in the context (or a stated derivation: surprise
# averages, target spreads, P/E premium to history). Sentences whose
# numbers don't trace are dropped — the renderer falls back to the
# deterministic catalyst/risk defaults for any bullets that drop out.
#
# Tolerance: ±5% relative for matches (covers reasonable rounding); a
# table of common derivations is computed once from the context.

import re as _re


def _allowed_numbers(ctx: dict) -> set[float]:
    """Build the whitelist of numeric values the LLM may cite. Includes
    raw fields, simple derivations (% changes, averages), and a few
    rounded variants so a "12.4x" match doesn't fail against 12.39."""
    vals: set[float] = set()

    def _add(v):
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            try:
                f = float(v)
                vals.add(round(f, 2))
                # Integer rounding ONLY for values that are conceptually
                # integers (analyst counts, beat ratios). Continuous
                # values like 4.83% yield shouldn't unlock 5.0 in the
                # allow-set — that's how "5.2% average surprise" slips
                # past the validator.
                if isinstance(v, int) or (isinstance(v, float) and v.is_integer()):
                    vals.add(round(f, 0))
            except (ValueError, OverflowError):
                pass

    # Raw scalars from context (anything that's a number).
    def _walk(d):
        if isinstance(d, dict):
            for v in d.values():
                _walk(v)
        elif isinstance(d, list):
            for v in d:
                _walk(v)
        else:
            _add(d)
    _walk(ctx)

    # Derived: P/E premium/discount to 5y average.
    pe_recent = ctx.get("pe_recent")
    pe_avg = ctx.get("pe_5y_avg")
    if isinstance(pe_recent, (int, float)) and isinstance(pe_avg, (int, float)) and pe_avg:
        delta = (pe_recent / pe_avg - 1.0) * 100
        _add(delta)
        _add(-delta)

    # Derived: target upside/downside (already in ctx as upside_pct, but
    # also surface the absolute pct for downside framing).
    up = ctx.get("upside_pct")
    if isinstance(up, (int, float)):
        _add(abs(up))

    # Derived: average surprise pct across valid surprises.
    last = ctx.get("last_surprise") or {}
    sp = last.get("eps_surprise_pct") if isinstance(last, dict) else None
    if isinstance(sp, (int, float)):
        _add(abs(sp))

    # Beat ratio numerator/denominator (e.g. "3 of 4 quarters").
    if isinstance(ctx.get("surprise_beats_last4"), int):
        _add(ctx["surprise_beats_last4"])
    if isinstance(ctx.get("surprise_n_recent"), int):
        _add(ctx["surprise_n_recent"])

    # Rating split + total
    for k in ("buy_count", "hold_count", "sell_count", "n_analysts"):
        _add(ctx.get(k))
    if all(isinstance(ctx.get(k), int) for k in ("buy_count", "hold_count", "sell_count")):
        _add(ctx["buy_count"] + ctx["hold_count"] + ctx["sell_count"])

    # Commodity YoY values
    for tag, info in (ctx.get("commodities") or {}).items():
        if isinstance(info, dict):
            _add(info.get("value"))
            _add(info.get("yoy_pct"))
            yoy = info.get("yoy_pct")
            if isinstance(yoy, (int, float)):
                _add(abs(yoy))

    return vals


_NUM_PATTERN = _re.compile(
    # Match signed/unsigned decimals, optionally followed by %, x, B, M, T, bps
    r"[+-]?\d+(?:[,\d]*\.?\d+|\.\d+)?(?:\s*(?:%|x|bps|B|M|T|K))?"
)
_INTEGER_NUM = _re.compile(r"\b\d{1,2}\b")


def _extract_numbers(text: str) -> list[float]:
    """Pull every numeric literal out of an LLM sentence. Strips
    commas, percentage signs, and unit suffixes; returns floats."""
    out = []
    for m in _NUM_PATTERN.finditer(text or ""):
        raw = m.group(0).strip()
        # strip suffix unit
        token = _re.sub(r"\s*(?:%|x|bps|B|M|T|K)$", "", raw, flags=_re.IGNORECASE)
        token = token.replace(",", "").rstrip(".")
        try:
            out.append(float(token))
        except ValueError:
            continue
    return out


def _number_matches(n: float, allowed: set[float], *, tol_pct: float = 5.0) -> bool:
    """Is n within tol_pct of any allowed value? Also accepts exact small
    integers (analyst counts, beat ratios) — those have to match exactly
    because rounding 2.0 to 3 changes the meaning."""
    if not allowed:
        return True  # nothing to validate against
    n_abs = abs(n)
    if n_abs == 0:
        return 0.0 in allowed or any(abs(a) < 0.01 for a in allowed)
    # Exact integer match (analyst counts, beat ratios)
    if n.is_integer() and 0 <= n <= 50:
        if n in allowed or -n in allowed:
            return True
    for a in allowed:
        a_abs = abs(a)
        denom = max(n_abs, a_abs)
        if denom < 0.01:
            continue
        if abs(n - a) / denom * 100 <= tol_pct:
            return True
        # Try sign-flipped (writer says "+5%" against a stored -5% value).
        if abs(n + a) / denom * 100 <= tol_pct:
            return True
    return False


def _validate_sentence(text: str, allowed: set[float]) -> bool:
    """Sentence passes if every numeric token in it traces to an allowed
    value. A sentence with no numeric tokens passes trivially (we want
    qualitative analytical voice to survive)."""
    nums = _extract_numbers(text)
    if not nums:
        return True
    return all(_number_matches(n, allowed) for n in nums)


def _validate_llm_output(payload: dict, ctx: dict) -> dict:
    """Drop sentences / bullets whose numbers don't trace to the context.

    Loose by design: we only police hallucinated numbers (the one thing
    Gemini can do that would mislead the analyst). Voice, phrasing,
    bullet count, and analytical framing are the model's leeway. A
    bullet with NO numbers passes trivially — interpretation without
    a number is fine and often better than a number with no insight.
    """
    allowed = _allowed_numbers(ctx)
    cleaned = dict(payload)

    # Thesis: drop sentences whose numbers don't trace. Re-join the rest.
    # IMPORTANT: split on a terminator followed by whitespace + capital
    # letter so decimal numbers stay intact. The earlier `findall` regex
    # had two failure modes: (1) split "3.5%" into "3" / "5%", (2)
    # silently SKIP regions it couldn't match (the bytes between matches
    # were dropped entirely), amputating prose mid-clause. `re.split` on
    # an actual sentence boundary preserves every byte and only splits
    # at true sentence ends.
    thesis = payload.get("thesis_paragraph") or ""
    if thesis:
        sentences = _re.split(r"(?<=[.!?])\s+(?=[A-Z])", thesis.strip())
        if not sentences:
            sentences = [thesis]
        kept = [s.strip() for s in sentences if _validate_sentence(s, allowed)]
        cleaned["thesis_paragraph"] = " ".join(kept).strip()
        dropped = len(sentences) - len(kept)
        if dropped:
            log.warning("Dropped %d thesis sentence(s) with ungrounded numbers", dropped)

    # Highlights: number-trace + ≤20-word cap so the pill doesn't overflow.
    hl = payload.get("highlights") or []
    hl_kept: list[dict] = []
    for it in hl:
        if not isinstance(it, dict):
            continue
        body = (it.get("body") or "").strip()
        if not body:
            continue
        if not _validate_sentence(body, allowed):
            continue
        if len(body.split()) > 20:
            continue
        hl_kept.append(it)
    cleaned["highlights"] = hl_kept

    # Catalysts / risks / watch_list: only the number-trace filter applies.
    # A bullet that says nothing numeric is fine — the prompt explicitly
    # allows interpretation without a number.
    for key in ("catalysts", "risks", "watch_list"):
        items = payload.get(key) or []
        kept: list[str] = []
        for it in items:
            if not isinstance(it, str) or not it.strip():
                continue
            if not _validate_sentence(it, allowed):
                continue
            kept.append(it)
        cleaned[key] = kept
        if len(kept) < len(items):
            log.warning("Dropped %d %s bullet(s) (ungrounded numbers)",
                         len(items) - len(kept), key)

    return cleaned


# ── Cache ────────────────────────────────────────────────────────

def _cache_key(ctx: dict) -> str:
    """Hash the context (excluding volatile fields) so equal data → same key."""
    stable = {k: v for k, v in ctx.items() if k not in ("ticker",)}
    payload = json.dumps(stable, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _cache_path(ticker: str, key: str) -> Path:
    safe = ticker.replace("/", "_").replace(".", "_")
    return _CACHE_DIR / f"{safe}__{key}.json"


def _read_cache(path: Path) -> Optional[dict]:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str))


# ── Main entry point ─────────────────────────────────────────────

def generate_summary(ticker: str, *, force_refresh: bool = False) -> Optional[dict]:
    """Build context → optionally call Gemini → return structured summary
    or None on failure. Caller is responsible for fallback behaviour.

    Cached by (ticker, context_hash) — re-rendering the deck without
    refreshing canonical_store is a free hit."""
    ctx = build_context(ticker)
    key = _cache_key(ctx)
    path = _cache_path(ticker, key)
    if not force_refresh:
        cached = _read_cache(path)
        if cached:
            return cached

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        log.info("GEMINI_API_KEY not set; LLM summary unavailable for %s", ticker)
        return None

    try:
        from src.providers.gemini import _call_gemini  # reuse the wrapper
        prompt = _prompt(ctx)
        out = _call_gemini(prompt, for_investment_view=True)
    except Exception as exc:
        log.warning("Gemini summary failed for %s: %s", ticker, exc)
        return None

    if not out or not isinstance(out, dict):
        return None

    # Validate shape
    required = {"thesis_paragraph", "catalysts", "risks", "watch_list"}
    missing = required - set(out.keys())
    if missing:
        log.warning("Gemini summary missing keys for %s: %s", ticker, missing)
        return None

    def _build_payload(raw: dict) -> dict:
        # Normalise highlights into 5-tuple-shaped dicts, preserving the
        # category order the prompt asked for. Bad inputs are dropped — the
        # templated `_derive_highlights` covers the missing slot.
        hl_in = raw.get("highlights") or []
        hl_out = []
        for it in hl_in[:5]:
            if isinstance(it, dict):
                cat = str(it.get("category") or "").strip().upper()
                body = str(it.get("body") or "").strip()
                if cat and body:
                    hl_out.append({"category": cat, "body": body})
        return {
            "thesis_paragraph": (raw.get("thesis_paragraph") or "").strip(),
            "catalysts":  list(raw.get("catalysts")  or [])[:3],
            "risks":      list(raw.get("risks")      or [])[:3],
            "watch_list": list(raw.get("watch_list") or [])[:3],
            "highlights": hl_out,
            "provider":   "gemini",
            "model":      "auto",
            "ticker":     ticker,
            "as_of":      datetime.now(timezone.utc).isoformat(),
            "context_hash": key,
        }

    payload = _build_payload(out)
    # Numeric-trace validator: drop any sentence / bullet that cites a
    # number we can't trace back to the context. Voice and bullet count
    # are the model's leeway — only hallucinated numbers are policed.
    payload = _validate_llm_output(payload, ctx)

    # TIER-2 STRUCTURED VALIDATION — produces ValidationFinding records
    # alongside the scrubbed payload. The numeric-trace path above
    # silently rewrites the prose; tier-2 KEEPS a record of which
    # tokens were unmatched so the provenance.xlsx audit trail can
    # surface them to the analyst.
    try:
        from src.services.validation_layer import run_tier_2
        anchors = _allowed_numbers(ctx)
        v_rep = run_tier_2(
            ticker,
            thesis_paragraph=payload.get("thesis_paragraph"),
            allowed_numbers=anchors,
        )
        if v_rep.findings:
            payload["validation_tier2"] = v_rep.as_dict()
    except Exception as exc:
        log.debug("Tier-2 validation skipped: %s", exc)

    _write_cache(path, payload)
    return payload
