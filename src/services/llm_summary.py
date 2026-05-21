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

    # Normalise FY year labels (strip an existing "FY" prefix so the prompt
    # doesn't end up with "FYFY2026").
    def _norm_fy(v):
        s = str(v or "").strip()
        if s.upper().startswith("FY"):
            s = s[2:].lstrip()
        return s

    return {
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
Write an earnings-preview package as a JSON object with these keys.
Voice: institutional sell-side analyst writing the morning note. Direct,
declarative, no hedging adverbs, no marketing language. Synthesize
across the data block above — connect signals into a view rather than
listing facts back. You have analytical leeway on framing; you do NOT
have leeway on numbers.

1. "thesis_paragraph": EXACTLY 4 sentences, no more, no less. Follow
   this rhythm precisely — readers expect this shape:

   Sentence 1 — "{COMPANY} enters earnings with focus on [4 sector-
     specific drivers]."   The drivers should be concrete operating
     levers (e.g. for a bank: net interest income, loan growth, credit
     quality, capital returns; for oil & gas: production volumes,
     realized prices, lifting costs, project ramps). NOT generic words
     like "earnings", "performance", or "results".

   Sentence 2 — "Recent performance has been [supported by X / driven
     by Y], while [headwind or concern Z]."   X / Y / Z must cite the
     data block (a surprise %, a sector trend, a macro forecast, a
     price move).

   Sentence 3 — "Investors should watch [5–7 specific items + closing
     commentary theme]."   Items must be specific (e.g. "NIM trajectory,
     provision expense, deposit costs, fee income, loan growth, and
     management commentary on capital deployment"). NOT vague
     ("growth", "margins", "outlook").

   Sentence 4 — "The setup appears [balanced / cautiously attractive /
     constructive / high-risk-high-reward], [one-clause justification]."
     The justification must reference a concrete anchor: valuation
     vs history, beat/miss track record, or consensus dispersion.

   This 4-sentence shape is non-negotiable. Do NOT add a fifth sentence.
   Do NOT collapse to three. Do NOT skip the bracketed slot fills.

2. "catalysts": EXACTLY 3 bullets, one sentence each. Forward-looking
   drivers only — what could MOVE consensus on the print or in the
   following weeks. Pick from these categories where the data block
   supports them: earnings release dynamics (beat vs miss, guidance
   delta), margin trajectory, loan / volume growth, dividend or buyback
   announcement, valuation re-rating triggers, sector-specific
   developments (commodity price moves, regulatory change), management
   commentary slots. Each bullet MUST cite a number from the data block.
   FORBIDDEN: bullets that just restate a single fact ("Dividend yield
   4.8%"), generic boilerplate ("strong execution"), or upside-to-target
   restated as a catalyst. The number anchors the analytical claim, not
   the other way around.

3. "risks": EXACTLY 3 bullets, same voice as catalysts. Company-specific
   downside drivers anchored in real numbers, tied to the investment
   case. FORBIDDEN: generic macro statements that don't directly affect
   this company's business model (a low domestic inflation print is
   NOT a risk for a commercial bank), single-fact restatements ("Low
   end of target range OMR 0.39"), or repetition of any catalyst with
   the sign flipped. Each risk must be a real downside path with a
   numeric anchor.

4. "watch_list": EXACTLY 3 specific questions ending in "?". Each must
   reference a real data point (a price level, a margin, a capex
   number, a surprise pct, a broker action, a guidance figure).
   Not generic open-enders. Frame as the precise question an analyst
   would put to management on the call.

5. "highlights": EXACTLY 5 short analytical takes for the front-page
   slide. JSON list of {"category": str, "body": str}. Use these five
   categories, in this order:
     - "EARNINGS"   — what the print is most likely to hinge on (an
                      operating lever or the beat/miss track record),
                      framed as a forward-looking analytical statement.
     - "VALUATION"  — how the current multiple sits relative to peers
                      or its own history; the re-rate vs de-rate path.
     - "POSITIONING"— consensus / target context AS AN INTERPRETATION,
                      not a restatement. Frame what the positioning
                      tells you (consensus dispersion, crowded long,
                      target asymmetry).
     - "WATCH"      — the single concrete number to listen for on the
                      call (NIM, loan growth, capex guidance, etc.).
     - "RISK"       — the most material company-specific downside,
                      tied to the investment case.
   Each body is ONE short sentence (≤14 words). Anchor every body in
   a number from the data block. NO pure restatements — the line must
   add interpretation. FORBIDDEN: starting a body with "Dividend yield
   X%" / "Forward P/E X.Xx" / "Target X" with no analytical context.

HARD RULES
  - Every number you write must trace to a value in the data block.
    Do not round inconsistently. Do not invent precise figures
    ("approximately 12%" is not acceptable cover for an invented
    number — drop the sentence instead).
  - If a sentence cannot cite a real number for the claim it is
    making, rewrite it as qualitative or drop it.
  - Currency, units, and time period must match how the data block
    presents them. When citing a macro figure, retain the "(IMF YYYY)"
    or "(WB YYYY)" tag from the data block so the source/year is
    visible in the deck.
  - No markdown fences, no preface, no trailing prose. JSON only.
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


_GENERIC_TOKENS = (
    "strong execution", "constructive guidance", "positive momentum",
    "robust performance", "challenging environment", "favorable backdrop",
    "favourable backdrop", "well positioned", "well-positioned",
    "moving forward", "going forward", "in due course",
)


def _is_generic_filler(text: str) -> bool:
    """True when a sentence consists mostly of empty analyst boilerplate
    with no numeric anchor. Used to drop catalyst/risk bullets that pass
    the numeric-trace test trivially (no numbers) but contribute nothing."""
    if not text:
        return True
    t = text.lower()
    if any(tok in t for tok in _GENERIC_TOKENS):
        return True
    return False


def _enforce_thesis_shape(thesis: str) -> tuple[str, bool]:
    """Lightly enforce the Apple/JPM/Tesla 4-sentence template.

    Returns (text, ok). When the output has 4 sentences AND the rhythm
    anchors are present ("enters earnings with focus on", "Investors
    should watch", "setup appears"), we trust it. Otherwise we surface
    `ok=False` so `generate_summary` can retry with a stricter reminder.
    """
    if not thesis:
        return "", False
    sentences = _re.findall(r"[^.!?]+[.!?]+", thesis)
    if len(sentences) != 4:
        return thesis.strip(), False
    joined = thesis.lower()
    anchors = ("enters earnings with focus on", "investors should watch", "setup appears")
    ok = all(a in joined for a in anchors)
    return thesis.strip(), ok


def _validate_llm_output(payload: dict, ctx: dict) -> dict:
    """Drop sentences/bullets whose numbers don't trace to the context.
    Returns a copy of payload with offending content removed."""
    allowed = _allowed_numbers(ctx)
    cleaned = dict(payload)

    # Thesis paragraph: validate the 4-sentence template AND that numeric
    # claims trace. When the template is broken we still surface the text
    # (the renderer falls back gracefully) but flag it so the caller can
    # retry once with a stricter reminder.
    thesis = payload.get("thesis_paragraph") or ""
    if thesis:
        thesis_text, shape_ok = _enforce_thesis_shape(thesis)
        sentences = _re.findall(r"[^.!?]+[.!?]+", thesis_text)
        if not sentences:
            sentences = [thesis_text]
        kept = [s.strip() for s in sentences if _validate_sentence(s, allowed)]
        cleaned["thesis_paragraph"] = " ".join(kept).strip()
        cleaned["_thesis_shape_ok"] = shape_ok
        dropped = len(sentences) - len(kept)
        if dropped:
            log.warning("Dropped %d thesis sentence(s) with ungrounded numbers", dropped)

    # Highlights: same trace check as catalysts/risks, plus body length cap.
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
        if _is_generic_filler(body):
            continue
        # Highlights cap at ~14 words to fit the slide pill. Trim aggressively.
        if len(body.split()) > 20:
            continue
        hl_kept.append(it)
    cleaned["highlights"] = hl_kept

    # Catalysts / risks: each bullet must (a) have at least one numeric anchor
    # that traces AND (b) not be generic-filler-only. watch_list bullets
    # can be qualitative (they're questions), so only the numeric check
    # applies there.
    for key in ("catalysts", "risks", "watch_list"):
        items = payload.get(key) or []
        kept: list[str] = []
        for it in items:
            if not isinstance(it, str) or not it.strip():
                continue
            if not _validate_sentence(it, allowed):
                continue
            if key in ("catalysts", "risks") and _is_generic_filler(it):
                continue
            # catalysts / risks must actually cite at least one number
            # (the prompt forbids unanchored claims; enforce it here).
            if key in ("catalysts", "risks") and not _extract_numbers(it):
                log.warning("Dropped %s bullet with no numeric anchor: %s", key, it[:80])
                continue
            kept.append(it)
        cleaned[key] = kept
        if len(kept) < len(items):
            log.warning("Dropped %d %s bullet(s) (validation)",
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
    # number we can't trace back to the context. Loosens the prompt's
    # prose patterns (the LLM gets analytical leeway) while keeping the
    # numeric grounding strict.
    payload = _validate_llm_output(payload, ctx)

    # One retry when the thesis_paragraph shape didn't match the
    # Apple/JPM/Tesla template (4 sentences with the rhythm anchors).
    # The retry prompt is identical apart from a stricter reminder
    # prepended — the model usually self-corrects on the second pass.
    if payload.get("_thesis_shape_ok") is False:
        log.info("Thesis shape mismatch for %s — retrying with stricter reminder", ticker)
        retry_prefix = (
            "PREVIOUS ATTEMPT DID NOT FOLLOW THE 4-SENTENCE TEMPLATE. "
            "Output EXACTLY four sentences for thesis_paragraph, in this order:\n"
            "  1. '<Company> enters earnings with focus on <four drivers>.'\n"
            "  2. 'Recent performance has been <X / driven by Y>, while <headwind>.'\n"
            "  3. 'Investors should watch <5-7 specific items + commentary theme>.'\n"
            "  4. 'The setup appears <posture>, <one-clause justification>.'\n"
            "Do NOT collapse to three sentences. Do NOT add a fifth.\n\n"
        )
        try:
            out2 = _call_gemini(retry_prefix + prompt, for_investment_view=True)
            if out2 and isinstance(out2, dict):
                payload2 = _validate_llm_output(_build_payload(out2), ctx)
                if payload2.get("_thesis_shape_ok"):
                    payload = payload2
                elif (payload2.get("thesis_paragraph") or "").strip():
                    # Even if shape is still imperfect, the retry's prose is
                    # often closer — keep its thesis but layer the lists from
                    # whichever pass produced more anchored bullets.
                    if len(payload2.get("catalysts") or []) >= len(payload.get("catalysts") or []):
                        payload = payload2
        except Exception as exc:
            log.warning("Thesis shape retry failed for %s: %s", ticker, exc)

    payload.pop("_thesis_shape_ok", None)
    _write_cache(path, payload)
    return payload
