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
        # Valuation history
        "pe_recent": pe_recent,
        "pe_5y_avg": pe_avg,
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

    pe_line = ""
    if isinstance(ctx.get("pe_recent"), (int, float)) and isinstance(ctx.get("pe_5y_avg"), (int, float)):
        avg = ctx["pe_5y_avg"]
        rec = ctx["pe_recent"]
        delta_pct = (rec / avg - 1.0) * 100 if avg else 0
        pe_line = (
            f"P/E around {rec:.1f}x vs 5-year average {avg:.1f}x "
            f"({delta_pct:+.0f}% relative)."
        )

    commodity_lines = []
    for tag, info in (ctx.get("commodities") or {}).items():
        if not isinstance(info, dict):
            continue
        val = info.get("value")
        yoy = info.get("yoy_pct")
        unit = info.get("unit", "")
        if val is None:
            continue
        yoy_str = f" ({yoy:+.1f}% YoY)" if isinstance(yoy, (int, float)) else ""
        commodity_lines.append(f"{tag}: {val} {unit}{yoy_str}")
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
Write an earnings-preview package as a JSON object with these keys:

1. "thesis_paragraph": EXACTLY 4 sentences, institutional sell-side voice,
   roughly 90-130 words. The four sentences must follow this structure:
     • S1 — Stance: state the call (constructive / cautious / balanced)
       relative to Street consensus ({rating}). Reference the actual rating
       label and analyst count.
     • S2 — Valuation/positioning evidence: cite the P/E vs 5-year avg line,
       the dividend yield, or the target-vs-current spread. At least one
       precise number with units (x, %, or {cur}).
     • S3 — Operating driver into the print: anchor on commodity-price moves,
       macro context, or the consensus next-Q EPS/Revenue figure. At least
       one precise number.
     • S4 — Swing factor: identify the single management commentary line
       that would shift consensus, framed as what investors will watch.
   GROUNDING CONTRACT: every number you write must appear verbatim (or as a
   stated derivation, e.g. "FY26 EPS of {fy1_eps}") in the data block above.
   If a sentence cannot cite a number, rewrite it as qualitative without the
   missing figure.

2. "catalysts": EXACTLY 3 bullets. Each bullet is one sentence and starts
   with the lever, then the number. Pattern: "<Lever> — <quantitative anchor>".
   Use only the data block above; do NOT use the literal numbers in the
   pattern examples below — those are format guides, not facts:
     • Pattern: "Beat-streak — N of last K quarters above consensus EPS"
     • Pattern: "Re-rate room — current P/E Xx vs 5y avg Yx"
     • Pattern: "Capital return — Z% dividend yield"
   Forbidden generics: "constructive guidance", "strong execution",
   "positive momentum", "supportive backdrop". Drop any bullet you cannot
   anchor in the data block.

3. "risks": EXACTLY 3 bullets. Same pattern as catalysts. Examples (format
   only — use real numbers from the data block):
     • Pattern: "Margin pressure — feedstock cost Z% YoY"
     • Pattern: "Target gap — Street PT implies X% downside vs last close"
     • Pattern: "Miss tape — N of last K quarters below consensus"

4. "watch_list": EXACTLY 3 questions ending in "?". Each must reference a
   specific data point from the block (price, margin, capex, surprise pct,
   broker action, etc.) — not a generic open-ended question.

VALIDATION CHECKLIST (apply silently before responding):
  □ Every numeric appearing in the JSON traces back to a value in the data block.
  □ No forbidden phrase appears.
  □ Sentence counts are exact (4 thesis, 3/3/3 bullets/questions).
  □ Bullets are pattern-conformant ("<Lever> — <number with unit>").
  □ No markdown fences, no preface, no trailing prose.

Return ONLY the JSON object.
"""


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

    payload = {
        "thesis_paragraph": (out.get("thesis_paragraph") or "").strip(),
        "catalysts":  list(out.get("catalysts")  or [])[:3],
        "risks":      list(out.get("risks")      or [])[:3],
        "watch_list": list(out.get("watch_list") or [])[:3],
        "provider":   "gemini",
        "model":      "auto",
        "ticker":     ticker,
        "as_of":      datetime.now(timezone.utc).isoformat(),
        "context_hash": key,
    }
    _write_cache(path, payload)
    return payload
