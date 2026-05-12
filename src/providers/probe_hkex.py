"""
HKEX (Hong Kong Exchange) probe provider — Stage 1.

HKEX exposes a rich equity-quote API at
`www1.hkex.com.hk/hkexwidget/data/getequityquote?sym=<code>` but
guards it with a JS-generated token that:
  - Doesn't appear in the static HTML
  - Expires within minutes (the token captured at 18:44 was already
    rejected at 18:46 with HTTP 403)
  - Is generated client-side from a rotating seed

So the provider uses Playwright to load the quote page, captures the
real getequityquote response from the network tab, parses the JSONP
wrapper, and caches the parsed data to disk for 24h. After the first
call per ticker per day, subsequent probes are zero-network.

Response payload is rich — verified against Tencent (sym=700):
  ric:            0700.HK            (Reuters identifier)
  issuer_name:    Tencent Holdings Ltd.
  ccy:            HKD                (trading currency)
  sedol:          BMMV2K8
  amt_os:         9,117,991,636      (shares outstanding)
  lot:            100
  hi52 / lo52:    52-week range
  eps:            24.7487            (trailing EPS)
  div_yield:      1.16               (%)
  mkt_cap, am:    market cap + assets (string + unit suffix)
  chairman, incorpin: governance + jurisdiction
  fiscal_year_end: e.g. "31 Dec 2025"

Tickers in our master use `.HK` suffix with leading zeros (0700.HK).
The HKEX API uses the bare code without zeros (700). The provider
strips both.
"""

from __future__ import annotations

import json
import re
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

from src.services.probe_harness import Provider, persist_raw, cache_root


_MIN_GAP = 4.0  # seconds between HKEX page loads — HKEX is friendly but no need to hammer
_REQ_LOCK = threading.Lock()
_last_call: float = 0.0


def _norm_sym(ticker: str) -> str:
    """0700.HK -> '700' (HKEX API accepts both but the canonical form
    has no leading zeros). Strip .HK suffix, then strip leading zeros."""
    s = (ticker or "").upper().replace(".HK", "").strip()
    return s.lstrip("0") or s


def _cache_path(symbol: str) -> Path:
    return cache_root() / "hkex" / f"quote_{symbol}.json"


def _read_cache(symbol: str, ttl_hours: float = 24) -> Optional[dict]:
    p = _cache_path(symbol)
    if not p.exists():
        return None
    age = datetime.now(timezone.utc) - datetime.fromtimestamp(
        p.stat().st_mtime, tz=timezone.utc
    )
    if age > timedelta(hours=ttl_hours):
        return None
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(symbol: str, payload: dict) -> None:
    p = _cache_path(symbol)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2, default=str))


def _rate_limit():
    global _last_call
    with _REQ_LOCK:
        elapsed = time.monotonic() - _last_call
        if elapsed < _MIN_GAP:
            time.sleep(_MIN_GAP - elapsed)
        _last_call = time.monotonic()


def _strip_jsonp(text: str) -> Optional[dict]:
    """HKEX wraps the response in jQuery<n>(...) — strip the callback
    and parse the inner JSON."""
    m = re.match(r"^\s*[a-zA-Z_$][\w$]*\((.*)\)\s*;?\s*$", text, re.DOTALL)
    inner = m.group(1) if m else text
    try:
        return json.loads(inner)
    except json.JSONDecodeError:
        return None


def _fetch_via_browser(symbol: str) -> Optional[dict]:
    """Use Playwright to load the quote page and intercept the
    getequityquote response. Returns the parsed `quote` dict or None."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None

    _rate_limit()

    quote_body: list[str] = []

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
        except Exception:
            return None
        try:
            ctx = browser.new_context(user_agent="Mozilla/5.0")
            page = ctx.new_page()

            def on_response(resp):
                if "getequityquote" in resp.url:
                    try:
                        quote_body.append(resp.text())
                    except Exception:
                        pass

            page.on("response", on_response)
            try:
                page.goto(
                    f"https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/"
                    f"Equities-Quote?sym={symbol}&sc_lang=en",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                page.wait_for_timeout(7000)
            except Exception:
                return None
        finally:
            browser.close()

    if not quote_body:
        return None
    parsed = _strip_jsonp(quote_body[0])
    if not parsed:
        return None
    data = parsed.get("data") or {}
    quote = data.get("quote")
    return quote if isinstance(quote, dict) else None


def _get_quote(symbol: str) -> Optional[dict]:
    """Cache-first wrapper around _fetch_via_browser."""
    cached = _read_cache(symbol)
    if cached:
        return cached
    fresh = _fetch_via_browser(symbol)
    if fresh:
        _write_cache(symbol, fresh)
    return fresh


def _comma_int(s: Any) -> Optional[float]:
    """Parse '9,117,991,636' -> 9117991636.0."""
    if s is None:
        return None
    try:
        return float(str(s).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _maybe_float(s: Any) -> Optional[float]:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


class HKEXProvider(Provider):
    name = "hkex"

    def __init__(self):
        self._cache: dict[str, Optional[dict]] = {}

    def _quote(self, ticker: str) -> dict:
        sym = _norm_sym(ticker)
        if sym not in self._cache:
            self._cache[sym] = _get_quote(sym)
        q = self._cache[sym]
        if not q:
            raise ValueError(f"HKEX returned no quote for sym={sym}")
        return q

    # ── Identity ──

    def _fetch_current_price(self, ticker: str):
        q = self._quote(ticker)
        raw_id = persist_raw(self.name, ticker, "current_price", q)
        # `ls` is last sale; `as` is ask; `bd` is bid. Prefer ls when present.
        for key in ("ls", "last", "as"):
            v = _maybe_float(q.get(key))
            if v is not None:
                return v, q.get("ccy") or "HKD", "", raw_id
        # Fallback: hist_closedate has the price embedded — but for
        # robustness, fail if no live price.
        raise ValueError("no price field (ls/last/as) on HKEX quote")

    def _fetch_market_cap(self, ticker: str):
        """HKEX reports mkt_cap as a string with `mkt_cap_u` (B/M/K).
        Convert to raw HKD."""
        q = self._quote(ticker)
        raw_id = persist_raw(self.name, ticker, "market_cap", q)
        v = _maybe_float(q.get("mkt_cap"))
        unit = (q.get("mkt_cap_u") or "").upper()
        if v is None:
            # Compute from shares × price as fallback.
            shares = _comma_int(q.get("amt_os"))
            price = _maybe_float(q.get("ls")) or _maybe_float(q.get("as"))
            if shares and price:
                return shares * price, q.get("ccy") or "HKD", "", raw_id
            raise ValueError("no mkt_cap and can't derive from shares × price")
        mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}.get(unit, 1)
        return v * mult, q.get("ccy") or "HKD", "", raw_id

    def _fetch_company_profile(self, ticker: str):
        q = self._quote(ticker)
        raw_id = persist_raw(self.name, ticker, "company_profile", q)
        profile = {
            "name":           q.get("issuer_name"),
            "ric":            q.get("ric"),
            "sedol":          q.get("sedol"),
            "currency":       q.get("ccy"),
            "country":        q.get("incorpin"),  # incorporated in
            "fiscal_year_end": q.get("fiscal_year_end"),
            "chairman":       q.get("chairman"),
            "shares_outstanding": _comma_int(q.get("amt_os")),
            "lot_size":       _maybe_float(q.get("lot")),
            "primary_exchange": q.get("primaryexch"),
        }
        if not profile["name"] and not profile["ric"]:
            raise ValueError("HKEX quote had no issuer_name or ric")
        return profile, "", "", raw_id

    # ── Valuation (a couple of fields come from the same quote payload) ──

    def _fetch_dividend_yield(self, ticker: str):
        q = self._quote(ticker)
        raw_id = persist_raw(self.name, ticker, "dividend_yield", q)
        v = _maybe_float(q.get("div_yield"))
        if v is None:
            raise ValueError("div_yield missing from HKEX quote")
        return v, "%", q.get("fiscal_year_end") or "", raw_id

    def _fetch_valuation_historical(self, ticker: str):
        """HKEX gives trailing EPS only — no full historical valuation.
        Emit the single-point trailing P/E if both price and EPS exist."""
        q = self._quote(ticker)
        raw_id = persist_raw(self.name, ticker, "valuation_historical", q)
        eps = _maybe_float(q.get("eps"))
        price = _maybe_float(q.get("ls"))
        if eps is None or price is None or eps == 0:
            raise ValueError("can't compute P/E (eps or price missing)")
        return ({
            "trailing_pe": round(price / eps, 2),
            "eps":          eps,
            "fiscal_year_end": q.get("fiscal_year_end"),
        }, "ratio", q.get("fiscal_year_end") or "", raw_id)

    # ── Everything else not implemented ──
    # HKEX's free widget API doesn't expose IS/BS/CF or historical bars;
    # those live in the filings PDFs at hkexnews.hk. Out of scope for
    # Day 4 — Yahoo + MS already cover Tencent / ICBC / Zijin fundamentals
    # at 10/10 in the v1 matrix.
