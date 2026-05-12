"""
NSE (India) probe provider — Stage 1.

Hits NSE's public REST API at `www.nseindia.com/api/...`. Works without
auth or cookies for the `quote-equity` endpoint — confirmed against
both ICICIBANK and JINDALSTEL in the panel. This is by far the
easiest exchange we've probed.

Endpoint shape (captured from ICICIBANK + JINDALSTEL):
  /api/quote-equity?symbol=<SYM>  → JSON with:
    - info:        {symbol, companyName, industry, isin, ...}
    - priceInfo:   {lastPrice, change, pChange, previousClose,
                    open, close, vwap, intraDayHighLow, weekHighLow, ...}
    - industryInfo:{macro, sector, industry, basicIndustry}
    - securityInfo:{boardStatus, tradingStatus, faceValue, issuedSize, ...}

What we DON'T have from NSE's free API:
  - Full income statement / balance sheet / cash flow
    (NSE links to filed PDFs but doesn't parse them; Yahoo + MS already
    cover Indian financials at 10/10, so this isn't a gap)
  - Historical price bars
    (The /historical/cm/equity endpoint returned HTML in our probe —
    needs cookies session. Out of scope for Stage 1.)

So NSE's job is `current_price`, `market_cap` (derived from
priceInfo.lastPrice × securityInfo.issuedSize), and `company_profile`.
That's enough to give the Indian panel tickers a third confirming
source alongside Yahoo + MS.

ICICI Bank in our master is `ICICIBANK.BO` (Bombay). NSE uses the
symbol `ICICIBANK` (no suffix). The provider strips both `.BO` and
`.NS` so it works whichever way the ticker is seeded.
"""

from __future__ import annotations

import time
from typing import Optional

import requests

from src.services.probe_harness import Provider, persist_raw


_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

_BASE = "https://www.nseindia.com"
_MIN_GAP = 1.0  # NSE is friendly; 1 second between calls is plenty
_last_call: float = 0.0


def _rate_limit():
    global _last_call
    elapsed = time.monotonic() - _last_call
    if elapsed < _MIN_GAP:
        time.sleep(_MIN_GAP - elapsed)
    _last_call = time.monotonic()


def _norm_symbol(t: str) -> str:
    """company_master ticker may carry .NS or .BO suffix; NSE wants the
    bare symbol."""
    return (t or "").upper().replace(".NS", "").replace(".BO", "").strip()


def _quote_equity(symbol: str) -> Optional[dict]:
    """Single rate-limited call to NSE's quote-equity endpoint."""
    _rate_limit()
    try:
        r = requests.get(
            f"{_BASE}/api/quote-equity",
            params={"symbol": symbol},
            headers=_HEADERS, timeout=15,
        )
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    ctype = r.headers.get("content-type", "")
    if not ctype.startswith("application/json"):
        return None
    try:
        return r.json()
    except ValueError:
        return None


class NSEProvider(Provider):
    name = "nse"

    def __init__(self):
        # Cache the quote dict across fields within one probe run so
        # we hit NSE at most once per ticker.
        self._cache: dict[str, Optional[dict]] = {}

    def _quote(self, ticker: str) -> dict:
        sym = _norm_symbol(ticker)
        if sym not in self._cache:
            self._cache[sym] = _quote_equity(sym)
        q = self._cache[sym]
        if not q:
            raise ValueError(f"NSE returned no data for {sym}")
        return q

    # ── Identity ──

    def _fetch_current_price(self, ticker: str):
        q = self._quote(ticker)
        raw_id = persist_raw(self.name, ticker, "current_price", q)
        price_info = q.get("priceInfo") or {}
        price = price_info.get("lastPrice")
        if price is None:
            raise ValueError("priceInfo.lastPrice missing")
        return float(price), "INR", "", raw_id

    def _fetch_market_cap(self, ticker: str):
        """NSE doesn't ship a marketCap field directly — we derive it
        from priceInfo.lastPrice × securityInfo.issuedSize. issuedSize
        is the total number of issued shares (FF + non-FF). Result is
        in INR, raw units (not millions)."""
        q = self._quote(ticker)
        raw_id = persist_raw(self.name, ticker, "market_cap", q)
        price_info = q.get("priceInfo") or {}
        sec_info = q.get("securityInfo") or {}
        price = price_info.get("lastPrice")
        issued = sec_info.get("issuedSize")
        if price is None or issued is None:
            raise ValueError("can't derive marketCap (price or issuedSize missing)")
        try:
            return float(price) * float(issued), "INR", "", raw_id
        except (TypeError, ValueError):
            raise ValueError("priceInfo / securityInfo had non-numeric values")

    def _fetch_company_profile(self, ticker: str):
        q = self._quote(ticker)
        raw_id = persist_raw(self.name, ticker, "company_profile", q)
        info = q.get("info") or {}
        industry_info = q.get("industryInfo") or {}
        profile = {
            "name":     info.get("companyName"),
            "symbol":   info.get("symbol"),
            "isin":     info.get("isin"),
            "sector":   industry_info.get("sector"),
            "industry": industry_info.get("industry") or industry_info.get("basicIndustry"),
            "macro":    industry_info.get("macro"),
            "country":  "India",
            "currency": "INR",
        }
        if not any(profile.values()):
            raise ValueError("NSE quote had no recognized profile fields")
        return profile, "", "", raw_id

    # ── Everything else is not implemented ──
    # NSE's free API doesn't expose IS/BS/CF or historical bars without
    # a cookies session. Yahoo + MS already cover Indian fundamentals
    # at 10/10 in the v1 matrix, so we accept this gap.
