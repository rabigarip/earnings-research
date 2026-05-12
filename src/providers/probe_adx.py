"""
Abu Dhabi Securities Exchange (ADX) probe provider — Stage 1.

Hits the public REST API at `apigateway.adx.ae` (the same endpoints
www.adx.ae's JS uses). Auth is a static API key extracted from their
site headers — public knowledge, embedded in their site JS.

The big design constraint: ADX rate-limits aggressively per-IP after
~2-3 requests. The provider mitigates with:

1. Single-flight lock — only one request in flight at a time
2. 8-second minimum gap between API calls (much higher than other
   providers; ADX is the most aggressive rate-limiter we've found)
3. Retry-with-backoff on empty bodies: 30s, 60s, 120s, then give up
4. Disk caching with 24h TTL — listed-companies is hit ONCE per day
5. Targeted shape: most fields read from the same 1-2 endpoints
   (listed-companies + securityBoards), so the cache amortises hard

Endpoints we use:
  /adx/lookups/1.1/data/listed-companies   → company profile
  /adx/marketwatch/1.1/securityBoards/mainMarket → price + volume

Endpoints we DON'T have (returns not_implemented):
  - Historical financials (income statement / balance sheet / cash flow)
    These require ADX's separate "Issuer Disclosures" portal which
    is HTML-only and Playwright-required. Out of scope for Day 2.
  - Historical price bars — ADX has a chart endpoint but it requires
    a per-security token captured at runtime. TODO.

So the ADX provider's job for Stage 1 is to fill `current_price`,
`market_cap`, and `company_profile` for ADCB + ADNOC Drilling. Full
financial-statement coverage falls to the IR-PDF fallback on Day 5.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import requests

from src.services.probe_harness import Provider, persist_raw, cache_root
from src.storage.db import load_company


# ── Public API key (extracted from the live www.adx.ae site) ──
# Anyone can read this from the site JS; we just record it here so
# the provider doesn't need a runtime extraction step.
_API_KEY = "1863a94c-582b-46f9-b4f0-0d02c0cc5307"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
    "channel-id": "OSS WEB",
    "x-correlation-id": "uuid",
    "x-uuid": "",
    "adx-gateway-apikey": _API_KEY,
    "Referer": "https://www.adx.ae/",
    "Origin": "https://www.adx.ae",
}

_BASE = "https://apigateway.adx.ae"

# Single-flight lock + minimum-gap rate-limiter. Class-level (shared
# across all ADX provider instances in one process).
_REQ_LOCK = threading.Lock()
_MIN_GAP_SECONDS = 8.0
_last_call_ts: float = 0.0


def _rate_limit():
    """Block until at least _MIN_GAP_SECONDS has passed since the
    last ADX API call from this process. Uses a class-level lock so
    a parallel test run can't race the gap."""
    global _last_call_ts
    with _REQ_LOCK:
        elapsed = time.monotonic() - _last_call_ts
        if elapsed < _MIN_GAP_SECONDS:
            time.sleep(_MIN_GAP_SECONDS - elapsed)
        _last_call_ts = time.monotonic()


def _adx_get(path: str, retries: int = 3, backoff: tuple = (30, 60, 120)) -> Optional[dict]:
    """Single rate-limited GET with retry-on-empty-body.

    ADX's failure mode under rate-limit is HTTP 200 with empty body
    (not 429, not an error code) — so we treat empty as transient
    and back off. Returns the parsed JSON or None after retries
    exhausted."""
    url = f"{_BASE}{path}"
    for attempt in range(retries + 1):
        _rate_limit()
        try:
            r = requests.get(url, headers=_HEADERS, timeout=20)
        except requests.RequestException:
            if attempt == retries:
                return None
            time.sleep(backoff[min(attempt, len(backoff) - 1)])
            continue
        if r.status_code == 200 and r.text.strip():
            try:
                return r.json()
            except ValueError:
                pass
        if attempt < retries:
            time.sleep(backoff[min(attempt, len(backoff) - 1)])
    return None


# ── Disk-backed cache ──
# We cache full endpoint responses (not per-ticker) since listed-
# companies and marketwatch are universal lists. Cache TTL is 24h
# for listed-companies (changes rarely) and 1h for securityBoards
# (intraday prices update during market hours).

def _cache_path(endpoint_key: str) -> Path:
    return cache_root() / "adx" / f"{endpoint_key}.json"


def _read_cache(endpoint_key: str, ttl_hours: float) -> Optional[dict]:
    p = _cache_path(endpoint_key)
    if not p.exists():
        return None
    age = (
        datetime.now(timezone.utc)
        - datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    )
    if age > timedelta(hours=ttl_hours):
        return None
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(endpoint_key: str, payload: dict) -> None:
    p = _cache_path(endpoint_key)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2, default=str))


# ── High-level fetchers (cache + API combined) ──


def _fetch_listed_companies() -> Optional[list]:
    """Returns the list of all ADX-listed companies (~75 rows).

    Cached for 24h — listed-companies changes only on IPO/delisting.
    Response shape (captured during discovery): top-level dict with
    `response` containing `companies` (list of dicts with `symbol`,
    `nameEn`, etc.). We defensively handle multiple plausible shapes."""
    cache_key = "listed_companies"
    cached = _read_cache(cache_key, ttl_hours=24)
    if cached is not None:
        return cached.get("_companies")

    payload = _adx_get("/adx/lookups/1.1/data/listed-companies")
    if payload is None:
        return None

    # Extract the companies list defensively (shape may vary).
    resp = payload.get("response") if isinstance(payload, dict) else None
    companies = None
    if isinstance(resp, dict):
        # observed: response.companies
        companies = resp.get("companies")
        if not isinstance(companies, list):
            # fall back: any list-valued key
            for v in resp.values():
                if isinstance(v, list) and v:
                    companies = v
                    break
    elif isinstance(resp, list):
        companies = resp

    if not isinstance(companies, list):
        return None

    _write_cache(cache_key, {"_companies": companies, "_fetched_at": datetime.now(timezone.utc).isoformat()})
    return companies


def _fetch_security_board() -> Optional[list]:
    """Returns the main-market price board (~75 rows with price/volume).

    Cached for 1h — intraday prices change but we don't need real-time
    for Stage 1 coverage."""
    cache_key = "security_board"
    cached = _read_cache(cache_key, ttl_hours=1)
    if cached is not None:
        return cached.get("_results")

    payload = _adx_get("/adx/marketwatch/1.1/securityBoards/mainMarket")
    if payload is None:
        return None

    resp = payload.get("response") if isinstance(payload, dict) else None
    results = None
    if isinstance(resp, dict):
        results = resp.get("results")
        if not isinstance(results, list):
            for v in resp.values():
                if isinstance(v, list) and v:
                    results = v
                    break
    elif isinstance(resp, list):
        results = resp

    if not isinstance(results, list):
        return None

    _write_cache(cache_key, {"_results": results, "_fetched_at": datetime.now(timezone.utc).isoformat()})
    return results


def _norm_symbol(t: str) -> str:
    """company_master uses suffix-style tickers (ADCB.AE). ADX's API
    uses the bare symbol (ADCB). Strip the .AE suffix and uppercase."""
    return (t or "").upper().replace(".AE", "").strip()


def _find_in_list(items: list[dict], symbol: str) -> Optional[dict]:
    """Match by symbol (exact) or company name (substring).

    Field names confirmed against the live ADX securityBoards/mainMarket
    response: rows have `companySymbol` ("ADCB" / "ADNOCDRILL"),
    `companyID` (full name like "ADNOC Drilling Company PJSC"), and
    `companyISIN`. We also accept the alternate `symbol`/`nameEn` keys
    in case ADX evolves the shape.
    """
    sym = _norm_symbol(symbol)
    for it in items:
        if not isinstance(it, dict):
            continue
        for key in ("companySymbol", "symbol", "Symbol", "displaySecCode"):
            if str(it.get(key, "")).upper() == sym:
                return it
    for it in items:
        if not isinstance(it, dict):
            continue
        for key in ("companyID", "nameEn", "NameEn", "issuerNameEn"):
            name = str(it.get(key, "")).upper()
            if name and (sym in name or _approximate_match(sym, name)):
                return it
    return None


def _approximate_match(sym: str, name: str) -> bool:
    """e.g. ADNOCDRILL → 'ADNOC DRILLING'. Strip non-alphanumerics and
    compare prefix-style."""
    a = "".join(c for c in sym if c.isalnum())
    b = "".join(c for c in name if c.isalnum())
    if not a or not b:
        return False
    return a in b or b in a


# ── Provider ──


class ADXProvider(Provider):
    name = "adx"

    def __init__(self):
        self._companies: Optional[list] = None
        self._board: Optional[list] = None

    def _companies_list(self) -> list:
        if self._companies is None:
            self._companies = _fetch_listed_companies() or []
        return self._companies

    def _board_list(self) -> list:
        if self._board is None:
            self._board = _fetch_security_board() or []
        return self._board

    # ── Identity ──

    def _fetch_current_price(self, ticker: str):
        board = self._board_list()
        row = _find_in_list(board, ticker)
        if not row:
            raise ValueError(f"{ticker} not on ADX main market board")
        raw_id = persist_raw(self.name, ticker, "current_price", row)
        # Field name from live capture: `last` is the current price.
        # Fall back to other plausible keys in case the shape evolves.
        for key in ("last", "price", "lastPrice", "currentPrice", "closePrice"):
            v = row.get(key)
            if v is not None:
                try:
                    return float(v), "AED", "", raw_id
                except (TypeError, ValueError):
                    pass
        raise ValueError("no price field found in board row")

    def _fetch_market_cap(self, ticker: str):
        board = self._board_list()
        row = _find_in_list(board, ticker)
        if not row:
            raise ValueError(f"{ticker} not on ADX main market board")
        raw_id = persist_raw(self.name, ticker, "market_cap", row)
        for key in ("marketCap", "MarketCap", "marketCapitalization", "cap"):
            v = row.get(key)
            if v is not None:
                try:
                    return float(v), "AED", "", raw_id
                except (TypeError, ValueError):
                    pass
        raise ValueError("no market_cap field found in board row")

    def _fetch_company_profile(self, ticker: str):
        # Profile data is ALSO in the security_board response (companyID
        # = full name, companyISIN). The dedicated listed-companies
        # endpoint adds sector/industry but is rate-limited harder, so
        # we use board-data first and fall back to listed-companies
        # only when the board row is missing those fields.
        board = self._board_list()
        row = _find_in_list(board, ticker)
        if not row:
            # Maybe ticker is delisted from main market; try listed-companies.
            companies = self._companies_list()
            row = _find_in_list(companies, ticker)
            if not row:
                raise ValueError(f"{ticker} not in ADX board or listed-companies")
        raw_id = persist_raw(self.name, ticker, "company_profile", row)
        profile = {
            "name":     row.get("companyID") or row.get("nameEn") or row.get("NameEn"),
            "symbol":   row.get("companySymbol") or row.get("symbol") or row.get("displaySecCode"),
            "isin":     row.get("companyISIN") or row.get("isin") or row.get("ISIN"),
            "sector":   row.get("sector") or row.get("Sector") or row.get("sectorNameEn"),
            "industry": row.get("industry") or row.get("industryNameEn"),
            "country":  "UAE",
            "currency": "AED",
        }
        if not (profile["name"] or profile["symbol"]):
            raise ValueError("board / listed-companies row had no recognized fields")
        return profile, "", "", raw_id

    # ── Everything else is not implemented yet ──
    # ADX exposes more data (issuer disclosures, financials, historical
    # bars) but those live on different portals that require Playwright
    # or per-security tokens. Out of scope for Day 2; revisit in Stage 2
    # or via the IR-PDF fallback (Day 5).
