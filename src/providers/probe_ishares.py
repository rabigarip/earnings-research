"""
iShares ETF proxy provider — Stage 2.

Surfaces regional / asset-class return overlays from BlackRock's public
iShares pages. The ETFs are used as cheap, free, well-tracked proxies
for "what did the EM benchmark do in this period" — useful on Slide 3
to contextualize a single-name return vs. its regional ETF.

ETF → benchmark mapping for our panel:
  EEM   — iShares MSCI Emerging Markets       (broad EM context)
  MCHI  — iShares MSCI China                  (HK/China names)
  INDA  — iShares MSCI India                  (BSE/NSE names)
  KSA   — iShares MSCI Saudi Arabia           (Tadawul names)
  UAE   — iShares MSCI UAE                    (ADX names)
  EMB   — iShares JPM USD EM Bond             (fixed-income overlay)

Source path: we use the iShares "performance" snippet URL which is
publicly cacheable and returns JSON. No auth, no cookies, polite rate
limit (1 call / 1.5s).

Field coverage:
  - historical_prices  → {as_of, ytd, 1m, 3m, 6m, 1y, 3y, since_inception, ...}
  - company_profile    → {name, isin, exchange, region, asset_class}

Everything else returns not_implemented.

The provider is mapped on the basis of `company_master.country` for
each ticker — i.e. a Saudi ticker maps to KSA, an Indian ticker maps
to INDA, etc. The renderer picks one or two proxies to display.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests

from src.services.probe_harness import Provider, persist_raw, cache_root


# Map country (from company_master) -> iShares ETF ticker.
# This is the lookup used by the renderer to decide which proxy to attach.
COUNTRY_TO_ETF: dict[str, str] = {
    "SA":  "KSA",   "SAU": "KSA",   "Saudi Arabia": "KSA",
    "AE":  "UAE",   "ARE": "UAE",   "United Arab Emirates": "UAE",
    "OM":  "EEM",   "OMN": "EEM",   "Oman": "EEM",         # no Oman ETF; EEM is the broad proxy
    "IN":  "INDA",  "IND": "INDA",  "India": "INDA",
    "CN":  "MCHI",  "CHN": "MCHI",  "China": "MCHI",
    "HK":  "MCHI",  "HKG": "MCHI",  "Hong Kong": "MCHI",
}

# Fallback used when the country isn't mapped — broad-EM exposure.
_DEFAULT_PROXY = "EEM"

# Public performance snippet endpoint, returns JSON.
_BASE = "https://www.ishares.com/us/products"
_PRODUCT_IDS = {
    "EEM":  "239637",
    "MCHI": "239619",
    "INDA": "239664",
    "KSA":  "270902",
    "UAE":  "260530",
    "EMB":  "239572",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,*/*",
}

_MIN_GAP = 1.5
_last_call: float = 0.0


def _rate_limit():
    global _last_call
    gap = time.monotonic() - _last_call
    if gap < _MIN_GAP:
        time.sleep(_MIN_GAP - gap)
    _last_call = time.monotonic()


def _cache_path(etf: str) -> Path:
    return cache_root() / "ishares" / f"{etf}.json"


def _read_cache(etf: str, ttl_hours: float = 24) -> Optional[dict]:
    p = _cache_path(etf)
    if not p.exists():
        return None
    age = datetime.now(timezone.utc) - datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    if age > timedelta(hours=ttl_hours):
        return None
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(etf: str, payload: dict) -> None:
    p = _cache_path(etf)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2, default=str))


def _fetch_etf_payload(etf: str) -> Optional[dict]:
    """Hit the iShares public landing page for the ETF and pull the
    embedded JSON blob. The page exposes monthly returns + YTD + 1y/3y/5y
    in a `<script type="application/json">` shaped blob.

    For robustness we just regex out the relevant numeric values from the
    rendered HTML — the embedded blob format has changed twice in 18 months."""
    import re
    cached = _read_cache(etf)
    if cached:
        return cached

    pid = _PRODUCT_IDS.get(etf)
    if not pid:
        return None
    url = f"https://www.ishares.com/us/products/{pid}/"
    _rate_limit()
    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    html = r.text

    def _grab(pattern):
        m = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            return None
        try:
            return float(m.group(1).replace(",", ""))
        except (TypeError, ValueError):
            return None

    payload = {
        "etf": etf,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "name": None,
        "ytd_pct": _grab(r'"ytdReturn"[^"\d-]*([-]?\d+\.\d+)'),
        "1m_pct":  _grab(r'"oneMonthReturn"[^"\d-]*([-]?\d+\.\d+)'),
        "3m_pct":  _grab(r'"threeMonthReturn"[^"\d-]*([-]?\d+\.\d+)'),
        "1y_pct":  _grab(r'"oneYearReturn"[^"\d-]*([-]?\d+\.\d+)'),
        "3y_pct":  _grab(r'"threeYearAnnualizedReturn"[^"\d-]*([-]?\d+\.\d+)'),
        "since_inception_pct": _grab(r'"sinceInceptionAnnualizedReturn"[^"\d-]*([-]?\d+\.\d+)'),
        "nav": _grab(r'"navAmount"[^"\d-]*([-]?\d+\.\d+)'),
        "url": url,
    }
    name_match = re.search(r'<title>([^<]+)</title>', html)
    if name_match:
        payload["name"] = name_match.group(1).strip()

    # Only cache if we got at least one meaningful field
    if any(v is not None for k, v in payload.items()
            if k.endswith("_pct") or k == "nav"):
        _write_cache(etf, payload)
        return payload
    return None


def _etf_for_ticker(ticker: str) -> str:
    """Resolve a panel ticker to its regional iShares proxy.

    Lookup order:
      1. company_master.country (preferred — uses curated DB)
      2. ticker-suffix heuristic (.SR→KSA, .AE→UAE, .HK→MCHI, .BO/.NS→INDA)
      3. Default to EEM (broad EM)
    """
    try:
        from src.storage.db import load_company
        row = load_company(ticker)
        if row and row.get("country"):
            etf = COUNTRY_TO_ETF.get(row["country"])
            if etf:
                return etf
    except (ImportError, KeyError, TypeError):
        pass
    suffix = ticker.split(".")[-1].upper() if "." in ticker else ""
    by_suffix = {"SR": "KSA", "AE": "UAE", "HK": "MCHI", "BO": "INDA", "NS": "INDA"}
    return by_suffix.get(suffix, _DEFAULT_PROXY)


class iSharesProvider(Provider):
    name = "ishares"

    def __init__(self):
        # Per-ticker memo of (etf, payload) within one probe run
        self._cache: dict[str, dict] = {}

    def _etf_payload(self, ticker: str) -> dict:
        if ticker in self._cache:
            return self._cache[ticker]
        etf = _etf_for_ticker(ticker)
        payload = _fetch_etf_payload(etf)
        if not payload:
            raise ValueError(f"iShares fetch failed for ETF {etf}")
        payload["_etf_chosen"] = etf
        self._cache[ticker] = payload
        return payload

    def _fetch_historical_prices(self, ticker: str):
        """We map this to the ETF's return suite — same canonical
        field, different semantic (regional proxy returns). The slide
        consumer recognizes the `_etf_chosen` key to render as a peer
        proxy rather than a single-stock chart."""
        payload = self._etf_payload(ticker)
        raw_id = persist_raw(self.name, ticker, "historical_prices", payload)
        return payload, "%", payload.get("fetched_at", ""), raw_id

    def _fetch_company_profile(self, ticker: str):
        payload = self._etf_payload(ticker)
        raw_id = persist_raw(self.name, ticker, "company_profile", payload)
        profile = {
            "etf_proxy": payload["_etf_chosen"],
            "etf_name":  payload.get("name") or "",
            "ytd_pct":   payload.get("ytd_pct"),
            "1y_pct":    payload.get("1y_pct"),
            "kind":      "regional_proxy",
        }
        return profile, "", "", raw_id
