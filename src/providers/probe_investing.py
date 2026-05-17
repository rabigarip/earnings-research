"""Investing.com probe provider — HTTP-only via curl_cffi.

Rewritten 2026-05 to drop Playwright. Cloudflare's bot challenge is bypassed
by curl_cffi's Chrome TLS impersonation, which means this module works on
any Python host (Render, local, CI) without a Chromium install.

Source of truth on each Investing equity page is the `<script id="__NEXT_DATA__">`
JSON blob — every store (equityStore, companyProfileStore, consensusEstimatesStore,
earningsStore) is serialized inside it. We parse the JSON directly rather than
scraping rendered text, which makes parsing far more stable.

Caching: 24h disk cache keyed by slug + page-kind so re-probes are zero-network.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

from src.services.probe_harness import Provider, persist_raw, cache_root


# ── Slug map ─────────────────────────────────────────────────────────────
# Curated Investing.com slug per Yahoo ticker. Add new tickers here after
# verifying the slug at https://www.investing.com/equities/<slug>.
_SLUGS: dict[str, str] = {
    # Saudi / Tadawul
    "2222.SR":        "saudi-aramco",
    "2020.SR":        "sa-fertilizers",
    # UAE / ADX + DFM (subject + peer tickers used in slide-3 peer table)
    "ADCB.AE":        "ad-commercial",
    "ADNOCDRILL.AE":  "adnoc-drilling",
    "ENBD.AE":        "emirates-nbd",            # Emirates NBD (peer)
    "FAB.AE":         "natl-bk-of-ad",           # First Abu Dhabi Bank (peer)
    "DIB.AE":         "db-islamic-bk",           # Dubai Islamic Bank (peer)
    # Oman / MSM
    "BKMB.OM":        "bank-muscat",
    "OQEP.OM":        "oq-exploration-and-production-cjsc",
    # Qatar / Kuwait (peer tickers)
    "QNBK.QA":        "qnb",                    # Qatar National Bank
    "NBKK.KW":        "national-bank-kt",       # National Bank of Kuwait
    # India / NSE
    "JINDALSTEL.NS":  "jindal-steel---power",
    "ICICIBANK.NS":   "icici-bank-ltd",   # NSE listing in INR (not 'icici-bank' = IBN ADR in USD)
    "ICICIBANK.BO":   "icici-bank-ltd",
    # China / Hong Kong
    "0700.HK":        "tencent-holdings-hk",
    "2899.HK":        "zijin-mining-group",
    "1398.HK":        "icbc",
}


def _slug(ticker: str) -> Optional[str]:
    return _SLUGS.get(ticker.upper())


# ── HTTP layer (curl_cffi) ───────────────────────────────────────────────

_BASE = "https://www.investing.com/equities"


def _get(url: str, *, timeout: float = 15.0) -> Optional[str]:
    """Single HTTP GET via curl_cffi with Chrome120 TLS fingerprint. Returns
    the response body string on 200, or None on any failure. Cloudflare's
    automated-traffic check is satisfied by the TLS fingerprint alone — no
    cookie or challenge solver is needed."""
    try:
        from curl_cffi import requests as cr
    except ImportError:
        return None
    try:
        r = cr.get(url, impersonate="chrome120", timeout=timeout,
                   headers={"Accept-Language": "en-US,en;q=0.9"})
    except Exception:
        return None
    if r.status_code != 200:
        return None
    return r.text


def _next_data(html: str) -> Optional[dict]:
    """Parse the __NEXT_DATA__ JSON blob; return the `pageProps.state` dict
    (with every store JSON-decoded). Returns None on shape problems."""
    if not html:
        return None
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        return None
    try:
        root = json.loads(m.group(1))
    except json.JSONDecodeError:
        return None
    state = (root.get("props") or {}).get("pageProps", {}).get("state") or {}
    out: dict[str, Any] = {}
    for key, raw in state.items():
        if isinstance(raw, str):
            try:
                out[key] = json.loads(raw)
            except json.JSONDecodeError:
                out[key] = raw
        else:
            out[key] = raw
    return out


# ── Disk cache ───────────────────────────────────────────────────────────

def _cache_dir() -> Path:
    return cache_root() / "investing"


def _tracked_dir() -> Path:
    """Repo-tracked Investing snapshot dir. Pre-warmed locally and committed
    so the Render deploy (whose egress IP is Cloudflare-blocked by
    Investing.com) reads from it instead of failing on the network call.
    Refreshed via `python -m scripts.refresh_investing_cache`."""
    from src.config import root
    return root() / "data" / "investing"


def _cache_path(slug: str, kind: str) -> Path:
    return _cache_dir() / f"{slug}__{kind}.json"


def _tracked_path(slug: str, kind: str) -> Path:
    return _tracked_dir() / f"{slug}__{kind}.json"


def _read_cache(slug: str, kind: str, ttl_hours: float = 24) -> Optional[dict]:
    p = _cache_path(slug, kind)
    if not p.exists():
        return None
    age = datetime.now(timezone.utc) - datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    if age > timedelta(hours=ttl_hours):
        return None
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _read_tracked(slug: str, kind: str) -> Optional[dict]:
    """Read the repo-committed snapshot for a slug/kind, or None if absent.
    No TTL — the snapshot is treated as the authoritative-for-now fallback
    when the network is unreachable from this host."""
    p = _tracked_path(slug, kind)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(slug: str, kind: str, payload: dict) -> None:
    p = _cache_path(slug, kind)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(payload, indent=2, default=str))
    except (OSError, PermissionError):
        # Render's project dir is read-only; cache write failure is non-fatal.
        pass


def _write_tracked(slug: str, kind: str, payload: dict) -> None:
    """Write a snapshot to the repo-tracked dir (used by the refresh script
    only — not from request-path code, since the deployed host is read-only)."""
    p = _tracked_path(slug, kind)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2, default=str))


# ── Page-level fetchers (cached) ─────────────────────────────────────────

def _fetch_with_fallback(slug: str, kind: str, url_suffix: str) -> Optional[dict]:
    """Three-layer fetch:
      1. 24h fresh cache (cache/probe/investing/...)
      2. Network via curl_cffi (Cloudflare bypass via TLS impersonation)
      3. Repo-tracked snapshot (data/investing/...) — used when the runtime
         egress IP is blocked by Cloudflare (e.g. Render's cloud range).
    Returns the decoded __NEXT_DATA__ state dict or None when all three miss.
    """
    cached = _read_cache(slug, kind)
    if cached:
        return cached
    html = _get(f"{_BASE}/{slug}{url_suffix}")
    state = _next_data(html) if html else None
    if state:
        _write_cache(slug, kind, state)
        return state
    # Network failed (Cloudflare 403 etc.). Try the repo-tracked snapshot.
    tracked = _read_tracked(slug, kind)
    if tracked:
        return tracked
    return None


def _fetch_equity_page(slug: str) -> Optional[dict]:
    return _fetch_with_fallback(slug, "equity", "")


def _fetch_consensus_page(slug: str) -> Optional[dict]:
    return _fetch_with_fallback(slug, "consensus", "-consensus-estimates")


def _fetch_earnings_page(slug: str) -> Optional[dict]:
    return _fetch_with_fallback(slug, "earnings", "-earnings")


# ── Field extractors ─────────────────────────────────────────────────────

def _equity_instrument(state: dict) -> dict:
    eq = state.get("equityStore") or {}
    return (eq.get("instrument") or {}) if isinstance(eq, dict) else {}


def _equity_price(state: dict) -> dict:
    instr = _equity_instrument(state)
    return instr.get("price") or {}


def _equity_fundamental(state: dict) -> dict:
    instr = _equity_instrument(state)
    return instr.get("fundamental") or {}


def _equity_key_metrics(state: dict) -> dict:
    eq = state.get("equityStore") or {}
    return (eq.get("keyMetrics") or {}) if isinstance(eq, dict) else {}


def _equity_price_changes(state: dict) -> dict:
    eq = state.get("equityStore") or {}
    return (eq.get("priceChanges") or {}) if isinstance(eq, dict) else {}


def _company_profile(state: dict) -> dict:
    cp = state.get("companyProfileStore") or {}
    return (cp.get("profile") or {}) if isinstance(cp, dict) else {}


def _forecast_summary(state: dict) -> dict:
    ce = state.get("consensusEstimatesStore") or {}
    return (ce.get("forecastSummary") or {}) if isinstance(ce, dict) else {}


def _earnings_forecasts(state: dict) -> list[dict]:
    es = state.get("earningsStore") or {}
    fc = es.get("forecasts") if isinstance(es, dict) else None
    return fc if isinstance(fc, list) else []


def _earnings_history(state: dict) -> list[dict]:
    """Historical earnings rows with surprise %, used for the surprise-track
    line. Investing's `earnings` list includes reported vs estimated EPS."""
    es = state.get("earningsStore") or {}
    eh = es.get("earnings") if isinstance(es, dict) else None
    return eh if isinstance(eh, list) else []


# ── Historical-prices helper (used by the deck writer directly, not as a
#   Provider field; lives here for proximity to the rest of the Investing
#   plumbing). ─────────────────────────────────────────────────────────────

def fetch_historical_prices(ticker: str, *, days: int = 380) -> Optional[dict]:
    """Return a 52-week close series for `ticker` from Investing.com.

    Output matches the canonical `historical_prices` shape the slide
    renderers already consume:
        {
          "close_series": [{"date": "YYYY-MM-DD", "close": float}, ...],
          "range_52w_low":  float,
          "range_52w_high": float,
          "perf_1d":  float, "perf_1w": float, "perf_1m": float,
          "perf_3m":  float, "perf_6m": float, "perf_ytd": float,
        }

    Returns None when the ticker has no Investing.com slug or the API
    rejects the request.
    """
    slug = _slug(ticker)
    if not slug:
        return None
    # Disk cache (24h) -> tracked snapshot (no TTL) -> network.
    cached = _read_cache(slug, "historical")
    if cached:
        return cached
    # First fetch the equity page once to grab the instrumentId.
    state = _fetch_equity_page(slug)
    if not state:
        # No equity page at all — fall back to tracked historical if present.
        return _read_tracked(slug, "historical")
    eq = state.get("equityStore") or {}
    iid = eq.get("instrumentId") if isinstance(eq, dict) else None
    if not iid:
        instr = _equity_instrument(state)
        iid = (instr.get("price") or {}).get("instrumentId")
    if not iid:
        return _read_tracked(slug, "historical")
    from datetime import datetime as _dt, timedelta as _td
    end = _dt.utcnow().date()
    start = end - _td(days=days)
    # api.investing.com requires a `domain-id` header (the public endpoints
    # validate against the WAF "Request.Domain" field) — calling without it
    # returns 400. Bypass _get and set the header explicitly.
    url = (
        f"https://api.investing.com/api/financialdata/historical/{iid}"
        f"?start-date={start.isoformat()}&end-date={end.isoformat()}"
        f"&time-frame=Daily&add-missing-rows=false"
    )
    try:
        from curl_cffi import requests as cr
    except ImportError:
        return None
    try:
        r = cr.get(url, impersonate="chrome120", timeout=15,
                   headers={"domain-id": "www",
                            "Accept-Language": "en-US,en;q=0.9"})
    except Exception:
        return _read_tracked(slug, "historical")
    if r.status_code != 200:
        return _read_tracked(slug, "historical")
    try:
        rows = (json.loads(r.text).get("data") or [])
    except json.JSONDecodeError:
        return _read_tracked(slug, "historical")
    def _to_float(v):
        if v is None: return None
        if isinstance(v, (int, float)): return float(v)
        try: return float(str(v))
        except (TypeError, ValueError): return None

    series: list[dict] = []
    for r in rows:
        ts = r.get("rowDateTimestamp") or ""
        close = _to_float(r.get("last_closeRaw"))
        if not ts or close is None:
            continue
        # API returns newest-first; sort oldest-first below.
        series.append({"date": ts[:10], "close": close})
    series.sort(key=lambda x: x["date"])
    if not series:
        return None
    closes = [x["close"] for x in series]
    out: dict[str, Any] = {
        "close_series":   series,
        "range_52w_low":  min(closes),
        "range_52w_high": max(closes),
    }
    # Perf bands — anchor everything off the latest close to keep them
    # internally consistent. Falls back silently when a band is shorter
    # than the requested window (e.g. recent IPO).
    today_close = closes[-1]
    today_date  = series[-1]["date"]
    def _close_at_or_before(target_date_str: str) -> Optional[float]:
        for entry in reversed(series):
            if entry["date"] <= target_date_str:
                return entry["close"]
        return None
    from datetime import date as _date
    today_d = _date.fromisoformat(today_date)
    deltas = {
        "perf_1d":  1, "perf_1w": 7, "perf_1m": 30,
        "perf_3m":  90, "perf_6m": 182, "perf_ytd": 0,
    }
    for key, delta_days in deltas.items():
        if key == "perf_ytd":
            anchor_date = _date(today_d.year, 1, 1).isoformat()
        else:
            anchor_date = (today_d - _td(days=delta_days)).isoformat()
        prev = _close_at_or_before(anchor_date)
        if prev and prev > 0:
            out[key] = (today_close / prev - 1.0) * 100
    # Persist successful network fetches to the disk cache so subsequent
    # calls hit (free local) instead of (paid network) — and so the local
    # refresh script can collect them all for committing to data/investing.
    try:
        _write_cache(slug, "historical", out)
    except Exception:
        pass
    return out


# ── Public Provider ──────────────────────────────────────────────────────

class InvestingProvider(Provider):
    name = "investing"

    def __init__(self):
        # Per-process in-memory cache. Disk cache (24h) sits underneath.
        self._mem: dict[str, dict] = {}

    def _state(self, ticker: str, kind: str) -> dict:
        slug = _slug(ticker)
        if not slug:
            raise NotImplementedError(f"No Investing.com slug for {ticker}")
        key = f"{slug}::{kind}"
        if key in self._mem:
            return self._mem[key]
        if kind == "equity":
            state = _fetch_equity_page(slug)
        elif kind == "consensus":
            state = _fetch_consensus_page(slug)
        elif kind == "earnings":
            state = _fetch_earnings_page(slug)
        else:
            raise ValueError(f"unknown kind {kind!r}")
        if not state:
            raise ValueError(f"Investing.com {kind} page returned no usable data for {ticker}")
        self._mem[key] = state
        return state

    # ── Required value-fetching methods (Provider interface) ─────────────

    def _fetch_current_price(self, ticker: str):
        state = self._state(ticker, "equity")
        price = _equity_price(state)
        last = price.get("last")
        if not isinstance(last, (int, float)):
            raise ValueError("equity page had no `last` price")
        raw_id = persist_raw(self.name, ticker, "current_price", price)
        return float(last), (price.get("currency") or ""), "", raw_id

    def _fetch_market_cap(self, ticker: str):
        """Market cap in raw listing-currency units (e.g. HKD for HK names).

        Important for HK tickers where MarketScreener appears to report only
        the H-share count, underestimating total market cap by ~13%.
        Investing's fundamental.marketCapRaw includes both A- and H-share
        floats and matches the live equity page.
        """
        state = self._state(ticker, "equity")
        fund = _equity_fundamental(state)
        mcap = fund.get("marketCapRaw")
        if not isinstance(mcap, (int, float)) or mcap <= 0:
            raise ValueError("equity page had no marketCapRaw")
        price = _equity_price(state)
        currency = (price.get("currency") or "").upper()
        raw_id = persist_raw(self.name, ticker, "market_cap", fund)
        return float(mcap), currency, "", raw_id

    def _fetch_dividend_yield(self, ticker: str):
        state = self._state(ticker, "equity")
        fundamental = _equity_fundamental(state)
        # Investing's fundamental block exposes the yield (in percent, already
        # scaled) under the key `yield`. The legacy names are kept as fallbacks
        # so a future Investing schema change doesn't break this provider.
        for key in ("yield", "dividend_yield", "dividendYield", "div_yield"):
            v = fundamental.get(key)
            if isinstance(v, (int, float)) and v > 0:
                return float(v), "%", "", persist_raw(self.name, ticker, "dividend_yield", fundamental)
        km = _equity_key_metrics(state)
        for key in ("dividendYield", "dividend_yield", "yield"):
            v = km.get(key)
            if isinstance(v, (int, float)) and v > 0:
                return float(v), "%", "", persist_raw(self.name, ticker, "dividend_yield", km)
        raise ValueError("dividend yield not in equity page fundamentals")

    def _fetch_target_price(self, ticker: str):
        state = self._state(ticker, "consensus")
        fs = _forecast_summary(state)
        mean = fs.get("target_price_consensus_mean")
        if not isinstance(mean, (int, float)):
            raise ValueError("Investing.com consensus page had no target_price_consensus_mean")
        raw_id = persist_raw(self.name, ticker, "target_price", fs)
        return {
            "mean": float(mean),
            "high": fs.get("target_price_consensus_high"),
            "low":  fs.get("target_price_consensus_low"),
            "n_analysts": fs.get("number_of_estimates")
                          or sum(int(fs.get(k) or 0) for k in (
                              "number_of_analysts_buy", "number_of_analysts_hold",
                              "number_of_analysts_sell")),
        }, "", "", raw_id

    def _fetch_rating_split(self, ticker: str):
        state = self._state(ticker, "consensus")
        fs = _forecast_summary(state)
        buy  = int(fs.get("number_of_analysts_buy")  or 0)
        hold = int(fs.get("number_of_analysts_hold") or 0)
        sell = int(fs.get("number_of_analysts_sell") or 0)
        if not (buy or hold or sell):
            raise ValueError("Investing.com consensus page had no analyst counts")
        consensus = (fs.get("consensus_recommendation") or "").strip() or None
        if not consensus:
            total = max(1, buy + hold + sell)
            if buy / total >= 0.6:   consensus = "BUY"
            elif sell / total >= 0.4: consensus = "SELL"
            elif buy > sell:          consensus = "OUTPERFORM"
            else:                     consensus = "HOLD"
        raw_id = persist_raw(self.name, ticker, "rating_split", fs)
        return {
            "buy": buy, "hold": hold, "sell": sell,
            "total": buy + hold + sell, "consensus": consensus,
        }, "", "", raw_id

    def _fetch_valuation_forward(self, ticker: str):
        state = self._state(ticker, "earnings")
        forecasts = _earnings_forecasts(state)
        if not forecasts:
            raise ValueError("Investing.com earnings page had no forecasts")
        # Sort forecasts in calendar order so we know which is the next print
        # and which roll up into FY+1 / FY+2 totals.
        rows = sorted(
            (f for f in forecasts if isinstance(f.get("reportYear"), int)),
            key=lambda f: (f["reportYear"], f.get("reportMonth") or 0),
        )
        if not rows:
            raise ValueError("Investing.com forecasts had no parseable rows")
        nxt = rows[0]
        # FY aggregates: sum the first 4 quarters that fall in the same year
        # as the next-Q's row (or the next 4 calendar quarters if year split).
        def _fy_agg(year: int) -> tuple[Optional[float], Optional[float]]:
            year_rows = [r for r in rows if r.get("reportYear") == year]
            if not year_rows:
                return None, None
            rev = sum(r["revenue"] for r in year_rows if isinstance(r.get("revenue"), (int, float))) or None
            eps = sum(r["eps"]     for r in year_rows if isinstance(r.get("eps"),     (int, float))) or None
            return rev, eps
        fy1_year = nxt["reportYear"]
        fy2_year = fy1_year + 1
        rev_fy1, eps_fy1 = _fy_agg(fy1_year)
        rev_fy2, eps_fy2 = _fy_agg(fy2_year)
        # "Period" string Investing implies: Q from reportMonth.
        rm = nxt.get("reportMonth") or 0
        qn = ((rm - 1) // 3 + 1) if rm else 0
        next_q_period = f"Q{qn} {nxt['reportYear']}" if qn else ""
        raw_id = persist_raw(self.name, ticker, "valuation_forward", {"forecasts": rows})
        return {
            "fy1_year":   fy1_year,
            "eps_fy1":    eps_fy1,
            "revenue_fy1": rev_fy1,
            "fy2_year":   fy2_year,
            "eps_fy2":    eps_fy2,
            "revenue_fy2": rev_fy2,
            "next_q_period":  next_q_period,
            "next_q_report_date": None,
            "eps_next_q":     nxt.get("eps"),
            "revenue_next_q": nxt.get("revenue"),
        }, "", "", raw_id

    def _fetch_income_statement_quarterly(self, ticker: str):
        """Surprise history — used by the thesis renderer as a track-record
        anchor. Investing's `earnings` list pre-computes the surprise%."""
        state = self._state(ticker, "earnings")
        history = _earnings_history(state)
        if not history:
            raise ValueError("Investing.com earnings page had no history rows")
        # Normalize to the shape the renderer already consumes (period_end,
        # eps_actual, eps_estimate, eps_surprise_pct, revenue_actual,
        # revenue_estimate, revenue_surprise_pct).
        out: list[dict] = []
        for row in history:
            if not isinstance(row, dict):
                continue
            yr = row.get("reportYear")
            mo = row.get("reportMonth") or 0
            qn = ((mo - 1) // 3 + 1) if isinstance(mo, int) and mo else 0
            period = f"Q{qn} {yr}" if qn and yr else ""
            out.append({
                "period": period,
                "eps_actual":    row.get("eps") or row.get("epsActual"),
                "eps_estimate":  row.get("epsForecast") or row.get("epsEstimate"),
                "eps_surprise_pct":      row.get("epsSurprisePct"),
                "revenue_actual":        row.get("revenue") or row.get("revenueActual"),
                "revenue_estimate":      row.get("revenueForecast") or row.get("revenueEstimate"),
                "revenue_surprise_pct":  row.get("revenueSurprisePct"),
            })
        if not out:
            raise ValueError("Investing.com history could not be normalised")
        raw_id = persist_raw(self.name, ticker, "income_statement_quarterly", {"surprise_history": out})
        return {"surprise_history": out}, "", "", raw_id
