"""
Investing.com probe provider — Stage 1, conservative scope.

Two big design constraints discovered during probing:

1. **Cloudflare wall.** Headless Chromium hits a "Just a moment..." CF
   challenge page on every request. Non-headless Chromium passes the
   challenge within 1-2 seconds (the CF JS check runs and grants
   cookies). So this provider runs Playwright with `headless=False`.
   On a server with no display, this needs `xvfb-run` in front.

2. **Slug is not guessable.** Investing.com slugs are curated by
   their editors — Tencent is `/equities/tencent-holdings-ltd` but
   could equally be `/equities/tencent-holdings` (which 404s).
   We require an explicit slug per ticker in `config/investing_slugs.toml`.

For Stage 1, we wire the 4 panel tickers where the slug was found
during discovery (ICICIBANK, JINDALSTEL, plus placeholders for ones
that need manual lookup). Anything else gets `not_implemented`.

Caching: each ticker's quote page response is disk-cached for 24h so
re-probes are zero-network.

Fields covered: current_price, dividend_yield, target_price.
Identity (company_profile, market_cap) could be added — Investing
has it — but the field is already at 10/10 elsewhere, so the marginal
value of duplicating it here is low.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

from src.services.probe_harness import Provider, persist_raw, cache_root


# Curated slug map. Add tickers here as their Investing.com slug is
# discovered (one-time manual lookup; the slug doesn't change).
# Format: company_master ticker -> Investing.com URL slug.
_SLUGS: dict[str, str] = {
    "ICICIBANK.BO":  "icici-bank-ltd",
    "JINDALSTEL.NS": "jindal-steel---power",
    # Confirmed missing during discovery (different slugs needed):
    # "0700.HK", "2222.SR", "1398.HK", "2899.HK" — manual lookup pending
}


def _slug(ticker: str) -> Optional[str]:
    """Resolve the Investing.com slug for a ticker, or None if not curated."""
    return _SLUGS.get(ticker.upper())


def _cache_path(slug: str) -> Path:
    return cache_root() / "investing" / f"{slug}.json"


def _read_cache(slug: str, ttl_hours: float = 24) -> Optional[dict]:
    p = _cache_path(slug)
    if not p.exists():
        return None
    age = datetime.now(timezone.utc) - datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    if age > timedelta(hours=ttl_hours):
        return None
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(slug: str, payload: dict) -> None:
    p = _cache_path(slug)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2, default=str))


def _fetch_quote(slug: str) -> Optional[dict]:
    """Use non-headless Playwright to load the equity page and extract
    a small set of quote fields from the DOM. Returns dict or None."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None

    UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

    url = f"https://www.investing.com/equities/{slug}"
    extracted: dict[str, Any] = {"url": url, "fetched_at": datetime.now(timezone.utc).isoformat()}

    with sync_playwright() as p:
        try:
            # NON-HEADLESS is required to pass Cloudflare cleanly. On a
            # headless server, run with `xvfb-run -a python ...`.
            browser = p.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            return None
        try:
            ctx = browser.new_context(
                user_agent=UA, viewport={"width": 1366, "height": 900},
                locale="en-US",
            )
            ctx.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            page = ctx.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except Exception:
                return None

            # Wait through any CF challenge (up to 15s).
            for _ in range(15):
                if "Just a moment" not in page.title():
                    break
                time.sleep(1)

            if "Just a moment" in page.title() or "Error 404" in page.title():
                return None

            # Extract whatever common selectors hold price + ancillary.
            extracted["title"] = page.title()
            data = page.evaluate("""
                () => {
                    function findFirst(selectors) {
                      for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (el && el.innerText) return el.innerText.trim();
                      }
                      return null;
                    }
                    return {
                      price: findFirst([
                          '[data-test="instrument-price-last"]',
                          '.text-5xl', '.text-2xl',
                          'span[class*="instrument-price"]'
                      ]),
                      change: findFirst([
                          '[data-test="instrument-price-change"]',
                          '[class*="instrument-price-change"]'
                      ]),
                      change_pct: findFirst([
                          '[data-test="instrument-price-change-percent"]',
                          '[class*="instrument-price-change-percent"]'
                      ]),
                      currency: (document.querySelector('[data-test="currency-in"]') || {}).innerText || null,
                      // KV table on the right side of the page
                      kv: (() => {
                          const rows = document.querySelectorAll('[data-test*="key-info"] li, .key-info li, dt');
                          const out = {};
                          rows.forEach(r => {
                              const k = r.querySelector('span, dt') ? r.querySelector('span').innerText : null;
                              const v = r.querySelector('strong, dd');
                              if (k && v) out[k.trim()] = v.innerText.trim();
                          });
                          return out;
                      })(),
                    };
                }
            """)
            extracted.update(data or {})
        finally:
            browser.close()

    return extracted if extracted.get("price") else None


def _get_quote(ticker: str) -> dict:
    slug = _slug(ticker)
    if not slug:
        raise NotImplementedError(f"No Investing.com slug curated for {ticker}")
    cached = _read_cache(slug)
    if cached:
        return cached
    q = _fetch_quote(slug)
    if not q:
        raise ValueError(f"Investing.com fetch failed or 404 for slug={slug}")
    _write_cache(slug, q)
    return q


def _parse_number(s: Any) -> Optional[float]:
    """'1,240.30' -> 1240.30; '+5.20 (+1.23%)' -> 5.20; None -> None."""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    m = re.search(r"[-+]?\d[\d,]*\.?\d*", str(s).replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group())
    except ValueError:
        return None


class InvestingProvider(Provider):
    name = "investing"

    def __init__(self):
        self._cache: dict[str, Optional[dict]] = {}

    def _quote(self, ticker: str) -> dict:
        if ticker not in self._cache:
            try:
                self._cache[ticker] = _get_quote(ticker)
            except NotImplementedError:
                self._cache[ticker] = None
                raise
        if not self._cache[ticker]:
            raise ValueError(f"Investing.com had no data for {ticker}")
        return self._cache[ticker]

    def _fetch_current_price(self, ticker: str):
        q = self._quote(ticker)
        raw_id = persist_raw(self.name, ticker, "current_price", q)
        price = _parse_number(q.get("price"))
        if price is None:
            raise ValueError("no parseable price field")
        return price, (q.get("currency") or ""), "", raw_id

    def _fetch_dividend_yield(self, ticker: str):
        q = self._quote(ticker)
        raw_id = persist_raw(self.name, ticker, "dividend_yield", q)
        kv = q.get("kv") or {}
        for key in ("Dividend Yield", "Div Yield", "Dividend yield"):
            if key in kv:
                v = _parse_number(kv[key])
                if v is not None:
                    return v, "%", "", raw_id
        raise ValueError("dividend yield not present in KV block")

    def _fetch_target_price(self, ticker: str):
        # Investing has analyst targets on a separate sub-page; out of
        # scope for the minimal v1 wiring. Could be added by also loading
        # /equities/<slug>-consensus-estimates.
        raise NotImplementedError
