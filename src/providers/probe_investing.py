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
    # India (originally curated Stage 1)
    "ICICIBANK.BO":   "icici-bank-ltd",
    "JINDALSTEL.NS":  "jindal-steel---power",
    # Stage 2: panel-wide curation. Confirmed via investing.com search
    # (slugs cross-checked against each /equities/<slug> landing page).
    "2222.SR":        "saudi-aramco",
    "0700.HK":        "tencent-holdings-hk",
    "1398.HK":        "icbc",
    "2899.HK":        "zijin-mining-group",
    "ADCB.AE":        "ad-commercial",
    "ADNOCDRILL.AE":  "adnoc-drilling",
    "BKMB.OM":        "bank-muscat",
    "OQEP.OM":        "oq-exploration-and-production-cjsc",
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


_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def _new_browser_context():
    """Launch Playwright Chromium (non-headless first; fall back to headless
    if the host has no display). Returns (sync_playwright_ctx, browser, ctx)
    or raises so the caller can short-circuit."""
    from playwright.sync_api import sync_playwright
    spc = sync_playwright().start()
    browser = None
    last_err = None
    for headless in (False, True):
        try:
            browser = spc.chromium.launch(
                headless=headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            break
        except Exception as exc:
            last_err = exc
            continue
    if browser is None:
        spc.stop()
        raise RuntimeError(f"Could not launch Chromium: {last_err}")
    ctx = browser.new_context(
        user_agent=_UA, viewport={"width": 1366, "height": 900}, locale="en-US",
    )
    ctx.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return spc, browser, ctx


def _load_through_cf(page, url: str, max_wait_s: int = 20) -> bool:
    """Navigate to `url` and wait through any Cloudflare challenge.
    Returns True if the real page loaded, False if we hit 404 / CF wall."""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception:
        return False
    for _ in range(max_wait_s):
        if "Just a moment" not in page.title():
            break
        time.sleep(1)
    if "Just a moment" in page.title() or "Error 404" in page.title():
        return False
    return True


def _fetch_quote(slug: str) -> Optional[dict]:
    """Use Playwright to load the equity page and extract a small set of
    quote fields from the DOM. Returns dict or None."""
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
    except ImportError:
        return None

    url = f"https://www.investing.com/equities/{slug}"
    extracted: dict[str, Any] = {"url": url, "fetched_at": datetime.now(timezone.utc).isoformat()}

    spc = browser = ctx = None
    try:
        spc, browser, ctx = _new_browser_context()
    except Exception:
        return None
    try:
        page = ctx.new_page()
        if not _load_through_cf(page, url):
            return None

        extracted["title"] = page.title()
        data = page.evaluate(
            """
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
            """
        )
        extracted.update(data or {})
    finally:
        try:
            browser.close()
        finally:
            spc.stop()

    return extracted if extracted.get("price") else None


# ── Consensus + earnings sub-page fetchers ─────────────────────

def _consensus_cache_path(slug: str) -> Path:
    return cache_root() / "investing" / f"{slug}__consensus.json"


def _earnings_cache_path(slug: str) -> Path:
    return cache_root() / "investing" / f"{slug}__earnings.json"


def _read_subpage_cache(path: Path, ttl_hours: float = 24) -> Optional[dict]:
    if not path.exists():
        return None
    age = datetime.now(timezone.utc) - datetime.fromtimestamp(
        path.stat().st_mtime, tz=timezone.utc)
    if age > timedelta(hours=ttl_hours):
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _write_subpage_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str))


def _fetch_sub_pages(slug: str) -> dict:
    """Single Playwright session that visits BOTH the consensus-estimates
    and earnings sub-pages, extracting:
      - consensus: rating distribution (buy/hold/sell counts) + 12m target
        + analyst count
      - earnings:  forward EPS + revenue for next quarter & next 2 FYs,
                    + EPS surprise history (last 4–8 quarters)

    Cached separately per sub-page so a partial failure still preserves
    the other page's data. Returns dict with `consensus` and `earnings`
    keys (each may be None on failure)."""
    out: dict[str, Any] = {"slug": slug,
                           "fetched_at": datetime.now(timezone.utc).isoformat()}
    cons_path = _consensus_cache_path(slug)
    earn_path = _earnings_cache_path(slug)
    cached_cons = _read_subpage_cache(cons_path)
    cached_earn = _read_subpage_cache(earn_path)
    if cached_cons and cached_earn:
        out["consensus"] = cached_cons
        out["earnings"]  = cached_earn
        return out

    spc = browser = ctx = None
    try:
        spc, browser, ctx = _new_browser_context()
    except Exception:
        out["consensus"] = cached_cons
        out["earnings"]  = cached_earn
        return out
    try:
        page = ctx.new_page()

        # ── 1. Consensus-estimates page ──
        if not cached_cons:
            url = f"https://www.investing.com/equities/{slug}-consensus-estimates"
            if _load_through_cf(page, url):
                cons = page.evaluate(
                    """
                    () => {
                        // Grab the rating distribution counts. The page renders
                        // "X Buy   Y Hold   Z Sell" alongside an aggregate
                        // total. We pull every visible numeric near those keywords.
                        const body = document.body.innerText || "";
                        const grab = (re) => {
                            const m = body.match(re);
                            return m ? parseFloat(m[1].replace(/,/g, '')) : null;
                        };
                        return {
                            n_buy:        grab(/(\\d+)\\s+Buy\\b/i),
                            n_hold:       grab(/(\\d+)\\s+Hold\\b/i),
                            n_sell:       grab(/(\\d+)\\s+Sell\\b/i),
                            target_mean:  grab(/Average[^0-9]{0,40}([0-9][0-9,]*\\.?\\d*)/i),
                            target_high:  grab(/High[^0-9]{0,40}([0-9][0-9,]*\\.?\\d*)/i),
                            target_low:   grab(/Low[^0-9]{0,40}([0-9][0-9,]*\\.?\\d*)/i),
                            consensus_label: (body.match(/Consensus\\s+Rating[^A-Za-z]*([A-Za-z\\s]+?)(?:\\n|$)/i) || [])[1] || null,
                        };
                    }
                    """
                )
                if cons and (cons.get("n_buy") is not None or cons.get("target_mean") is not None):
                    _write_subpage_cache(cons_path, cons)
                    cached_cons = cons

        # ── 2. Earnings page ──
        if not cached_earn:
            url = f"https://www.investing.com/equities/{slug}-earnings"
            if _load_through_cf(page, url):
                earn = page.evaluate(
                    """
                    () => {
                        const body = document.body.innerText || "";
                        const scaled = s => {
                            if (!s) return null;
                            const m = String(s).match(/([0-9][0-9.,]*)\\s*([BMT]?)/i);
                            if (!m) return null;
                            const v = parseFloat(m[1].replace(/,/g,''));
                            const u = (m[2]||'').toUpperCase();
                            return u==='T'?v*1e12 : u==='B'?v*1e9 : u==='M'?v*1e6 : v;
                        };

                        // FY guidance summary — companies phrase it many ways.
                        // Try the most common variants in order; each must
                        // anchor on "FY<year>" + an EPS-like float + a
                        // revenue-like dollar amount.
                        //
                        // Variant A (Aramco): "FY2026 EPS guidance set at $0.52
                        //   with revenue forecast of $493.34B; FY2027 targets
                        //   $0.58 EPS and $544.09B revenue"
                        // Variant B (Tencent etc.): "FY2026 EPS of HK$22.10 ...
                        //   revenue of HK$700.0B"
                        // Variant C (banks): "FY2026 EPS guidance $1.18 ...
                        //   revenue $14.2B"
                        const fyForecasts = [];
                        const fyVariants = [
                            // FY<yr> ... <eps> ... <revenue>
                            /FY\\s*(\\d{4})[^.;]{0,80}?\\$?([0-9]+\\.[0-9]+)[^.;]{0,80}?\\$?([0-9][0-9.,]*\\s*[BMT])/gi,
                            // FY<yr> targets <eps> EPS and <revenue>
                            /FY\\s*(\\d{4})\\s+targets[^.;]{0,40}?\\$?([0-9]+\\.[0-9]+)\\s*EPS[^.;]{0,40}?\\$?([0-9][0-9.,]*\\s*[BMT])/gi,
                        ];
                        for (const re of fyVariants) {
                            re.lastIndex = 0;
                            const seenYears = new Set();
                            let m;
                            while ((m = re.exec(body)) !== null && fyForecasts.length < 4) {
                                const yr = parseInt(m[1]);
                                if (seenYears.has(yr)) continue;
                                seenYears.add(yr);
                                fyForecasts.push({
                                    year: yr,
                                    eps:  parseFloat(m[2]),
                                    revenue: scaled(m[3]),
                                });
                            }
                            if (fyForecasts.length >= 2) break;
                        }
                        fyForecasts.sort((a, b) => a.year - b.year);

                        // Next quarter — pulled DIRECTLY from the upcoming
                        // row in the surprise/forecast table, which is the
                        // most reliable place. The row has "--" for actuals.
                        //
                        // Layout: <date>\\t<period>\\t--\\t/<eps_est>\\t--\\t/<rev_est>
                        const upcomingRE = /([A-Z][a-z]{2}\\s+\\d{1,2},\\s+\\d{4})\\t([\\d\\/A-Za-z]+)\\t--\\t\\/([0-9.]+)\\t--\\t\\/([0-9.,]+[BMT]?)/;
                        const upcomingMatch = body.match(upcomingRE);
                        const upcoming = upcomingMatch ? {
                            report_date: upcomingMatch[1],
                            period: upcomingMatch[2],
                            eps_estimate: parseFloat(upcomingMatch[3]),
                            revenue_estimate: scaled(upcomingMatch[4]),
                        } : null;

                        // Surprise history table — rows are tab-separated:
                        //   <date>\\t<period>\\t<eps_actual>\\t/<eps_forecast>\\t<rev_actual>\\t/<rev_forecast>\\t<eps_surprise%>\\t<rev_surprise%>
                        // Period column is "MM/YYYY" or sometimes "Q# YYYY".
                        const surprise_rows = [];
                        const rowRE = /([A-Z][a-z]{2}\\s+\\d{1,2},\\s+\\d{4})\\t([\\d/A-Za-z]+)\\t([\\d.,-]+)\\t\\/([\\d.,-]+)\\t([\\d.,-]+B?M?T?)\\t\\/([\\d.,-]+B?M?T?)\\t([+-]?[\\d.]+%|0%)\\t([+-]?[\\d.]+%|0%)/g;
                        while ((m = rowRE.exec(body)) !== null && surprise_rows.length < 8) {
                            const eps_actual = parseFloat(m[3]);
                            const eps_est    = parseFloat(m[4]);
                            const eps_sur    = parseFloat(m[7].replace('%',''));
                            if (isNaN(eps_actual)) continue;
                            surprise_rows.push({
                                report_date: m[1],
                                period:      m[2],
                                eps_actual:  eps_actual,
                                eps_estimate: eps_est,
                                revenue_actual:   scaled(m[5]),
                                revenue_estimate: scaled(m[6]),
                                eps_surprise_pct: eps_sur,
                                rev_surprise_pct: parseFloat(m[8].replace('%','')),
                            });
                        }

                        const fy1 = fyForecasts[0] || {};
                        const fy2 = fyForecasts[1] || {};
                        return {
                            next_q_eps:         upcoming ? upcoming.eps_estimate     : null,
                            next_q_revenue:     upcoming ? upcoming.revenue_estimate : null,
                            next_q_period:      upcoming ? upcoming.period           : null,
                            next_q_report_date: upcoming ? upcoming.report_date      : null,
                            fy1_year:    fy1.year || null,
                            fy1_eps:     fy1.eps  || null,
                            fy1_revenue: fy1.revenue || null,
                            fy2_year:    fy2.year || null,
                            fy2_eps:     fy2.eps  || null,
                            fy2_revenue: fy2.revenue || null,
                            surprise_history: surprise_rows,
                        };
                    }
                    """
                )
                if earn and (earn.get("next_q_eps") is not None or earn.get("surprise_history")):
                    _write_subpage_cache(earn_path, earn)
                    cached_earn = earn
    finally:
        try:
            browser.close()
        finally:
            spc.stop()

    out["consensus"] = cached_cons
    out["earnings"]  = cached_earn
    return out


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

    # ── Sub-page-backed fields (consensus + earnings) ────────────

    def _sub_pages(self, ticker: str) -> dict:
        """Return the cached consensus+earnings bundle for this ticker.
        Cached on the instance to avoid double-loading within one probe
        run (Playwright is the expensive call)."""
        attr = f"_sub_{ticker}"
        if not hasattr(self, attr):
            slug = _slug(ticker)
            if not slug:
                raise NotImplementedError(f"No slug for {ticker}")
            setattr(self, attr, _fetch_sub_pages(slug))
        return getattr(self, attr)

    def _fetch_target_price(self, ticker: str):
        bundle = self._sub_pages(ticker)
        cons = bundle.get("consensus") or {}
        mean = cons.get("target_mean")
        if mean is None:
            raise ValueError("Investing.com consensus page had no target_mean")
        raw_id = persist_raw(self.name, ticker, "target_price", bundle)
        # Add n_analysts derived from buy/hold/sell counts so the renderer
        # can show "X analysts covering" without falling back to MS.
        n = sum(int(cons.get(k) or 0) for k in ("n_buy", "n_hold", "n_sell"))
        return {
            "mean": float(mean),
            "high": cons.get("target_high"),
            "low":  cons.get("target_low"),
            "n_analysts": n or None,
        }, "", "", raw_id

    def _fetch_rating_split(self, ticker: str):
        bundle = self._sub_pages(ticker)
        cons = bundle.get("consensus") or {}
        if not any(cons.get(k) is not None for k in ("n_buy", "n_hold", "n_sell")):
            raise ValueError("Investing.com consensus page had no buy/hold/sell counts")
        raw_id = persist_raw(self.name, ticker, "rating_split", bundle)
        buy  = int(cons.get("n_buy")  or 0)
        hold = int(cons.get("n_hold") or 0)
        sell = int(cons.get("n_sell") or 0)
        consensus_label = (cons.get("consensus_label") or "").strip() or None
        # Derive label from majority if not explicitly published.
        if not consensus_label:
            total = max(1, buy + hold + sell)
            if buy / total >= 0.6: consensus_label = "BUY"
            elif sell / total >= 0.4: consensus_label = "SELL"
            elif buy > sell: consensus_label = "OUTPERFORM"
            else: consensus_label = "HOLD"
        return {
            "buy": buy, "hold": hold, "sell": sell,
            "total": buy + hold + sell, "consensus": consensus_label,
        }, "", "", raw_id

    def _fetch_valuation_forward(self, ticker: str):
        bundle = self._sub_pages(ticker)
        earn = bundle.get("earnings") or {}
        if not any(earn.get(k) is not None for k in
                    ("next_q_eps", "fy1_eps", "next_q_revenue", "fy1_revenue")):
            raise ValueError("Investing.com earnings page had no forward estimates")
        raw_id = persist_raw(self.name, ticker, "valuation_forward", bundle)
        return {
            # FY+1
            "fy1_year":   earn.get("fy1_year"),
            "eps_fy1":    earn.get("fy1_eps"),
            "revenue_fy1": earn.get("fy1_revenue"),
            # FY+2
            "fy2_year":   earn.get("fy2_year"),
            "eps_fy2":    earn.get("fy2_eps"),
            "revenue_fy2": earn.get("fy2_revenue"),
            # Next reported quarter
            "next_q_period":      earn.get("next_q_period"),
            "next_q_report_date": earn.get("next_q_report_date"),
            "eps_next_q":         earn.get("next_q_eps"),
            "revenue_next_q":     earn.get("next_q_revenue"),
        }, "", "", raw_id

    def _fetch_income_statement_quarterly(self, ticker: str):
        """Surprise history is a quarterly fact, not a forecast, so we
        map it to income_statement_quarterly. The renderer can use the
        last-4 rows for slide-2 "track record" context."""
        bundle = self._sub_pages(ticker)
        earn = bundle.get("earnings") or {}
        history = earn.get("surprise_history") or []
        if not history:
            raise ValueError("Investing.com earnings page had no surprise history")
        raw_id = persist_raw(self.name, ticker, "income_statement_quarterly", bundle)
        return {
            "surprise_history": history,
        }, "", "", raw_id
