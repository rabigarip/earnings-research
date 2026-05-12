# Week 2 — Exchange Endpoint Discovery Notes

Source-by-source field notes captured during Week 2 probing. Each entry
documents what we tried, what works, what's blocked, and how to wire
the provider when implementation begins.

## ADX (Abu Dhabi Securities Exchange) — endpoints captured, rate-limited

**Status:** Real public REST API discovered. Auth headers captured.
First call succeeds; rapid follow-ups return empty bodies (rate-limit
or IP-ban). Provider needs careful pacing + retry-with-backoff.

**Base URL:** `https://apigateway.adx.ae`

**Auth headers** (extracted from the live www.adx.ae site — these are
public values embedded in their site JS):

```
channel-id: OSS WEB
x-correlation-id: uuid
x-uuid:
adx-gateway-apikey: 1863a94c-582b-46f9-b4f0-0d02c0cc5307
Accept: application/json
Referer: https://www.adx.ae/
```

**Endpoints confirmed working** (one request at a time, 5s+ between):

| Endpoint | Returns |
|---|---|
| `/adx/lookups/1.1/data/listed-companies` | `response.companies[]` — all listed issuers with symbol + nameEn |
| `/adx/marketwatch/1.1/securityBoards/mainMarket` | `response.results[]` — main board securities with prices |
| `/adx/marketwatch/1.1/securityBoard/marketwatch` | full marketwatch snapshot |
| `/adx/marketwatch/1.1/trading-status` | open/closed flag |
| `/adx/tradings/1.1/news?categoryName=cd` | disclosure news |

**Anti-block strategy needed:**
1. Single-flight requests with min 5s between any two
2. Retry on empty body with exponential backoff
3. Cache aggressively — 24h on listed-companies, 1h on price board
4. If banned, fall back to Playwright (still works because it loads
   index.html which sets cookies before XHR)

**Field mapping** (TBD — pending an un-rate-limited capture). The
shape is `{symbol, nameEn, sector, ...}` for listed-companies, and
similar for market data with `lastPrice`, `volume`, etc.

## Saudi Exchange (formerly Tadawul) — geo-blocked

**Status:** Access Denied from non-Saudi IPs. Playwright headless gets
the same 403/blocking message. Direct API attempts return 404.

**Workarounds:**
- Argaam.com mirrors Tadawul data and is not geo-blocked — but their
  structure is HTML-only and Arabic-default. Practical option.
- Public CSV/Excel mirrors at saudiexchange.sa/Resources/... — found
  during the probe but returned 404. Need to verify the right path.
- For Aramco specifically: Yahoo Finance has full coverage (verified
  in Week 1), so Tadawul isn't strictly required for the panel.

**Recommended path:** Skip Saudi Exchange as a primary source for
Stage 1. Use Yahoo (already 10/10 on Aramco). Document this as a
Stage 2 paid-data gap if KSA-specific data is needed.

## MSX (Muscat Securities Exchange) — wrong URL paths

**Status:** Initial URLs returned "resource not available". Site is
small enough that the correct paths can be found by manual browse
once we have a working browser session.

**Next steps:** Re-probe with `https://www.msx.om/en/...` paths once
ADX provider is shipped. The Bank Muscat (BKMB) issuer page should
exist at a predictable URL.

## NSE / BSE (India) — not yet probed

**Status:** Pending. Documented previously: NSE requires a
cookies-session warm-up (visit nseindia.com first, then the API
endpoints); BSE has a JSON API at `/api/companyinfo`. Should be the
easiest exchanges of the lot once we get there.

## HKEX — not yet probed

**Status:** Pending. URL pattern for filings is `hkexnews.hk/listedco/...`
and is predictable. PDFs only — needs pdfplumber on top.

---

## Week 2 revised sequence (after Day 1 findings)

| Day | What | Why |
|---|---|---|
| **Day 1 (done)** | ADX endpoint + auth discovery | Found public API + key; rate-limited |
| **Day 2** | ADX provider w/ rate-limiting + retry | Need it to fill ADCB + ADNOCDRILL |
| **Day 3** | NSE + BSE providers | Easier than MENA exchanges, knock out India |
| **Day 4** | HKEX filings + MSX retry | HK richest filings; MSX retry with correct URL |
| **Day 5** | IR-PDF fallback + v2 probe run | Catches any panel gaps still open |

Saudi Exchange dropped from Stage 1 — geo-restricted; Yahoo already
covers Aramco at 10/10 so panel-level coverage isn't affected.
