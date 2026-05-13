# Consumer journey on the hosted site

What actually happens when someone opens the Render deployment, end-to-end.

## Two lanes, two latencies

The hosted site has two distinct paths for getting a deck. Picking which one fires depends on whether the ticker is on the curated 10-name **panel** or off-panel.

### Lane 1 — Panel ticker (fast lane, zero compute on request)

```
User → /jabal page
   ↓ lists 10 pre-rendered decks
User → clicks "Download .pptx" on a panel ticker
   ↓ GET /api/jabal/{ticker}/deck.pptx
Server → returns static file from static/decks/{ticker}.pptx
```

**Latency: <100ms.** The deck was rendered overnight by the cron job; the request path is a pure static-file lookup. No data fetching, no PPTX rendering, no Cloudflare-protected source touched.

What the consumer sees:
- `/jabal` HTML page lists each panel ticker with: company name, sector, confidence summary (H/M/L cell counts), and the sources behind the deck (yahoo, marketscreener, investing, ir_pdf, etc.).
- Clicking any row downloads a 3-slide PPTX in the Jabal design system.
- Deck content reflects the canonical_store as of the last cron run (default 04:00 UTC daily).

### Lane 2 — Off-panel ticker (slow lane, on-demand)

```
User → ticker search box
User → POST /api/reports {ticker: "NVDA"}
Server → 12-step legacy pipeline (or Jabal renderer when JABAL_RENDERER=1)
   ↓ ~30-90 seconds of fetches (yahoo, marketscreener, etc.)
   ↓ canonical_store refresh + deck render
User → polls / downloads when ready
```

**Latency: 30–90s** (subject to MarketScreener rate-limit, IR-PDF availability, Investing.com Playwright). Hits Render free-tier's 30s limit if all sources need to be touched; the legacy "permissive" readiness mode lets thin tickers ship anyway.

## What the cron job does (04:00 UTC daily)

`scripts/render_panel_decks.py`:

1. For each ticker in `PANEL` (10 names):
   1. **Refresh canonical_store** via `daily_refresh.py` for cadence ∈ {daily, weekly, quarterly}. Providers: yahoo, marketscreener, macro, ishares, commodities. Investing.com is opt-in (Playwright needs xvfb-run on a paid tier).
   2. **Render Slide 1 / 2 / 3** via the existing `render_jabal_*` modules — exactly the same code path that backs `JABAL_RENDERER=1` on the legacy pipeline.
   3. **Persist** `static/decks/{ticker}.pptx` + `{ticker}.meta.json` (sources, confidence summary, last-refreshed timestamp, file size).
2. Write `static/decks/index.json` — the panel directory the `/api/jabal/panel` endpoint serves.

Total wall-clock per ticker: ~15-25s. Full panel (10 tickers) ≈ 4 minutes.

## API surface

| Route | Method | Purpose | Latency |
|---|---|---|---|
| `/jabal` | GET | Public HTML panel listing | <50ms |
| `/api/jabal/panel` | GET | JSON of all pre-rendered decks (drives the SPA panel) | <10ms |
| `/api/jabal/{ticker}` | GET | Per-ticker metadata (sources, confidence, size) | <10ms |
| `/api/jabal/{ticker}/deck.pptx` | GET | Static file download | <100ms |
| `/api/tickers/search?q=` | GET | Ticker search (panel + DB) | ~50ms |
| `/api/reports` (POST) | POST | On-demand render for off-panel tickers | 30-90s |

## What the consumer sees (slide-level)

A typical panel deck contains:

| Slide | What's on it |
|---|---|
| **1 — Snapshot** | Company name (Georgia 26pt), ticker + sector + industry meta, 3 consensus cards (rating + target + upside), 6-cell key-data grid, 6-cell coloured performance row, 52-week range bar with gold fill + diamond marker, 5 category-pill highlights |
| **2 — Thesis** | Auto-templated Executive Summary card (data-driven from canonical_store; mentions commodity context for E&P/agri names, IMF macro context for the country), estimates table led by next-Q row (period in label, footnote shows analyst count + next print date), Catalysts (green dots) + Risks (red dots) cards, numbered "What to watch" |
| **3 — Valuation** | 52-week price chart, P/E 5-year range bars with current diamond, peer table (iShares regional benchmark row), 3-card sentiment row (real Buy/Hold/Sell distribution, average target + range, last 3 broker actions) |

## What's intentionally NOT in the request path

- **Network fetches.** Not Yahoo, not MarketScreener, not Investing.com.
- **PPTX rendering.** All slide composition happened during the cron run.
- **Database writes.** Only static-file reads.
- **LLM calls.** Gemini summarization isn't used by the Jabal renderer (the executive summary is data-templated).

This means the user-facing surface is **dependency-free** for the panel — even if every external data source is down at the moment of the request, the consumer still gets a usable deck (the deck just won't reflect the failures until the next cron cycle).

## Operational checklist for "is the site healthy?"

1. **Panel decks fresh?** `GET /api/jabal/panel` → check `rendered_at` is within 24h.
2. **All 10 tickers present?** Same call, `panel_size` should be 10.
3. **Confidence mix changed unexpectedly?** Per-ticker `by_confidence` flagged in the meta. A previously-High cell falling to Low means a source disagreement or a data-quality issue.
4. **Cron last status?** Render dashboard → `jabal-panel-refresh` cron service → last run logs.

## Limits + known gaps

- **OQEP balance sheet** is on a PDF page rendered as an image (no text layer). Filed-statement BS extraction requires OCR; the deck shows BS as "not available" honestly rather than estimating.
- **ADCB MS mapping** returns Aldar Properties data (separate MS slug needs to be re-curated in `company_master`). Until fixed, broker_actions for ADCB are suppressed at render time.
- **Investing.com on Render** is excluded from the cron because Chromium / Playwright don't run reliably on the free tier. The local development environment can include it (`--include-investing`); cells canonical-sourced from Investing get refreshed manually or via paid runtime.
- **Off-panel tickers** still go through the legacy 30-90s `/api/reports` path. To add a new ticker to the fast lane: append it to `PANEL` in `probe_harness.py`, run `render_panel_decks.py --tickers <NEW>` locally, commit `static/decks/<NEW>.*`.
