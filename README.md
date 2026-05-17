# Earnings Research

Institutional earnings-preview note generator. Enter a ticker, get a
3-page institutional PPTX with Snapshot · Thesis & Expectations ·
Valuation & Positioning slides — built off a free-source data stack
(Yahoo Finance, MarketScreener, Investing.com) with a Bloomberg-export
upload path for high-confidence runs.

**Live:** https://earnings-research-ur07.onrender.com/ (one site = API + UI)

---

## What it does

For every supported ticker the system produces a 3-slide deck:

1. **Snapshot** — company / sector / exchange header, analyst consensus
   (rating + target + upside), key data (last close, market cap, P/E,
   div yield), 6-bucket performance row (1D / 1W / 1M / 3M / 6M / YTD),
   52-week range bar, and 5 data-driven highlight pills.
2. **Thesis & Expectations** — Gemini-drafted thesis paragraph with
   forced numeric grounding, Q+1 estimates table (Jabal Est · Consensus
   · Δ · YoY), catalysts / risks / what-to-watch lists with quantitative
   anchors.
3. **Valuation & Positioning** — 52-week price chart, 5-year forward
   P/E history chart, peer comparables table (5 curated peers per
   ticker), analyst consensus distribution, average target price,
   last 3 broker actions.

Bank tickers swap the EBITDA row for NII and drop the EBITDA Margin
row (cost-to-income / NIM need balance-sheet data we don't fetch).

---

## Architecture

```
                ┌────────────── On-demand deck flow ─────────────────┐
                │                                                    │
   user POST    │   pipeline.run_preview(ticker)                     │
   /api/reports ├──▶ 1. validate_ticker (Yahoo)                      │
                │   2. resolve_mapping (company_master)              │
                │   3. fetch_quote                                   │
                │   4. fetch_financials  ──┐                         │
                │   5. fetch_consensus     │                         │
                │   5b. fetch_marketscreener_pages (MS scrape)       │
                │   5c. fetch_earnings_date (Yahoo fallback)         │
                │   6. fetch_news                                    │
                │   7. reconcile + derived metrics                   │
                │   8. summarize_news (skipped; deferred to step 11) │
                │   9. build_report_payload (canonical assembly)     │
                │   10. qa_validate                                  │
                │   10b. report_readiness gate                       │
                │   11. draft_pptx_sections (Gemini, last)           │
                │   11b. data_validation warnings                    │
                │   12. generate_report (PPTX) ──────────────────────┴──▶ .pptx
                │
                └───────────────────────────────────────────────────────┘

Data sources (free stack, trust ladder for canonical reconciliation):

   Investing.com   ──┐
   MarketScreener  ──┼─▶ canonical_store ─▶ slide renderer
   Yahoo Finance   ──┤                          │
   World Bank/IMF  ──┘                          │
                                                ▼
   Bloomberg export upload ─▶ payload override ─┘
   (per-ticker xlsx via /api/bloomberg/upload)
```

Investing.com sits in front of MarketScreener in the trust ladder
because its analyst data (target price, ratings, surprise history) is
more consistently structured. MS is the primary source for forward
quarterly forecasts; Yahoo backstops everything Yahoo covers; Bloomberg
upload overrides all of them when the analyst provides one.

---

## Quick start (local)

```bash
git clone https://github.com/YOUR_USERNAME/earnings-research.git
cd earnings-research
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env             # add GEMINI_API_KEY for the thesis paragraph
python -m src.main --init-db     # seed SQLite from data/company_master.json

# Generate a deck via the CLI
python -m src.main --ticker 2222.SR --mode preview --skip-llm

# Or run the web API
uvicorn src.api:app --reload --port 8000
# → http://localhost:8000/        (UI)
# → http://localhost:8000/docs    (API docs)
```

Tests: `pytest tests/ -v`

---

## Deploy (Render — one site = API + UI)

The repo is configured for a single Render Web Service that serves the
React UI at `/` and the FastAPI backend at `/api/*`. Push to `main`
triggers an auto-deploy:

```bash
./scripts/build_static.sh           # build frontend into static/
git add static/ && git commit -m "..."
git push origin main
```

`render.yaml` declares the service. Important env vars:
- `DATABASE_PATH=/tmp/earnings-data/earnings.db` (Render dir is read-only)
- `REPORT_OUTPUT_DIR=/tmp/earnings-outputs`
- `GEMINI_API_KEY` (set in Render dashboard)
- `JABAL_RENDERER=1` (force the 3-slide deck — only user-visible product)

A cron pre-renders the panel of curated tickers daily so first-visit
loads are instant. See `render.yaml`.

---

## Target ticker universe

| Region | Ticker | Company | Coverage |
|---|---|---|---|
| GCC | `2222.SR` | Saudi Aramco | Yahoo + MS + Investing |
| GCC | `2020.SR` | SABIC Agri-Nutrients | Yahoo + MS + Investing |
| GCC | `ADCB.AE` | Abu Dhabi Commercial Bank | MS + Investing (yfinance 404s ADX) |
| GCC | `ADNOCDRILL.AE` | ADNOC Drilling | MS + Investing |
| GCC | `BKMB.OM` | Bank Muscat | MS + Investing |
| GCC | `OQEP.OM` | OQ Exploration & Production | MS + Investing |
| India | `JINDALSTEL.NS` | Jindal Steel | Yahoo + MS + Investing |
| India | `ICICIBANK.NS` | ICICI Bank | Yahoo + MS + Investing |
| China/HK | `0700.HK` | Tencent Holdings | Yahoo + MS + Investing |
| China/HK | `2899.HK` | Zijin Mining | Yahoo + MS + Investing |
| China/HK | `1398.HK` | ICBC | Yahoo + MS + Investing |

`data/company_master.json` carries `marketscreener_id` and `peer_group`
(5 curated peers) for each of these. `probe_investing._SLUGS` carries
the Investing.com slug. Adding a new ticker requires updating both.

---

## The Investing.com cache (Cloudflare workaround)

**The problem:** Cloudflare blocks Render's egress IPs from reaching
Investing.com. `curl_cffi`'s TLS-fingerprint impersonation passes the
JS challenge, but the IP-reputation layer rejects cloud-IP traffic.

**The fix:** every Investing.com page used by the deck is pre-fetched
locally (or from GitHub Actions runners, which aren't IP-flagged) and
the JSON snapshot is committed to `data/investing/<slug>__<kind>.json`.
`probe_investing.py` has a three-layer fetcher:

1. `cache/probe/investing/` — 24h disk cache (writable on local; no-op
   on Render's read-only project dir)
2. Live network via `curl_cffi` — works from local, blocked on Render
3. `data/investing/` — repo-tracked snapshot, always available

Refresh manually with:

```bash
python -m scripts.refresh_investing_cache
git add data/investing/ && git commit -m "chore(cache): refresh"
git push
```

Or set up the GHA daily refresh — see `docs/github-action-setup.md`.

---

## Bloomberg upload (Stage 2)

For high-confidence runs the analyst exports BEST / EE consensus to
Excel and uploads it via the UI or:

```bash
curl -F file=@2222.SR_cons_q.xlsx \
     https://earnings-research-ur07.onrender.com/api/bloomberg/upload
```

The two file shapes are:
- `<TICKER>_cons_q.xlsx` — quarterly consensus (BEST / EE)
- `<TICKER>_FA.xlsx` — financial-analysis sheet (annual history)

When present, Bloomberg values take precedence over MS / Investing /
Yahoo in the deck. See `src/services/bloomberg_parser.py`.

---

## Adding a new ticker

1. Add a row to `data/company_master.json` with `ticker`, `company_name`,
   `exchange`, `country`, `currency`, `is_bank`, and `peer_group`
   (5 yfinance-resolvable peer tickers).
2. Look up the MS slug at `marketscreener.com` and add it to
   `marketscreener_id`.
3. Look up the Investing.com slug at `investing.com` and add it to
   `_SLUGS` in `src/providers/probe_investing.py`.
4. Run `python -m scripts.refresh_investing_cache --tickers NEW.XX`
   and commit the resulting `data/investing/<slug>__*.json` files.
5. Run `python -m src.main --init-db` to merge `company_master.json`
   into the local SQLite.
6. Test: `python -m src.main --ticker NEW.XX --mode preview`.

---

## Project layout

```
earnings-research/
├── config/settings.toml          # thresholds, renderer flag, calendar tunables
├── data/
│   ├── company_master.json       # curated ticker universe + MS slugs + peers
│   ├── investing/                # pre-warmed Investing.com snapshots
│   └── bloomberg/                # uploaded Bloomberg xlsx (per ticker)
├── src/
│   ├── api.py                    # FastAPI app
│   ├── pipeline.py               # 12-step orchestrator
│   ├── main.py                   # CLI
│   ├── providers/
│   │   ├── probe_yahoo.py        # yfinance probe → canonical_store
│   │   ├── probe_marketscreener.py
│   │   ├── probe_investing.py    # curl_cffi + cache + tracked snapshots
│   │   ├── probe_macro.py        # World Bank / IMF
│   │   ├── probe_commodities.py  # OPEC / EIA / World Bank
│   │   ├── probe_ishares.py      # ETF proxy returns (opt-in)
│   │   └── probe_ir_pdf.py       # Company IR PDFs (opt-in)
│   ├── services/
│   │   ├── fetch_marketscreener_pages.py
│   │   ├── bloomberg_parser.py
│   │   ├── canonical_store.py    # reconciled-value read API
│   │   ├── reconcile_sources.py  # trust-ladder logic
│   │   ├── llm_summary.py        # Gemini thesis prompt (forced grounding)
│   │   ├── render_jabal_snapshot.py    # slide 1
│   │   ├── render_jabal_thesis.py      # slide 2
│   │   ├── render_jabal_valuation.py   # slide 3
│   │   └── generate_report.py    # writes the .pptx
│   ├── models/                   # Pydantic schemas (FinancialPeriod, ReportPayload, …)
│   └── storage/db.py             # SQLite schema + seed
├── frontend/                     # Vite + React app
├── static/                       # built frontend (committed for one-site Render deploy)
├── scripts/
│   ├── refresh_investing_cache.py  # snapshot Investing.com → data/investing/
│   ├── daily_refresh.py            # canonical_store cadence-based refresh
│   ├── probe_sources.py            # provider registry
│   └── render_build.sh             # Render build hook
├── tests/                        # pytest
├── render.yaml                   # Render service + cron blueprint
└── requirements.txt
```

---

## Governance notes

- **LLM never touches numbers.** Gemini drafts the thesis paragraph and
  bullets only. Every numeric claim must reference a value from the data
  block; the prompt has a forbidden-phrase filter and an explicit
  grounding contract. See `src/services/llm_summary.py`.
- **No silent fallbacks in the value path.** `canonical_store` records
  per-cell provenance (`canonical_source`, `sources_with_value`); the
  slide footer credits every contributing provider.
- **Bloomberg override is the analyst's veto.** Anything the analyst
  uploads via `/api/bloomberg/upload` takes precedence in the deck —
  the free stack is the baseline, not the truth.
- **Read-only filesystem on Render.** Writable paths are `/tmp/...`
  only. The Investing cache writes are wrapped in try/except so a
  read-only failure on Render is a no-op (the tracked snapshot fallback
  handles it).
