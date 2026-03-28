# Earnings Research — Backend API & Pipeline

Backend for earnings preview: governed pipeline (Yahoo, MarketScreener, Zawya, Gemini) plus a **FastAPI web API** and **Render** deployment (one-site: API + static frontend).

---

## Quick Start (local)

```bash
# 1. Clone and set up
git clone https://github.com/YOUR_USERNAME/earnings-research.git
cd earnings-research
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. Optional: Gemini for news summarization
cp .env.example .env
# Add GEMINI_API_KEY to .env if you want LLM summarization

# 3. Initialize DB (seeds from data/company_master.json)
python -m src.main --init-db

# 4. CLI: run a preview (skip-llm = no Gemini, faster)
python -m src.main --ticker 2010.SR --mode preview --skip-llm

# 5. Web API (for frontend / Render)
uvicorn src.api:app --reload --port 8000
# → http://localhost:8000/docs  and  http://localhost:8000/api/reports
```

**Tests:** `pytest tests/ -v`

**Local quality testing:** See **`docs/LOCAL-TESTING.md`** for cleaning cache/outputs, running sector tickers, and checking Investment View fallback quality. Quick clean: `./scripts/clean_local.sh` then `python -m src.main --init-db`.

---

## Deploy to Render (one site = API + UI at one URL)

1. **Build the frontend** into `static/` (so one Web Service can serve both):
   ```bash
   ./scripts/build_static.sh
   ```
2. **Commit and push** (including the `static/` folder):
   ```bash
   git add static/
   git commit -m "Add static frontend for one-site deploy"
   git push origin main
   ```
3. In [Render](https://render.com): **New → Web Service**, connect **earnings-research**.
4. **Build:** `pip install -r requirements.txt` · **Start:** `uvicorn src.api:app --host 0.0.0.0 --port $PORT`
5. Open your Render URL — you get the **full app** (login, reports, New Report) at that one address. API docs: `/docs`.

Details: **`docs/ONE-SITE-RENDER.md`**. For API-only or separate frontend deploy: **`docs/DEPLOY-RENDER.md`**.

---

## Test Universe

| Ticker | Company | Type | Tests |
|--------|---------|------|-------|
| `2010.SR` | SABIC | Industrial | EBITDA ✓, standard metrics |
| `1120.SR` | Al Rajhi Bank | Bank | EBITDA skipped, bank path |

---

## Pipeline Steps

Every step prints a status box to the terminal and returns a `StepResult`.

| # | Step | Critical? | Source |
|---|------|-----------|--------|
| 1 | `validate_ticker` | YES — stops pipeline | Yahoo |
| 2 | `resolve_mapping` | YES — stops pipeline | Local seed |
| 3 | `fetch_quote` | No | Yahoo |
| 4 | `fetch_financials` | No | Yahoo |
| 5 | `fetch_consensus` | No | MarketScreener → Yahoo fallback |
| 6 | `fetch_news` | No | Yahoo + Reuters + Zawya |
| 7 | `reconcile` | No | Computed |
| 8 | `summarize_news` | No | Gemini (LLM) |
| 9 | `build_report_payload` | No | Assembled |
| 10 | `generate_report` | No | python-docx |

---

## Data sources and URLs

**MarketScreener** (primary) and **Yahoo Finance** (fallback) with field-by-field mapping and scrape order are documented in:

**`docs/DATA_SOURCE_AND_URL_REFERENCE.md`**

Use it for: which page provides which memo field, slug discovery, quarterly vs annual URLs, and fallback behavior.

---

## Project Structure

```
earnings-research/
├── config/
│   └── settings.toml              # Thresholds, timeouts, model version
├── data/
│   ├── company_master.json        # Curated company seed (used by init-db)
│   └── kpi_memory/                # Manual KPI JSON (optional)
├── src/
│   ├── main.py                    # CLI: --ticker, --mode preview, --init-db
│   ├── api.py                     # FastAPI app: /api/reports, /api/preview, etc.
│   ├── config.py                  # TOML loader + path resolver
│   ├── pipeline.py                # Orchestrator (steps 1–11)
│   ├── constants/
│   │   └── iv_quality.py          # Investment View: banned phrases, word bounds, guardrail
│   ├── models/
│   │   ├── step_result.py         # StepResult + StepTimer
│   │   ├── company.py             # CompanyMaster
│   │   ├── financials.py          # QuoteSnapshot, FinancialPeriod, DerivedMetrics
│   │   ├── news.py                # NewsItem, NewsSummary
│   │   └── report_payload.py      # ReportPayload
│   ├── providers/
│   │   ├── yahoo.py               # All Yahoo/yfinance calls
│   │   ├── marketscreener.py      # Consensus scraping (stub)
│   │   ├── gemini.py              # LLM wrapper (summarization only)
│   │   └── news/
│   │       ├── base.py            # Abstract NewsProvider
│   │       ├── yahoo_news.py      # Yahoo news adapter
│   │       ├── reuters_news.py    # Reuters adapter (stub)
│   │       ├── registry.py        # Provider wiring
│   │       └── local/
│   │           └── zawya.py       # Zawya Saudi adapter (stub)
│   ├── services/
│   │   ├── validate_ticker.py     # Step 1
│   │   ├── resolve_mapping.py     # Step 2
│   │   ├── fetch_quote.py         # Step 3
│   │   ├── fetch_financials.py    # Step 4
│   │   ├── fetch_consensus.py     # Step 5
│   │   ├── fetch_news.py          # Step 6
│   │   ├── reconcile.py           # Step 7
│   │   ├── summarize_news.py      # Step 8
│   │   ├── build_report_payload.py # Step 9
│   │   └── generate_report.py     # Step 10
│   └── storage/
│       ├── db.py                  # SQLite schema + queries
│       └── kpi_memory.py          # Manual KPI CRUD
├── scripts/
│   ├── seed_company_master.py      # Merge CSV into company_master.json
│   ├── diagnostics.py            # newsapi | sabic diagnostics
│   └── diagnostics_sabic.py      # SABIC vs working (used by diagnostics sabic)
├── tests/                         # pytest tests/ -v
├── docs/                          # DATA_SOURCE_AND_URL_REFERENCE, DEPLOY-RENDER, INVESTMENT_VIEW_FLOW
├── outputs/                       # Generated .docx (gitignored)
├── cache/                         # HTML cache (gitignored)
├── requirements.txt
├── render.yaml                    # Render Web Service blueprint
├── .env.example
└── .gitignore
```

---

## Adding a New Company

1. Edit `data/company_master.json` — add an entry
2. Run `python -m src.main --init-db`
3. Run `python -m src.main --ticker NEW.XX --mode preview`

## Adding a New Country's News Source

1. Create `src/providers/news/local/mubasher.py` (or whatever)
2. Subclass `NewsProvider`, set `country_code = "AE"` (or whatever)
3. Register the instance in `src/providers/news/registry.py`
4. Done — any company with `country: "AE"` will auto-use it

## Adding Calendar Mode

See the TODO in `src/main.py`. Implementation is ~30 lines using
`yf.Ticker(t).calendar` for each seeded company.

---

## Governance Notes

- **Model pinned** in `config/settings.toml` (`gemini.model`)
- **Every step** returns a `StepResult` with status, source, fallback flag, timing
- **Full audit trail** stored in SQLite `pipeline_runs.step_results`
- **No silent fallbacks** — if a source fails, the log says exactly which one
- **LLM never touches numbers** — Gemini is for text summarization only
- **Kill switch**: set `--skip-llm` or remove GEMINI_API_KEY to disable LLM
- **Company mappings are curated**, not auto-discovered
