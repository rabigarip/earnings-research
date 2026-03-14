# News sources & context providers

## Design

- **One provider module per source** with provider-specific scraping/parsing (URL validity, section exclusion, date parsing, extracted_fact, relevance).
- **Common interface** (`RecentContextProvider`): candidate discovery, listing-page parsing, article-page parsing (enrich), date extraction, extracted_fact generation, relevance tagging. Shared `NormalizedArticle` and pipeline: **retrieve → enrich → validate → dedupe → rank → render**.
- **Registry/config** per provider: `provider_name`, `domains`, `countries`, `source_priority`, `allowed_for_company_facts`, `allowed_for_sector_context`, `date_confidence_policy`.

## Per-country setup

The recent-context pipeline uses **context providers** (Reuters, Zawya, SCMP, etc.) to fetch news. Each provider can be scoped to **countries** in `config/settings.toml` via `countries = ["SA", "CN", ...]`. If `countries` is missing or empty, the provider is **global**. When running context for a company, only global providers or those listing that company’s country are used.

| Region | Provider(s) |
|--------|-------------|
| Global | Reuters |
| MENA (SA, AE, BH, KW, OM, QA, EG) | Zawya |
| China (CN) | South China Morning Post (scmp), NewsAPI (fallback) |
| India (IN) | Business Standard, Economic Times |
| South Africa (ZA) | Business Day (business_day), Daily Investor, Moneyweb |

---

## SCMP (China) – preferred: NewsAPI

**Recommended:** No browser; works in web apps and serverless.

1. Get a free API key at [newsapi.org](https://newsapi.org) (100 requests/day).
2. Set in `.env`: `NEWSAPI_KEY=your-key` or in `config/settings.toml` under `[news]` → `newsapi_key = "your-key"`.
3. SCMP will use NewsAPI (`domains=scmp.com`) first.

**Fallbacks when no API key:** (1) HTTP GET to SCMP search, (2) Playwright if response is small, (3) Google `site:scmp.com`. For browser: `pip install "playwright>=1.42.0"` then `playwright install chromium`.

---

## Adding a new country-specific source

1. **Implement** a provider in `src/providers/context/` (subclass `RecentContextProvider` or `SearchBasedContextProvider` in `base.py`): `provider_id`, `search_company_articles`, `search_sector_articles`, optional `enrich_metadata`.
2. **Register** in `src/providers/context/registry.py`: add to `_PROVIDER_CLASSES`.
3. **Config** in `config/settings.toml`: new `[[news.context_providers]]` with `provider_name`, `enabled`, `countries`, `domain_patterns`, `source_priority`, etc., and add the id to `recent_context_sources`.

---

# Web application deployment (no browser)

For **web/API/serverless**, running Playwright on each request is often impractical. Alternatives:

| Option | Notes |
|--------|--------|
| **News/search APIs** (recommended) | NewsAPI.org, Bing News, NewsCatcher, SerpApi. Add a provider that calls the API and maps to `NormalizedArticle`. No browser. |
| **Pre-compute** | Scheduled worker runs full pipeline (including Playwright); web app reads from DB/cache only. |
| **Browser-as-a-service** | Browserless.io, ScrapingBee, Apify: your backend calls their API; they return HTML. |
| **RSS / feeds** | Use HTTP-only feeds where available; map to `NormalizedArticle`. |
| **Disable heavy providers** | Set `enabled = false` for `scmp` in web; use Reuters + News API only for live path. |
| **Dedicated worker** | Web app enqueues job; separate worker runs pipeline with Playwright and writes results to DB. |

**Recommendation:** Short term: disable SCMP in web and use **Reuters + NewsAPI** for China. Medium term: add a scheduled/on-demand worker for full pipeline; web app reads stored results.

**Scripts:** `python -m scripts.diagnostics newsapi` (test NewsAPI key); `python -m scripts.diagnostics sabic [--working 1120.SR]` (SABIC vs working company diagnostic).

For MarketScreener/Yahoo URLs and field mapping, see **DATA_SOURCE_AND_URL_REFERENCE.md**.
