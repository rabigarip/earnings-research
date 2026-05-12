# Production wire-up — promoting the probe stack to a live pipeline

**Goal.** The Stage 1 / 2 probe stack proves the data is reachable. This doc describes how to promote it from "demo running once" to "daily-refreshed canonical store the report generator reads from." It's an architecture spec plus a sequenced implementation plan.

## What changes (and what doesn't)

**Keeps:**
- Provider modules (`src/providers/probe_*.py`) — the network code is production-grade.
- `Provider` contract (`fetch(ticker, field) → ProbeCell`) — unchanged.
- `src/services/reconcile_sources.py` — canonical value selection logic.
- SQLite as the storage layer (`src/storage/db.py`).
- Existing `company_master` + `financial_snapshots` + `earnings_calendar` tables.

**Adds:**
- Two new SQLite tables: `coverage_observations` and `reconciled_values`.
- A nightly refresh runner: `scripts/daily_refresh.py`.
- A read-side service: `src/services/canonical_store.py` that the report generator calls.
- Lock-file based single-flight protection so a 25-minute refresh doesn't double-run.

**Removes:**
- Direct provider calls from `generate_report.py` — report generation reads only from `reconciled_values`, never from the network.

## The data model

### `coverage_observations` (new table)

One row per (run, ticker, field, provider) observation. This is the immutable audit log — every probe attempt is recorded with raw response ID so we can reconstruct any historical canonical value.

```sql
CREATE TABLE coverage_observations (
    observation_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            TEXT NOT NULL,                  -- e.g. "2026-05-12T03:00"
    observed_at       TEXT NOT NULL,                  -- ISO8601 UTC
    ticker            TEXT NOT NULL,
    field             TEXT NOT NULL,
    provider          TEXT NOT NULL,
    value_json        TEXT,                           -- canonical JSON-encoded value
    units             TEXT DEFAULT '',
    as_of             TEXT DEFAULT '',
    raw_response_id   TEXT DEFAULT '',                -- pointer to cache/probe/...
    error             TEXT DEFAULT '',
    latency_ms        INTEGER,
    FOREIGN KEY (ticker) REFERENCES company_master(ticker)
);
CREATE INDEX idx_obs_ticker_field ON coverage_observations(ticker, field);
CREATE INDEX idx_obs_run         ON coverage_observations(run_id);
```

### `reconciled_values` (new table)

One row per (ticker, field) — the **current** canonical value. Replaced each refresh. This is the table the report generator queries.

```sql
CREATE TABLE reconciled_values (
    ticker                 TEXT NOT NULL,
    field                  TEXT NOT NULL,
    canonical_value_json   TEXT NOT NULL,
    canonical_source       TEXT NOT NULL,
    confidence             TEXT NOT NULL,             -- High / Medium / Low
    sources_with_value     TEXT,                      -- CSV
    sources_agreeing       TEXT,                      -- CSV
    max_disagreement_pct   REAL,
    last_refreshed_at      TEXT NOT NULL,
    last_observation_id    INTEGER,                   -- pointer to the obs this came from
    notes                  TEXT DEFAULT '',
    PRIMARY KEY (ticker, field),
    FOREIGN KEY (ticker) REFERENCES company_master(ticker)
);
```

## Refresh schedule (per-field cadence)

Not every field needs to refresh daily. Saving cost (and respecting rate limits) requires tiered cadences:

| Field | Cadence | Why |
|---|---|---|
| `current_price` | hourly during market hours | Live data; analyst needs intraday for the cover page |
| `market_cap` | daily (4am UTC) | Settles after close |
| `historical_prices` | daily | Append last day's bar |
| `dividend_yield`, `valuation_historical` | daily | Tracks current_price |
| `valuation_forward`, `target_price`, `rating_split` | weekly (Sunday 4am UTC) | Analyst notes move on a weekly cadence |
| `income_statement_*`, `balance_sheet`, `cash_flow` | quarterly (next-day after each company's earnings, plus rolling weekly retry) | Filings drop on earnings dates; the rolling retry catches late publishers |
| `company_profile` | monthly | Effectively static |

Implementation: `scripts/daily_refresh.py` takes a `--cadence={hourly,daily,weekly,quarterly}` flag and queries which (ticker, field) pairs are due.

## The refresh pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│  scripts/daily_refresh.py --cadence=daily                       │
│                                                                 │
│  1. Acquire lock (cache/refresh.lock; PID + start_ts)           │
│  2. Compute target cells = company_master × due_fields(cadence) │
│  3. For each (ticker, field):                                   │
│       for provider in wired_providers:                          │
│           cell = provider.fetch(ticker, field)                  │
│           INSERT coverage_observations (...)                    │
│  4. For each (ticker, field):                                   │
│       reconciled = reconcile_one_cell(ticker, field)            │
│       UPSERT reconciled_values (...)                            │
│  5. Emit refresh_summary.json (per-cell hit/miss, latency)      │
│  6. Release lock                                                │
└─────────────────────────────────────────────────────────────────┘
```

## The read side

`src/services/canonical_store.py` (new):

```python
def get_canonical(ticker: str, field: str) -> Optional[CanonicalValue]:
    """Single canonical value lookup. Returns None if missing."""

def get_all_fields(ticker: str) -> dict[str, CanonicalValue]:
    """All canonical values for one ticker; the report generator's primary call."""

def get_freshness(ticker: str, field: str) -> Optional[timedelta]:
    """How stale is this cell? Used to flag 'data older than expected' in the deck footer."""
```

The report generator (`src/services/generate_report.py`) is refactored once: every call to a `probe_*` provider becomes a call to `canonical_store.get_canonical()`. No network calls inside report generation.

## Operational concerns

### Failure handling

- **Provider down (HTTP / parse error).** Logged in `coverage_observations.error`. Reconciler falls back to next provider in TRUST_LADDER. If all providers fail, the cell is marked stale but the prior `reconciled_values` row is retained — the report still ships with the last-known value plus a freshness warning.
- **Conflicting values (>5% drift).** Cell marked Low confidence; both sources' values stored in the observation log. Manual review queue (Stage 2.5 scope).
- **Rate-limit hit.** Provider returns `ProbeCell(error="rate_limited")`. The refresh runner pauses that provider for an exponentially-backed-off window (1m → 4m → 16m → drop until next run).

### Cron wiring

A single cron entry handles all cadences via a dispatcher that reads `cache/refresh.lock` to skip if another run is mid-flight:

```cron
0 * * * *  cd /opt/earnings-research && python -m scripts.daily_refresh --cadence=hourly  >> logs/refresh.log 2>&1
5 4 * * *  cd /opt/earnings-research && python -m scripts.daily_refresh --cadence=daily   >> logs/refresh.log 2>&1
5 4 * * 0  cd /opt/earnings-research && python -m scripts.daily_refresh --cadence=weekly  >> logs/refresh.log 2>&1
```

### Observability

`outputs/refresh_summary.json` is emitted after each run:

```json
{
  "run_id": "2026-05-12T04:00",
  "cadence": "daily",
  "duration_s": 187,
  "cells_attempted": 130,
  "cells_succeeded": 124,
  "cells_failed": 6,
  "per_provider": {
    "yahoo": {"hit": 78, "miss": 4, "rate_limited": 0},
    "marketscreener": {"hit": 46, "miss": 0, "rate_limited": 2},
    ...
  },
  "alerts": [
    "MS rate-limited 2 cells; will retry next run",
    "OQEP balance_sheet IR-PDF parse error (known issue)"
  ]
}
```

A trivial dashboard (`scripts/print_freshness.py`) reads this and `reconciled_values` to produce the human-readable summary the operator skims daily.

## Stage 2 implementation sequence (one engineer-week)

| Day | Deliverable |
|---|---|
| 1 | Schema migrations: `coverage_observations` + `reconciled_values` tables. Unit-test the upsert path. |
| 2 | `scripts/daily_refresh.py` skeleton: lock, cadence filter, single-provider drive. Run end-to-end against `--only yahoo` against the 10-ticker panel. |
| 3 | Wire all 7 (or 8 with Investing.com on display) providers; add `--cadence=hourly` for price-only refresh. |
| 4 | `src/services/canonical_store.py` + refactor `generate_report.py` to read from it. Regression-test against an existing deck (BKMB or 2222.SR). |
| 5 | Cron wiring; `print_freshness.py` dashboard; on-call runbook for failure modes. |

## When the paid-data feed lands

Adding the paid forward-estimate provider (Cap IQ + Visible Alpha) is a single-file change:

```python
# src/providers/paid_estimates.py
class PaidEstimatesProvider(Provider):
    name = "paid_estimates"
    def _fetch_valuation_forward(self, ticker): ...
    def _fetch_target_price(self, ticker): ...
    def _fetch_rating_split(self, ticker): ...
```

Add it to `_load_providers()` and put it at the top of the TRUST_LADDER (above MarketScreener for those three fields). The next refresh picks it up; `reconciled_values` for those fields flips from Low (MS-only) to High (paid canonical + MS cross-check). No consumer code changes.

## What this gets us

End-state after Stage 2:
- **Daily-fresh canonical data** for all 13 fields × 10+ panel tickers.
- **Filing-grade** balance sheet / cash flow / income statement for any ticker where we curate an IR PDF — same trust level Bloomberg has.
- **Bloomberg-grade** prices / valuations / dividends — confirmed by cross-source agreement.
- **Bloomberg-grade forward estimates** — once Cap IQ + Visible Alpha is wired.
- **Audit trail** — every report deck can be reconstructed from `coverage_observations` for any historical date.
- **Single source of truth** — the report generator is decoupled from network state; deck-building is reproducible offline.
