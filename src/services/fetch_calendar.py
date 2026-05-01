"""
Earnings calendar refresh service.

Populates `earnings_calendar` from three data sources in priority order:

  1. MarketScreener /calendar/ page — best GCC coverage, gives confirmed
     announce dates for most of our seeded universe
  2. Yahoo Finance (yfinance) — fallback; good for US/DM tickers
  3. Bloomberg manual export (cons_q.xlsx) — implicit next-quarter date
     from the first estimate column, for tickers with files on disk

Rows upsert by (ticker, event_date). A later source can raise the
`confirmed` flag but won't lower it. `last_checked` is refreshed on
every run.

Design notes:
  - Pure network I/O, no LLM, no PPTX rendering.
  - Bounded thread pool (configurable via settings.toml → [calendar]).
  - Skips tickers re-checked within `refresh_stale_hours` (default 12h).
"""

from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from src.config import cfg
from src.storage.db import (
    get_conn,
    list_companies,
    load_company,
    upsert_calendar_event,
)

log = logging.getLogger(__name__)


# ── Config ───────────────────────────────────────────────────────────────

@dataclass
class CalendarConfig:
    max_workers: int = 6
    per_request_min_seconds: float = 0.25  # crude RPS ceiling (~4 req/s)
    refresh_stale_hours: int = 12
    http_timeout_seconds: int = 15


def _load_config() -> CalendarConfig:
    c = cfg().get("calendar", {}) or {}
    return CalendarConfig(
        max_workers=int(c.get("max_workers", 6)),
        per_request_min_seconds=float(c.get("per_request_min_seconds", 0.25)),
        refresh_stale_hours=int(c.get("refresh_stale_hours", 12)),
        http_timeout_seconds=int(c.get("http_timeout_seconds", 15)),
    )


# ── Rate limit (global, simple token-style) ──────────────────────────────

_rl_lock = threading.Lock()
_rl_last: float = 0.0


def _rate_limit(min_seconds: float) -> None:
    global _rl_last
    with _rl_lock:
        now = time.monotonic()
        wait = (_rl_last + min_seconds) - now
        if wait > 0:
            time.sleep(wait)
        _rl_last = time.monotonic()


# ── Staleness check ──────────────────────────────────────────────────────

def _is_fresh(ticker: str, stale_hours: int) -> bool:
    """Return True if a *future* earnings event for this ticker was checked
    within the freshness window.

    The earlier implementation looked at MAX(last_checked) across ALL rows
    for the ticker. That meant a ticker whose past event was scraped 11h
    ago would be considered "fresh" even though we never re-checked the
    upcoming event — so the next earnings date never refreshed until
    `stale_hours` past the most-recent past event check. Filtering on
    `event_date >= today` fixes that.
    """
    conn = get_conn()
    row = conn.execute(
        "SELECT MAX(last_checked) AS last_checked "
        "FROM earnings_calendar "
        "WHERE ticker = ? AND event_date >= DATE('now')",
        (ticker,),
    ).fetchone()
    conn.close()
    last = (row["last_checked"] if row else None) or ""
    if not last:
        return False
    try:
        # SQLite stores in UTC naive 'YYYY-MM-DD HH:MM:SS'
        dt = datetime.fromisoformat(last.replace(" ", "T")).replace(tzinfo=timezone.utc)
    except Exception:
        return False
    return (datetime.now(timezone.utc) - dt) < timedelta(hours=stale_hours)


# ── Fetch: Yahoo ─────────────────────────────────────────────────────────

def _fetch_yahoo(ticker: str, timeout: int) -> dict | None:
    """Return a dict with parsed next-earnings info, or None if unavailable."""
    try:
        import yfinance as yf
    except ImportError:
        log.warning("yfinance not installed; calendar fetch disabled")
        return None

    try:
        cal = yf.Ticker(ticker).calendar
    except Exception as exc:
        log.info("[calendar] %s yahoo fetch failed: %s", ticker, exc)
        return None

    if not cal:
        return None

    # yfinance returns either a dict with 'Earnings Date' as [date] or [date_start, date_end],
    # or sometimes a pandas DataFrame. Normalize to a list of datetimes.
    dates: list = []
    if isinstance(cal, dict):
        raw = cal.get("Earnings Date") or cal.get("earningsDate")
        if isinstance(raw, list):
            dates = raw
        elif raw is not None:
            dates = [raw]
    else:
        # DataFrame path (older yfinance)
        try:
            raw = cal.loc["Earnings Date"].tolist()
            dates = raw
        except Exception:
            dates = []

    # Convert to datetime objects
    parsed: list[datetime] = []
    for d in dates:
        dt: datetime | None = None
        if hasattr(d, "to_pydatetime"):
            try:
                dt = d.to_pydatetime()
            except Exception:
                dt = None
        elif hasattr(d, "date"):
            try:
                dt = datetime.combine(d.date(), datetime.min.time())
            except Exception:
                dt = None
        elif isinstance(d, datetime):
            dt = d
        if dt is not None:
            parsed.append(dt)

    if not parsed:
        return None

    # First is the soonest expected earnings; second (if present) indicates a range → not confirmed.
    first = min(parsed)
    confirmed = len(parsed) == 1

    # Consensus hints (Yahoo sometimes exposes these)
    consensus_revenue = None
    consensus_eps = None
    if isinstance(cal, dict):
        consensus_eps = _as_float(cal.get("Earnings Average"))
        consensus_revenue = _as_float(cal.get("Revenue Average"))

    return {
        "event_date": first.date().isoformat(),
        "confirmed": confirmed,
        "consensus_revenue": consensus_revenue,
        "consensus_eps": consensus_eps,
        "source": "yahoo",
    }


def _as_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ── Fetch: MarketScreener ────────────────────────────────────────────────

def _ms_company_url(company_row: dict | None) -> str:
    """Resolve the MS /quote/stock/<SLUG>/ URL from whatever the seed row has.

    company_master.json uses `marketscreener_id` (the slug like
    `SABIC-AGRI-NUTRIENTS-COMP-6497974`), but legacy code paths sometimes
    expect a fully-qualified `marketscreener_company_url`. Accept either.
    Returns "" when neither field is populated.
    """
    if not company_row:
        return ""
    explicit = (company_row.get("marketscreener_company_url") or "").strip()
    if explicit:
        return explicit
    slug = (company_row.get("marketscreener_id") or "").strip()
    if not slug:
        return ""
    # MS paths look like https://www.marketscreener.com/quote/stock/<SLUG>/
    return f"https://www.marketscreener.com/quote/stock/{slug}/"


def _fetch_marketscreener(ticker: str, company_row: dict | None) -> dict | None:
    """Return {event_date, confirmed, source} from MS /calendar/ page, or None.

    Resolves the MS company URL from either `marketscreener_company_url`
    (explicit) or `marketscreener_id` (slug). Earlier this only checked the
    explicit URL field, which silently no-op'd for the entire seeded
    universe (the seed file uses `marketscreener_id`).
    """
    base = _ms_company_url(company_row)
    if not base:
        return None
    try:
        from src.providers.marketscreener_pages import fetch_calendar_events
    except ImportError:
        return None
    try:
        payload, _status = fetch_calendar_events(base, cache_key_prefix=ticker)
    except Exception as exc:
        log.info("[calendar] %s MS fetch failed: %s", ticker, exc)
        return None
    if not payload:
        return None
    date = payload.get("next_expected_earnings_date")
    if not date or not re.match(r"\d{4}-\d{2}-\d{2}", str(date)):
        return None
    return {
        "event_date": str(date)[:10],
        # MS shows confirmed dates on /calendar/ — treat presence as confirmed.
        "confirmed": True,
        "source": "marketscreener",
    }


# ── Fetch: Bloomberg xlsx (period-end of first estimate quarter) ────────

def _fetch_bloomberg(ticker: str) -> dict | None:
    """Return a placeholder event date from the cons_q.xlsx when present.

    The cons_q grid only gives us the period-END (e.g. 2026-03-31 for Q1
    2026). Announcement dates typically land 4–6 weeks after period-end —
    earlier the placeholder used the period-end itself, which planted
    tickers in the wrong calendar month (Q1 results announced in late
    April showed up under March 31).

    New behaviour: shift the period-end forward by 35 days so the ticker
    lands in the *expected* announcement month, but keep `confirmed=false`
    so the calendar UI badges the date as "estimated". The actual MS or
    Yahoo refresh, when it succeeds, will overwrite with the true date.
    """
    try:
        from src.services.bloomberg_parser import load_bloomberg_bundle
    except ImportError:
        return None
    bundle = load_bloomberg_bundle(ticker)
    if bundle is None:
        return None
    nxt = bundle.next_estimate_quarter()
    if nxt is None or not nxt.period_end:
        return None
    try:
        period_end_dt = datetime.fromisoformat(str(nxt.period_end)[:10])
    except ValueError:
        return None
    estimated_announce = (period_end_dt + timedelta(days=35)).date().isoformat()
    return {
        "event_date": estimated_announce,
        "confirmed": False,
        "period_label": nxt.period_label,
        "period_end": nxt.period_end,  # surface raw period-end for UI tooltip
        "source": "bloomberg",
    }


# ── Per-ticker worker ────────────────────────────────────────────────────

def _prune_nearby_duplicates(
    ticker: str, kept_date: str, kept_confirmed: bool, kept_source: str, *, day_window: int = 7
) -> None:
    """Drop near-duplicate rows for the same ticker within ±N days of the row
    just upserted, when the kept row dominates by confirmation rank.

    Source confirmation rank (high → low):
        marketscreener-confirmed (calendar shows date)
      > yahoo-confirmed (single date, not a range)
      > yahoo-range (Yahoo gave a window of dates)
      > bloomberg (period-end + 35 day estimate)

    Without this, MS-confirmed Apr 29 and Yahoo-range Apr 30 would render as
    two adjacent chips for the same earnings event. Now the higher-confidence
    one wins and the rest are removed.
    """
    if not kept_confirmed:
        return  # only confirmed rows can dominate
    try:
        conn = get_conn()
        conn.execute(
            """
            DELETE FROM earnings_calendar
            WHERE ticker = ?
              AND event_date != ?
              AND ABS(julianday(event_date) - julianday(?)) <= ?
              AND confirmed = 0
            """,
            (ticker, kept_date, kept_date, int(day_window)),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        log.info("[calendar] %s prune-near failed: %s", ticker, exc)


def _refresh_one(ticker: str, config: CalendarConfig) -> str:
    """Try MS → Yahoo → Bloomberg in order; upsert every hit so all three
    contribute (later sources can raise `confirmed` but not lower it).

    After each upsert from a confirmed source, prune any nearby (±7 days)
    unconfirmed rows for the same ticker so the calendar UI shows one chip
    per earnings event rather than two adjacent ones from disagreeing
    sources.

    Returns a short status token for reporting: 'fresh'|'updated'|'none'|'error'.
    """
    if _is_fresh(ticker, config.refresh_stale_hours):
        return "fresh"

    company_row = None
    try:
        company_row = load_company(ticker)
    except Exception:
        company_row = None

    got_any = False
    errored = False

    # 1. MarketScreener (best GCC coverage).
    _rate_limit(config.per_request_min_seconds)
    try:
        ms_info = _fetch_marketscreener(ticker, company_row)
    except Exception as exc:
        log.info("[calendar] %s MS error: %s", ticker, exc)
        ms_info, errored = None, True
    if ms_info:
        try:
            upsert_calendar_event(
                ticker=ticker,
                event_date=ms_info["event_date"],
                confirmed=bool(ms_info.get("confirmed")),
                source=ms_info.get("source", "marketscreener"),
            )
            _prune_nearby_duplicates(
                ticker, ms_info["event_date"], bool(ms_info.get("confirmed")),
                ms_info.get("source", "marketscreener"),
            )
            got_any = True
        except Exception as exc:
            log.warning("[calendar] %s MS upsert failed: %s", ticker, exc)
            errored = True

    # 2. Yahoo Finance.
    _rate_limit(config.per_request_min_seconds)
    try:
        y_info = _fetch_yahoo(ticker, config.http_timeout_seconds)
    except Exception as exc:
        log.info("[calendar] %s yahoo error: %s", ticker, exc)
        y_info, errored = None, True
    if y_info:
        try:
            upsert_calendar_event(
                ticker=ticker,
                event_date=y_info["event_date"],
                confirmed=bool(y_info.get("confirmed")),
                consensus_revenue=y_info.get("consensus_revenue"),
                consensus_eps=y_info.get("consensus_eps"),
                source=y_info.get("source", "yahoo"),
            )
            _prune_nearby_duplicates(
                ticker, y_info["event_date"], bool(y_info.get("confirmed")),
                y_info.get("source", "yahoo"),
            )
            got_any = True
        except Exception as exc:
            log.warning("[calendar] %s yahoo upsert failed: %s", ticker, exc)
            errored = True

    # 3. Bloomberg (local xlsx — fast, no network).
    if not got_any:
        try:
            b_info = _fetch_bloomberg(ticker)
        except Exception as exc:
            log.info("[calendar] %s bbg error: %s", ticker, exc)
            b_info = None
        if b_info:
            try:
                upsert_calendar_event(
                    ticker=ticker,
                    event_date=b_info["event_date"],
                    confirmed=bool(b_info.get("confirmed")),
                    period_label=b_info.get("period_label", ""),
                    source=b_info.get("source", "bloomberg"),
                )
                got_any = True
            except Exception as exc:
                log.warning("[calendar] %s bbg upsert failed: %s", ticker, exc)
                errored = True

    if got_any:
        return "updated"
    return "error" if errored else "none"


# ── Public API ───────────────────────────────────────────────────────────

def refresh_calendar(
    tickers: Iterable[str] | None = None,
    *,
    force: bool = False,
) -> dict:
    """Refresh earnings calendar for the given tickers (or all seeded companies).

    `force=True` bypasses the freshness check and re-fetches every ticker.
    Returns a summary dict suitable for API responses.
    """
    config = _load_config()
    if force:
        config.refresh_stale_hours = 0

    if tickers is None:
        tickers = [c["ticker"] for c in list_companies() if c.get("ticker")]
    tickers = [t for t in tickers if t]

    if not tickers:
        return {"total": 0, "updated": 0, "fresh": 0, "none": 0, "error": 0}

    counters = {"fresh": 0, "updated": 0, "none": 0, "error": 0}
    t0 = time.monotonic()

    with ThreadPoolExecutor(max_workers=config.max_workers) as pool:
        futs = {pool.submit(_refresh_one, t, config): t for t in tickers}
        for fut in as_completed(futs):
            token = fut.result()
            counters[token] = counters.get(token, 0) + 1

    elapsed = time.monotonic() - t0
    log.info(
        "[calendar] refresh complete: total=%d updated=%d fresh=%d none=%d error=%d (%.1fs)",
        len(tickers), counters["updated"], counters["fresh"],
        counters["none"], counters["error"], elapsed,
    )
    return {"total": len(tickers), **counters, "elapsed_seconds": round(elapsed, 1)}
