"""
Earnings calendar refresh service.

Populates `earnings_calendar` from Yahoo Finance (primary) with concurrent
fetching + per-host rate limiting. Safe to call repeatedly; rows upsert and
`last_checked` is refreshed on every run.

Design notes:
  - Pure network I/O, no LLM, no PPTX rendering.
  - Bounded thread pool (configurable via settings.toml → [calendar]).
  - Skips tickers re-checked within `refresh_stale_hours` (default 12h).
  - Yahoo returns a single "next earnings date" per ticker; we store that
    as one row. Past dates are ignored here (they belong in `actuals`).
"""

from __future__ import annotations

import logging
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
    """Return True if ticker's latest earnings_calendar.last_checked is within window."""
    conn = get_conn()
    row = conn.execute(
        "SELECT MAX(last_checked) AS last_checked FROM earnings_calendar WHERE ticker = ?",
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


# ── Per-ticker worker ────────────────────────────────────────────────────

def _refresh_one(ticker: str, config: CalendarConfig) -> str:
    """Returns a short status token for reporting: 'fresh'|'updated'|'none'|'error'."""
    if _is_fresh(ticker, config.refresh_stale_hours):
        return "fresh"
    _rate_limit(config.per_request_min_seconds)
    try:
        info = _fetch_yahoo(ticker, config.http_timeout_seconds)
    except Exception as exc:
        log.info("[calendar] %s error: %s", ticker, exc)
        return "error"
    if not info:
        return "none"
    try:
        upsert_calendar_event(
            ticker=ticker,
            event_date=info["event_date"],
            confirmed=bool(info.get("confirmed")),
            consensus_revenue=info.get("consensus_revenue"),
            consensus_eps=info.get("consensus_eps"),
            source=info.get("source", "yahoo"),
        )
    except Exception as exc:
        log.warning("[calendar] %s upsert failed: %s", ticker, exc)
        return "error"
    return "updated"


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
