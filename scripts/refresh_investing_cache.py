"""Pre-warm Investing.com snapshots for every curated slug and commit
them to data/investing/.

Cloudflare blocks Render's egress IPs from reaching Investing.com, so
the deployed runtime can't fetch fresh data. This script runs locally
(or on a non-cloud CI runner) where the fetches succeed, then writes
JSON snapshots into the repo. probe_investing.py falls back to these
snapshots whenever the live network call fails (i.e. from Render).

Usage:
    python -m scripts.refresh_investing_cache
    python -m scripts.refresh_investing_cache --tickers ADCB.AE,2222.SR
"""
from __future__ import annotations

import argparse
import sys
import time

from src.providers.probe_investing import (
    _SLUGS, _fetch_equity_page, _fetch_consensus_page, _fetch_earnings_page,
    fetch_historical_prices, _tracked_path, _write_tracked, _slug,
    _get, _next_data,
)


def _fetch_and_track(ticker: str) -> dict[str, bool]:
    """Pull every Investing page we use for `ticker` and commit a snapshot
    per kind under data/investing/. Returns a per-kind ok/error map."""
    slug = _SLUGS.get(ticker.upper())
    out: dict[str, bool] = {}
    if not slug:
        print(f"[skip] {ticker}: no slug in _SLUGS — add it first", file=sys.stderr)
        return {"slug": False}

    # Equity page
    state = _fetch_equity_page(slug)
    if state:
        _write_tracked(slug, "equity", state)
        out["equity"] = True
    else:
        out["equity"] = False

    # Consensus page
    state = _fetch_consensus_page(slug)
    if state:
        _write_tracked(slug, "consensus", state)
        out["consensus"] = True
    else:
        out["consensus"] = False

    # Earnings page
    state = _fetch_earnings_page(slug)
    if state:
        _write_tracked(slug, "earnings", state)
        out["earnings"] = True
    else:
        out["earnings"] = False

    # Historical bars
    hp = fetch_historical_prices(ticker.upper())
    if hp and hp.get("close_series"):
        _write_tracked(slug, "historical", hp)
        out["historical"] = True
    else:
        out["historical"] = False

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", help="Comma-separated; defaults to every slug in _SLUGS")
    ap.add_argument("--delay", type=float, default=1.5,
                    help="Seconds between tickers (be polite to Cloudflare)")
    args = ap.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = sorted(_SLUGS.keys())

    print(f"Refreshing Investing.com snapshots for {len(tickers)} tickers...")
    print(f"Output: data/investing/<slug>__<kind>.json\n")

    summary: dict[str, dict[str, bool]] = {}
    for i, tk in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] {tk:18}", end=" ", flush=True)
        try:
            result = _fetch_and_track(tk)
            ok = sum(1 for v in result.values() if v)
            total = len(result)
            kinds = " ".join(k for k, v in result.items() if v)
            print(f"-> {ok}/{total} ok ({kinds})")
            summary[tk] = result
        except Exception as exc:
            print(f"-> ERROR {type(exc).__name__}: {exc}")
            summary[tk] = {"error": False}
        if i < len(tickers):
            time.sleep(args.delay)

    # Tally
    total_ok = sum(sum(1 for v in r.values() if v) for r in summary.values())
    total_attempts = sum(len(r) for r in summary.values())
    print(f"\nDone. {total_ok}/{total_attempts} fetches succeeded.")
    print("Commit the resulting data/investing/ tree to make the snapshots live.")
    return 0 if total_ok > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
