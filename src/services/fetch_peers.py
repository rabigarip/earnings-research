"""Peer-group valuation helper."""

from __future__ import annotations

import statistics
import yfinance as yf


def fetch_peer_multiples(peer_tickers: list[str]) -> dict:
    """Fetch peer forward P/E and EV/EBITDA medians from Yahoo info."""
    pe_values: list[float] = []
    ev_ebitda_values: list[float] = []
    for t in peer_tickers or []:
        tt = (t or "").strip().upper()
        if not tt:
            continue
        try:
            info = yf.Ticker(tt).info or {}
        except Exception:
            continue
        pe = info.get("forwardPE") or info.get("trailingPE")
        if isinstance(pe, (int, float)) and pe > 0:
            pe_values.append(float(pe))
        ev = info.get("enterpriseValue")
        ebitda = info.get("ebitda")
        if isinstance(ev, (int, float)) and isinstance(ebitda, (int, float)) and ebitda > 0:
            ev_ebitda_values.append(float(ev) / float(ebitda))
    return {
        "pe_sector_median": round(statistics.median(pe_values), 1) if pe_values else None,
        "ev_ebitda_sector_median": round(statistics.median(ev_ebitda_values), 1) if ev_ebitda_values else None,
        "peer_count": len(pe_values),
    }

