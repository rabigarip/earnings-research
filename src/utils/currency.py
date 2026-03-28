"""Currency conversion utility (Yahoo FX with deterministic fallback)."""

from __future__ import annotations

from functools import lru_cache
import yfinance as yf


@lru_cache(maxsize=32)
def get_fx_rate(from_ccy: str, to_ccy: str) -> float:
    """Get FX rate; return 1.0 for same currency."""
    f = (from_ccy or "").strip().upper()
    t = (to_ccy or "").strip().upper()
    if not f or not t or f == t:
        return 1.0
    pair = f"{f}{t}=X"
    try:
        info = yf.Ticker(pair).info or {}
        rate = info.get("regularMarketPrice") or info.get("previousClose")
        if isinstance(rate, (int, float)) and rate > 0:
            return float(rate)
    except Exception:
        pass
    fallback = {"SARUSD": 0.2667, "USDSAR": 3.75}
    return float(fallback.get(f"{f}{t}", 1.0))


def convert(amount: float | None, from_ccy: str, to_ccy: str) -> float | None:
    """Convert amount or return None when amount is None."""
    if amount is None:
        return None
    try:
        return float(amount) * get_fx_rate(from_ccy, to_ccy)
    except Exception:
        return None

