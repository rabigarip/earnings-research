"""Peer-group valuation helper."""

from __future__ import annotations

import logging
import statistics
import yfinance as yf

logger = logging.getLogger(__name__)


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


def _fmt_mcap_usd(v) -> str:
    if not isinstance(v, (int, float)) or v <= 0:
        return "—"
    if v >= 1e12: return f"${v/1e12:,.2f}T"
    if v >= 1e9:  return f"${v/1e9:,.1f}B"
    if v >= 1e6:  return f"${v/1e6:,.0f}M"
    return f"${v:,.0f}"


def fetch_peer_rows(peer_tickers: list[str]) -> list[dict]:
    """Fetch one peer-table row per yfinance ticker.

    Returns list of dicts with: name, ticker, market_cap_fmt, pe (raw),
    pe_fmt, div_yield_fmt, ret_1y (raw), ret_1y_fmt. Tickers that fail
    to resolve are skipped with a log warning.

    Used by the slide-3 peer comparables table.
    """
    rows: list[dict] = []
    for t in peer_tickers or []:
        tt = (t or "").strip()
        if not tt:
            continue
        try:
            tk = yf.Ticker(tt)
            info = tk.info or {}
        except Exception as exc:
            logger.warning("peer fetch failed for %s: %s", tt, exc)
            continue
        name = info.get("longName") or info.get("shortName") or tt
        mcap_usd = info.get("marketCap")
        # Note: marketCap from Yahoo is in the listed currency, not USD —
        # converting properly would need an FX layer. The reference deck
        # uses listed-currency caps too (SAR for Saudi, etc.), so we render
        # the number as-is and drop the $ when currency isn't USD.
        currency = (info.get("currency") or "").upper()
        mcap_fmt = _fmt_mcap_usd(mcap_usd) if currency == "USD" else _fmt_mcap_usd(mcap_usd).replace("$", "")
        pe = info.get("trailingPE") or info.get("forwardPE")
        pe_val = float(pe) if isinstance(pe, (int, float)) and pe > 0 else None
        pe_fmt = f"{pe_val:.1f}x" if pe_val else "—"
        div_y = info.get("dividendYield")
        div_fmt = "—"
        if isinstance(div_y, (int, float)) and div_y > 0:
            # Yahoo gives dividendYield as a decimal (0.0472) for some,
            # but as a percentage (4.72) for others. Heuristic: <1 → decimal.
            div_pct = div_y * 100 if div_y < 1 else div_y
            div_fmt = f"{div_pct:.2f}%"
        # 1Y return: derive from 52w high/low if not in info, or use history
        ret_1y = None
        try:
            hist = tk.history(period="1y")
            if not hist.empty and "Close" in hist.columns:
                first = float(hist["Close"].iloc[0])
                last = float(hist["Close"].iloc[-1])
                if first > 0:
                    ret_1y = (last / first - 1.0) * 100
        except Exception:
            ret_1y = None
        ret_1y_fmt = f"{ret_1y:+.1f}%" if isinstance(ret_1y, (int, float)) else "—"
        rows.append({
            "name": name,
            "ticker": tt,
            "market_cap_fmt": mcap_fmt,
            "pe": pe_val,
            "pe_fmt": pe_fmt,
            "div_yield_fmt": div_fmt,
            "ret_1y": ret_1y,
            "ret_1y_fmt": ret_1y_fmt,
        })
    return rows

