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


def _peer_row_from_investing(ticker: str) -> dict | None:
    """Build a peer-table row from Investing.com using the same three-layer
    fallback as probe_investing (24h cache → live network → repo-tracked
    snapshot under data/investing/). Returns None when the ticker has no
    curated slug AND no snapshot.

    Critically, this routes through _fetch_equity_page rather than its own
    curl_cffi call so it picks up the data/investing/ snapshot on Render
    (Cloudflare blocks Render's egress IP from reaching Investing live).
    """
    try:
        from src.providers.probe_investing import (
            _slug, _fetch_equity_page, _equity_instrument,
        )
    except ImportError:
        return None
    slug = _slug(ticker)
    if not slug:
        # Search the API only as last resort — it also fails from Render's
        # blocked IP, so most non-curated tickers can't be resolved live.
        try:
            from curl_cffi import requests as cr
            r = cr.get(
                f"https://api.investing.com/api/search/v2/search?q={ticker.upper().replace('.','+')}&page=1&size=10",
                impersonate="chrome120", timeout=10, headers={"domain-id": "www"},
            )
            quotes = (r.json() or {}).get("quotes") or []
            for it in quotes:
                url = it.get("url") or ""
                if "/equities/" in url:
                    slug = url.replace("/equities/", "")
                    break
        except Exception:
            return None
    if not slug:
        return None
    state = _fetch_equity_page(slug)
    if not state:
        return None
    instr = _equity_instrument(state)
    if not instr:
        return None
    price_block = instr.get("price") or {}
    fund = instr.get("fundamental") or {}
    name = (instr.get("englishName") or {}).get("shortName") \
        or (instr.get("englishName") or {}).get("fullName") \
        or ticker
    currency = (price_block.get("currency") or "").upper()
    mcap = fund.get("marketCapRaw")
    mcap_fmt = _fmt_mcap_usd(mcap) if currency == "USD" else _fmt_mcap_usd(mcap).replace("$", "")
    pe = fund.get("ratio") if isinstance(fund.get("ratio"), (int, float)) else None
    pe_fmt = f"{pe:.1f}x" if pe else "—"
    div = fund.get("yield")
    div_fmt = f"{float(div):.2f}%" if isinstance(div, (int, float)) and div > 0 else "—"
    ret_1y = fund.get("oneYearReturn") if isinstance(fund.get("oneYearReturn"), (int, float)) else None
    ret_1y_fmt = f"{ret_1y:+.1f}%" if isinstance(ret_1y, (int, float)) else "—"
    pb = fund.get("priceToBook") if isinstance(fund.get("priceToBook"), (int, float)) else None
    pb_fmt = f"{pb:.1f}x" if pb else "—"
    ev_ebitda = fund.get("enterpriseToEbitda") if isinstance(fund.get("enterpriseToEbitda"), (int, float)) else None
    ev_ebitda_fmt = f"{ev_ebitda:.1f}x" if ev_ebitda else "—"
    return {
        "name": name, "ticker": ticker,
        "market_cap_fmt": mcap_fmt,
        "pe": pe, "pe_fmt": pe_fmt,
        "pb": pb, "pb_fmt": pb_fmt,
        "ev_ebitda": ev_ebitda, "ev_ebitda_fmt": ev_ebitda_fmt,
        "div_yield_fmt": div_fmt,
        "ret_1y": ret_1y, "ret_1y_fmt": ret_1y_fmt,
    }


def fetch_peer_rows(peer_tickers: list[str]) -> list[dict]:
    """Fetch one peer-table row per ticker.

    Primary: yfinance (`Ticker(t).info` + 1y history). Falls back to
    Investing.com for tickers yfinance can't resolve (UAE / Oman peers
    like ENBD.AE, FAB.AE, ENBD.AD). Tickers that fail both sources are
    skipped with a log warning.

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
            info = {}
            tk = None
        # If yfinance returned nothing usable, try Investing.com.
        if not (info.get("longName") or info.get("shortName") or info.get("marketCap")):
            inv_row = _peer_row_from_investing(tt)
            if inv_row:
                rows.append(inv_row)
            else:
                # Last resort: ticker-only row so the table layout doesn't break.
                rows.append({
                    "name": tt, "ticker": tt,
                    "market_cap_fmt": "—", "pe": None, "pe_fmt": "—",
                    "pb": None, "pb_fmt": "—",
                    "ev_ebitda": None, "ev_ebitda_fmt": "—",
                    "div_yield_fmt": "—", "ret_1y": None, "ret_1y_fmt": "—",
                })
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
        # P/B (Yahoo: priceToBook). For banks this doubles as our P/TBV
        # proxy — yfinance doesn't expose tangibleBookValue separately, so
        # bank decks render P/B in the P/TBV column with that caveat.
        pb_raw = info.get("priceToBook")
        pb_val = float(pb_raw) if isinstance(pb_raw, (int, float)) and pb_raw > 0 else None
        pb_fmt = f"{pb_val:.1f}x" if pb_val else "—"
        # EV/EBITDA (Yahoo: enterpriseToEbitda). Meaningless for banks.
        ev_raw = info.get("enterpriseToEbitda")
        ev_ebitda_val = float(ev_raw) if isinstance(ev_raw, (int, float)) and ev_raw > 0 else None
        ev_ebitda_fmt = f"{ev_ebitda_val:.1f}x" if ev_ebitda_val else "—"
        rows.append({
            "name": name,
            "ticker": tt,
            "market_cap_fmt": mcap_fmt,
            "pe": pe_val,
            "pe_fmt": pe_fmt,
            "pb": pb_val,
            "pb_fmt": pb_fmt,
            "ev_ebitda": ev_ebitda_val,
            "ev_ebitda_fmt": ev_ebitda_fmt,
            "div_yield_fmt": div_fmt,
            "ret_1y": ret_1y,
            "ret_1y_fmt": ret_1y_fmt,
        })
    return rows

