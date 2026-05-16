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
    """Build a peer-table row from Investing.com when yfinance can't resolve
    the ticker (ADX / MSM listings 404 on yfinance).

    Looks up the slug via investing.com's public search API, fetches the
    equity page, decodes the __NEXT_DATA__ JSON and extracts name, market
    cap, P/E, dividend yield, and 1Y return. Returns None if any step
    fails — caller falls back to a ticker-only row.
    """
    try:
        from curl_cffi import requests as cr
    except ImportError:
        return None
    import re as _re, json as _json
    # Use the curated slug if we have one; otherwise search by ticker symbol.
    slug = None
    try:
        from src.providers.probe_investing import _SLUGS as _CURATED
        slug = _CURATED.get(ticker.upper())
    except Exception:
        pass
    if not slug:
        try:
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
    # Fetch the equity page and pull __NEXT_DATA__.
    try:
        r = cr.get(f"https://www.investing.com/equities/{slug}",
                   impersonate="chrome120", timeout=15,
                   headers={"Accept-Language": "en-US,en;q=0.9"})
        if r.status_code != 200:
            return None
        m = _re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, _re.S)
        if not m:
            return None
        d = _json.loads(m.group(1))
        state = d.get("props", {}).get("pageProps", {}).get("state") or {}
        eq = state.get("equityStore")
        if isinstance(eq, str): eq = _json.loads(eq)
        if not isinstance(eq, dict): return None
        instr = eq.get("instrument") or {}
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
        return {
            "name": name, "ticker": ticker,
            "market_cap_fmt": mcap_fmt,
            "pe": pe, "pe_fmt": pe_fmt,
            "div_yield_fmt": div_fmt,
            "ret_1y": ret_1y, "ret_1y_fmt": ret_1y_fmt,
        }
    except Exception as exc:
        logger.warning("Investing peer fetch failed for %s: %s", ticker, exc)
        return None


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

