"""
Yahoo Finance probe provider (Stage 1).

Wraps `yfinance` calls and emits one field per `_fetch_<field>` method,
following the Provider contract in `probe_harness.py`.

Coverage prior: strong for US / India / China / HK, weak-to-broken for
MENA (most `.OM` and some `.AE` return 404 on .info). The probe makes
this explicit per ticker × field rather than aggregating to a single
"yahoo works" boolean.

Caching strategy: yfinance objects are NOT cached at the library level
between calls within one probe run, so we accept the per-call cost.
Raw responses (the JSON `info` dict, the DataFrame rows) get
persisted via `persist_raw` so re-running the reconciler is free.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Optional

try:
    import yfinance as yf
except ImportError:  # pragma: no cover — yfinance is a hard dep, but tolerate
    yf = None  # type: ignore

from src.services.probe_harness import Provider, persist_raw


def _safe(d: dict | None, key: str, default=None):
    if not d:
        return default
    v = d.get(key)
    if v is None:
        return default
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return default
    return v


def _df_to_records(df) -> list[dict]:
    """Convert a yfinance DataFrame to list-of-dicts safely (NaN → None)."""
    if df is None or df.empty:
        return []
    try:
        records = []
        for col in df.columns:
            row = {"period_end": str(col)}
            for idx in df.index:
                val = df.loc[idx, col]
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    val = None
                else:
                    val = float(val) if isinstance(val, (int, float)) else str(val)
                row[str(idx)] = val
            records.append(row)
        return records
    except Exception:
        return []


class YahooProvider(Provider):
    name = "yahoo"

    def __init__(self):
        if yf is None:
            raise RuntimeError("yfinance is not installed")
        self._ticker_cache: dict[str, Any] = {}

    def _t(self, ticker: str):
        """Memoise the yfinance Ticker object across fields of one probe."""
        if ticker not in self._ticker_cache:
            self._ticker_cache[ticker] = yf.Ticker(ticker)
        return self._ticker_cache[ticker]

    # ── Identity / market ──

    def _fetch_current_price(self, ticker: str):
        t = self._t(ticker)
        info = t.info or {}
        raw_id = persist_raw(self.name, ticker, "current_price", info)
        price = (
            _safe(info, "currentPrice")
            or _safe(info, "regularMarketPrice")
            or _safe(info, "previousClose")
        )
        if price is None:
            raise ValueError("no price in info")
        return (
            round(float(price), 4),
            (info.get("currency") or ""),
            "",  # live quote
            raw_id,
        )

    def _fetch_historical_prices(self, ticker: str):
        t = self._t(ticker)
        # 1-year daily history is enough for the test panel.
        hist = t.history(period="1y", interval="1d", auto_adjust=False)
        if hist is None or hist.empty:
            raise ValueError("no historical bars")
        records = []
        for idx, row in hist.iterrows():
            records.append({
                "date": str(idx)[:10],
                "open": float(row["Open"]) if not math.isnan(row["Open"]) else None,
                "high": float(row["High"]) if not math.isnan(row["High"]) else None,
                "low":  float(row["Low"])  if not math.isnan(row["Low"])  else None,
                "close":float(row["Close"]) if not math.isnan(row["Close"]) else None,
                "volume": int(row["Volume"]) if not math.isnan(row["Volume"]) else None,
            })
        raw_id = persist_raw(self.name, ticker, "historical_prices", records)
        info = t.info or {}
        return (
            {"n_bars": len(records), "first": records[0]["date"], "last": records[-1]["date"]},
            (info.get("currency") or ""),
            records[-1]["date"],
            raw_id,
        )

    def _fetch_market_cap(self, ticker: str):
        info = self._t(ticker).info or {}
        raw_id = persist_raw(self.name, ticker, "market_cap", info)
        mcap = _safe(info, "marketCap")
        if mcap is None:
            raise ValueError("no marketCap")
        return (
            float(mcap),
            (info.get("currency") or ""),
            "",
            raw_id,
        )

    def _fetch_company_profile(self, ticker: str):
        info = self._t(ticker).info or {}
        raw_id = persist_raw(self.name, ticker, "company_profile", info)
        profile = {
            "name":       info.get("shortName") or info.get("longName"),
            "sector":     info.get("sector"),
            "industry":   info.get("industry"),
            "country":    info.get("country"),
            "currency":   info.get("currency"),
            "summary":    (info.get("longBusinessSummary") or "")[:500],
            "website":    info.get("website"),
        }
        if not any(profile.values()):
            raise ValueError("info empty — Yahoo has no profile for this ticker")
        return (profile, "", "", raw_id)

    # ── Financials ──

    def _fetch_income_statement_annual(self, ticker: str):
        t = self._t(ticker)
        df = t.financials  # annual
        recs = _df_to_records(df)
        if not recs:
            raise ValueError("no annual income statement")
        raw_id = persist_raw(self.name, ticker, "income_statement_annual", recs)
        info = t.info or {}
        return (
            {"n_periods": len(recs), "fields_present": list({k for r in recs for k in r.keys()})[:10]},
            (info.get("financialCurrency") or info.get("currency") or ""),
            recs[0].get("period_end", ""),
            raw_id,
        )

    def _fetch_income_statement_quarterly(self, ticker: str):
        t = self._t(ticker)
        df = t.quarterly_financials
        recs = _df_to_records(df)
        if not recs:
            raise ValueError("no quarterly income statement")
        raw_id = persist_raw(self.name, ticker, "income_statement_quarterly", recs)
        info = t.info or {}
        return (
            {"n_periods": len(recs), "fields_present": list({k for r in recs for k in r.keys()})[:10]},
            (info.get("financialCurrency") or info.get("currency") or ""),
            recs[0].get("period_end", ""),
            raw_id,
        )

    def _fetch_balance_sheet(self, ticker: str):
        t = self._t(ticker)
        df = t.balance_sheet  # annual
        recs = _df_to_records(df)
        if not recs:
            raise ValueError("no balance sheet")
        raw_id = persist_raw(self.name, ticker, "balance_sheet", recs)
        info = t.info or {}
        return (
            {"n_periods": len(recs), "fields_present": list({k for r in recs for k in r.keys()})[:10]},
            (info.get("financialCurrency") or info.get("currency") or ""),
            recs[0].get("period_end", ""),
            raw_id,
        )

    def _fetch_cash_flow(self, ticker: str):
        t = self._t(ticker)
        df = t.cashflow  # annual
        recs = _df_to_records(df)
        if not recs:
            raise ValueError("no cash flow")
        raw_id = persist_raw(self.name, ticker, "cash_flow", recs)
        info = t.info or {}
        return (
            {"n_periods": len(recs), "fields_present": list({k for r in recs for k in r.keys()})[:10]},
            (info.get("financialCurrency") or info.get("currency") or ""),
            recs[0].get("period_end", ""),
            raw_id,
        )

    # ── Valuation ──

    def _fetch_valuation_historical(self, ticker: str):
        """Yahoo doesn't ship a full multi-year P/E history. The closest
        we get is trailingPE, forwardPE, priceToBook from `info`.
        This will show as low coverage in the matrix — that's the
        honest read for Yahoo on this field."""
        info = self._t(ticker).info or {}
        raw_id = persist_raw(self.name, ticker, "valuation_historical", info)
        vals = {
            "trailing_pe":   _safe(info, "trailingPE"),
            "forward_pe":    _safe(info, "forwardPE"),
            "price_to_book": _safe(info, "priceToBook"),
            "ev_to_ebitda":  _safe(info, "enterpriseToEbitda"),
            "ev_to_revenue": _safe(info, "enterpriseToRevenue"),
        }
        if all(v is None for v in vals.values()):
            raise ValueError("no valuation ratios in info")
        return (vals, "ratio", "", raw_id)

    def _fetch_dividend_yield(self, ticker: str):
        info = self._t(ticker).info or {}
        raw_id = persist_raw(self.name, ticker, "dividend_yield", info)
        # yfinance returns this as a decimal (0.048 = 4.8%). Normalise to %.
        y = _safe(info, "dividendYield") or _safe(info, "trailingAnnualDividendYield")
        if y is None:
            raise ValueError("no dividend yield")
        return (round(float(y) * 100, 3), "%", "", raw_id)

    def _fetch_valuation_forward(self, ticker: str):
        info = self._t(ticker).info or {}
        raw_id = persist_raw(self.name, ticker, "valuation_forward", info)
        vals = {
            "forward_pe":     _safe(info, "forwardPE"),
            "forward_eps":    _safe(info, "forwardEps"),
            "price_to_sales": _safe(info, "priceToSalesTrailing12Months"),
        }
        if all(v is None for v in vals.values()):
            raise ValueError("no forward valuation fields")
        return (vals, "ratio", "", raw_id)

    # ── Analyst ──

    def _fetch_target_price(self, ticker: str):
        info = self._t(ticker).info or {}
        raw_id = persist_raw(self.name, ticker, "target_price", info)
        mean = _safe(info, "targetMeanPrice")
        if mean is None:
            raise ValueError("no targetMeanPrice")
        return ({
            "mean":   _safe(info, "targetMeanPrice"),
            "median": _safe(info, "targetMedianPrice"),
            "high":   _safe(info, "targetHighPrice"),
            "low":    _safe(info, "targetLowPrice"),
            "n_analysts": _safe(info, "numberOfAnalystOpinions"),
        }, (info.get("currency") or ""), "", raw_id)

    def _fetch_rating_split(self, ticker: str):
        """Yahoo doesn't expose a clean B/H/S bucket count for non-US.
        `recommendationKey` is a single bucket (e.g. "buy"). For
        the panel test we capture it as a one-element distribution —
        the reconciler decides how to compare to MS's 5-bucket split."""
        info = self._t(ticker).info or {}
        raw_id = persist_raw(self.name, ticker, "rating_split", info)
        key = info.get("recommendationKey")
        n = _safe(info, "numberOfAnalystOpinions")
        if not key:
            raise ValueError("no recommendationKey")
        return ({"consensus": key, "n_analysts": n}, "", "", raw_id)
