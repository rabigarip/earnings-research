"""
Bloomberg-uploaded consensus provider.

The user exports a BEST/EE estimate sheet to Excel, uploads it (CLI
drop or via POST /api/bloomberg/upload), and that data becomes the
canonical source for forward EPS / Revenue / EBITDA / Net income /
Target price / Rating split. Slots into the trust ladder above
Investing.com — so when Bloomberg data is present, the deck's slide-2
footnote reads "Consensus: Bloomberg · N analysts" automatically.

Format: long-form CSV / XLSX. One row per (ticker, period, metric).
See docs/stage2/bloomberg_upload.md for the schema.

This provider does NOT make network calls. It reads from:
    data/bloomberg/consensus.csv   (default; can be merged from .xlsx)
A future enhancement is per-ticker files (data/bloomberg/<TICKER>.csv).
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Optional

from src.services.probe_harness import Provider, persist_raw


_DEFAULT_CSV = Path(__file__).resolve().parent.parent.parent / "data" / "bloomberg" / "consensus.csv"


def _load_csv(path: Path = _DEFAULT_CSV) -> list[dict]:
    """Read the Bloomberg-uploaded long-form table. Returns list of
    dicts with all columns coerced to native types where sensible."""
    if not path.is_file():
        return []
    rows: list[dict] = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            row = {k.strip(): (v.strip() if isinstance(v, str) else v)
                    for k, v in raw.items() if k}
            # Coerce numeric fields. Leave non-numeric strings alone.
            for k in ("mean", "low", "high", "median", "std_dev",
                      "num_estimates", "buy_count", "hold_count", "sell_count"):
                v = row.get(k)
                if v in (None, ""):
                    row[k] = None
                else:
                    try:
                        row[k] = float(v)
                    except (TypeError, ValueError):
                        row[k] = None
            # Upper-case period_type / metric for easier matching
            for k in ("period_type", "metric"):
                v = row.get(k)
                row[k] = v.upper() if isinstance(v, str) else v
            rows.append(row)
    return rows


def _rows_for(ticker: str, rows: list[dict]) -> list[dict]:
    """Filter the loaded table to a single ticker (case-insensitive)."""
    t = (ticker or "").upper()
    return [r for r in rows if (r.get("ticker") or "").upper() == t]


def _pick(rows: list[dict], *, period_type: str, period_label: str | None = None,
           metric: str) -> Optional[dict]:
    """Find the first row matching (period_type, period_label?, metric)."""
    pt = period_type.upper()
    m = metric.upper()
    pl = period_label.upper() if period_label else None
    for r in rows:
        if r.get("period_type") != pt:
            continue
        if r.get("metric") != m:
            continue
        if pl is not None and (r.get("period_label") or "").upper() != pl:
            continue
        return r
    return None


def _all_annual_labels(rows: list[dict], metric: str) -> list[str]:
    """Sorted list of distinct period_labels for ANNUAL rows of `metric`."""
    out: set[str] = set()
    for r in rows:
        if r.get("period_type") == "ANNUAL" and r.get("metric") == metric.upper():
            lbl = r.get("period_label")
            if lbl:
                out.add(lbl)
    # Sort numerically. Extract a 4-digit year from labels like
    # "FY2026", "2026", "Q1_2026" etc. — strip everything non-digit first.
    import re as _re
    def _yr(s: str) -> int:
        m = _re.search(r"\d{4}", s or "")
        return int(m.group()) if m else 9999
    return sorted(out, key=_yr)


class BloombergProvider(Provider):
    name = "bloomberg"

    def __init__(self):
        # Load once per provider instance. The harness creates a fresh
        # instance per refresh, so a re-upload is picked up on next run.
        self._rows = _load_csv()

    def _ticker_rows(self, ticker: str) -> list[dict]:
        rows = _rows_for(ticker, self._rows)
        if not rows:
            raise NotImplementedError(
                f"No Bloomberg consensus rows for {ticker} in {_DEFAULT_CSV}"
            )
        return rows

    # ── Forward valuation ────────────────────────────────────

    def _fetch_valuation_forward(self, ticker: str):
        rows = self._ticker_rows(ticker)
        years = _all_annual_labels(rows, "EPS")
        bundle: dict[str, Any] = {}
        if years:
            fy1 = years[0]
            fy2 = years[1] if len(years) > 1 else None
            eps_fy1 = _pick(rows, period_type="ANNUAL", period_label=fy1, metric="EPS")
            rev_fy1 = _pick(rows, period_type="ANNUAL", period_label=fy1, metric="REVENUE")
            bundle.update({
                "fy1_year": fy1,
                "eps_fy1":  eps_fy1.get("mean") if eps_fy1 else None,
                "revenue_fy1": rev_fy1.get("mean") if rev_fy1 else None,
                "eps_fy1_detail":  eps_fy1,
                "revenue_fy1_detail": rev_fy1,
            })
            if fy2:
                eps_fy2 = _pick(rows, period_type="ANNUAL", period_label=fy2, metric="EPS")
                rev_fy2 = _pick(rows, period_type="ANNUAL", period_label=fy2, metric="REVENUE")
                bundle.update({
                    "fy2_year": fy2,
                    "eps_fy2":  eps_fy2.get("mean") if eps_fy2 else None,
                    "revenue_fy2": rev_fy2.get("mean") if rev_fy2 else None,
                })
        # Next-quarter row — first QUARTERLY EPS/REV
        nq_eps = next((r for r in rows
                        if r.get("period_type") == "QUARTERLY" and r.get("metric") == "EPS"),
                       None)
        nq_rev = next((r for r in rows
                        if r.get("period_type") == "QUARTERLY" and r.get("metric") == "REVENUE"),
                       None)
        if nq_eps:
            bundle["eps_next_q"]    = nq_eps.get("mean")
            bundle["next_q_period"] = nq_eps.get("period_label")
        if nq_rev:
            bundle["revenue_next_q"] = nq_rev.get("mean")
        if not bundle:
            raise ValueError("No EPS/Revenue rows in Bloomberg upload")
        raw_id = persist_raw(self.name, ticker, "valuation_forward",
                              {"rows": rows[:50]})
        return bundle, "", "", raw_id

    # ── Target price ─────────────────────────────────────────

    def _fetch_target_price(self, ticker: str):
        rows = self._ticker_rows(ticker)
        tp = _pick(rows, period_type="TARGET", metric="TARGET_PRICE")
        if not tp:
            raise ValueError("No TARGET_PRICE row in Bloomberg upload")
        raw_id = persist_raw(self.name, ticker, "target_price", tp)
        return {
            "mean":   tp.get("mean"),
            "high":   tp.get("high"),
            "low":    tp.get("low"),
            "median": tp.get("median"),
            "n_analysts": int(tp.get("num_estimates")) if tp.get("num_estimates") else None,
        }, tp.get("currency") or "", tp.get("as_of_date") or "", raw_id

    # ── Rating split ─────────────────────────────────────────

    def _fetch_rating_split(self, ticker: str):
        rows = self._ticker_rows(ticker)
        rt = _pick(rows, period_type="RATING", metric="RATING")
        if not rt:
            raise ValueError("No RATING row in Bloomberg upload")
        buy   = int(rt.get("buy_count")  or 0)
        hold  = int(rt.get("hold_count") or 0)
        sell  = int(rt.get("sell_count") or 0)
        total = int(rt.get("num_estimates") or buy + hold + sell or 0)
        if total == 0:
            raise ValueError("RATING row had no buy/hold/sell counts or analyst total")
        # Derive consensus label from BBG composite score where present.
        score = rt.get("mean")
        consensus = "OUTPERFORM"
        if isinstance(score, (int, float)):
            # BBG scale is 1=strong sell ... 5=strong buy
            if   score >= 4.0: consensus = "BUY"
            elif score >= 3.0: consensus = "OUTPERFORM"
            elif score >= 2.0: consensus = "HOLD"
            else:               consensus = "SELL"
        else:
            if   buy / max(total, 1) >= 0.6: consensus = "BUY"
            elif sell / max(total, 1) >= 0.4: consensus = "SELL"
            elif buy > sell:                  consensus = "OUTPERFORM"
            else:                              consensus = "HOLD"
        raw_id = persist_raw(self.name, ticker, "rating_split", rt)
        return {
            "buy":   buy,
            "hold":  hold,
            "sell":  sell,
            "total": total,
            "consensus": consensus,
        }, "", rt.get("as_of_date") or "", raw_id

    # ── Dividend yield (annual DPS / current price would need price too) ──
    # We don't synthesize dividend_yield here — BBG exports DPS, not the
    # yield ratio. Leave that to the live-price providers.
