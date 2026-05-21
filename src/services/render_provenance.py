"""Provenance .xlsx sidecar — one row per number on the deck.

Walks `canonical_store` for the ticker, plus the memo computed values
and the macro snapshot, and emits an Excel workbook the analyst can
open alongside the .pptx to audit every figure: source provider,
URL, the year/quarter the value belongs to, and when it was fetched.

This file is the answer to "where did each number come from, and for
which period?" — the single sidecar the user asked for. LLM-generated
prose cells are marked `LLM (Gemini)` with the upstream numeric anchor
ID list rather than left ambiguous.

Layout:
  Sheet "Provenance"
    Slide | Section | Metric | Value | Source | Source URL | Data Period | Fetched At | Notes
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from src.services.canonical_store import (
    get_all_fields, get_observations_by_provider,
)


# ── Field → slide / section / human label mapping ────────────────
#
# Maps canonical_store field names to where they appear on the deck.
# Fields not in the map are still surfaced (as "Other / Other") so the
# analyst sees the full inventory; this only controls grouping.
_FIELD_MAP: dict[str, tuple[str, str, str]] = {
    # field name              (slide, section, human label)
    "company_profile":        ("Slide 1", "Header", "Company profile"),
    "quote":                  ("Slide 1", "Header", "Quote"),
    "rating_split":           ("Slide 1", "Analyst Consensus", "Rating split (buy/hold/sell)"),
    "consensus_target":       ("Slide 1", "Analyst Consensus", "Average target price"),
    "valuation_forward":      ("Slide 1", "Key Data / Slide 2 Table",
                                "Forward P/E + Q+1 forecasts"),
    "valuation_historical":   ("Slide 3", "P/E Chart", "Historical P/E series"),
    "historical_prices":      ("Slide 3", "52-Week Price Chart",
                                "Daily closes + 52w range + performance buckets"),
    "income_statement_quarterly": ("Slide 3", "Earnings History Chart",
                                     "Per-quarter actual vs estimate surprise track"),
    "income_statement_annual":    ("Slide 2", "Estimates Table (history)",
                                     "Annual revenue / EBITDA / NI history"),
    "broker_actions":         ("Slide 2", "Catalysts (track-record anchor)", "Recent broker actions"),
    "dividends_payments":     ("Slide 1", "Key Data", "Dividend yield"),
    "earnings_calendar":      ("Slide 1", "Header", "Next earnings date"),
}


# ── Helpers ──────────────────────────────────────────────────────

def _value_repr(v: Any) -> str:
    """Compact text rendering of a canonical_value for the Value column.
    Dicts and lists are collapsed to their key list so the row stays
    legible — the analyst can drill into the raw .json fixtures if a
    deeper view is needed."""
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        if abs(v) >= 1e9:
            return f"{v:,.2f}"
        if abs(v) >= 1e6:
            return f"{v:,.0f}"
        if abs(v) >= 1:
            return f"{v:,.2f}"
        return f"{v:.4f}"
    if isinstance(v, dict):
        keys = list(v.keys())[:6]
        more = " …" if len(v) > 6 else ""
        return f"<dict: {', '.join(keys)}>{more}"
    if isinstance(v, list):
        return f"<list[{len(v)}]>"
    s = str(v)
    return s if len(s) <= 80 else s[:77] + "…"


def _source_url(provider: str, ticker: str) -> str:
    """Best-effort URL for the canonical source. We don't keep per-cell
    URLs in canonical_store (the raw_observations table holds those),
    so this gives the analyst the right page to land on for a given
    provider."""
    p = (provider or "").lower()
    if "investing" in p:
        return "https://www.investing.com (per-equity slug, see data/investing/)"
    if "marketscreener" in p:
        return "https://www.marketscreener.com"
    if "yahoo" in p or "yfinance" in p:
        return f"https://finance.yahoo.com/quote/{ticker}"
    if "bloomberg" in p:
        return "Analyst Bloomberg export (data/bloomberg/)"
    if p in ("imf", "imf weo"):
        return "https://www.imf.org/external/datamapper"
    if p in ("wb", "world bank", "worldbank"):
        return "https://api.worldbank.org/v2"
    if "gemini" in p or "llm" in p:
        return "https://aistudio.google.com (Gemini API)"
    return ""


def _data_period_from_value(field: str, value: Any) -> str:
    """Pull a human period tag from the value payload when present.
    Different providers store period tags under different keys —
    this is the de-facto union over what we've seen in the wild."""
    if not isinstance(value, dict):
        return ""
    # Macro fields: explicit year tag with source label.
    if field == "company_profile":
        bits = []
        if value.get("macro_year"):
            bits.append(f"macro {value.get('macro_year')}")
        if value.get("gdp_growth_fcst_year"):
            bits.append(f"IMF GDP {value.get('gdp_growth_fcst_year')}F")
        if value.get("inflation_fcst_year"):
            bits.append(f"IMF infl {value.get('inflation_fcst_year')}F")
        if bits:
            return ", ".join(bits)
    # Forecast bundle: surface fy1_year / next_q_period as the period anchor.
    if field == "valuation_forward":
        bits = []
        if value.get("next_q_period"):
            bits.append(str(value["next_q_period"]))
        if value.get("fy1_year"):
            bits.append(f"FY{value['fy1_year']}E")
        return ", ".join(bits)
    # Generic keys some providers stamp.
    for k in ("period", "as_of", "year", "fiscal_year", "report_year", "asof"):
        if value.get(k):
            return str(value[k])
    return ""


# ── Main entry ───────────────────────────────────────────────────

def write_provenance_xlsx(ticker: str, out_path: Path,
                            memo_data: Optional[dict] = None) -> Optional[Path]:
    """Walk canonical_store + memo + macro for `ticker` and write an
    .xlsx audit sheet to `out_path`. Returns the path on success, None
    if openpyxl is unavailable (kept optional so the deck still ships).
    """
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, Alignment, PatternFill
        from openpyxl.utils import get_column_letter
    except ImportError:
        return None

    wb = Workbook()
    ws = wb.active
    ws.title = "Provenance"
    headers = ["Slide", "Section", "Metric", "Value", "Source",
                 "Source URL", "Data Period", "Fetched At", "Notes"]
    ws.append(headers)
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="0D1117")
    for col_idx, _ in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="left", vertical="center")

    rows: list[list[str]] = []

    # 1. Every canonical_store cell for the ticker.
    cv = get_all_fields(ticker)
    for field, c in sorted(cv.items()):
        slide, section, label = _FIELD_MAP.get(field, ("Other", "Other", field))
        period = _data_period_from_value(field, c.value)
        # If the cell value is a dict carrying its own per-key sources,
        # we still emit one row at the field level — the deck consumes
        # the dict as one unit.
        rows.append([
            slide, section, label,
            _value_repr(c.value),
            (c.canonical_source or "").strip(),
            _source_url(c.canonical_source, ticker),
            period,
            c.last_refreshed_at.strftime("%Y-%m-%d %H:%M UTC"),
            (c.notes or "").strip(),
        ])

    # 2. Memo-derived fields (next_quarter_consensus_revenue/eps, source).
    if memo_data:
        nq_src = memo_data.get("next_quarter_consensus_source")
        nq_label = memo_data.get("next_earnings_label") or ""
        if memo_data.get("next_quarter_consensus_revenue") is not None:
            rows.append([
                "Slide 2", "Estimates Table",
                f"Q+1 consensus revenue ({nq_label})",
                _value_repr(memo_data["next_quarter_consensus_revenue"]),
                nq_src or "MarketScreener",
                _source_url(nq_src or "MarketScreener", ticker),
                nq_label, "", "Cascade: MS → Investing → Yahoo",
            ])
        if memo_data.get("next_quarter_consensus_eps") is not None:
            rows.append([
                "Slide 2", "Estimates Table",
                f"Q+1 consensus EPS ({nq_label})",
                _value_repr(memo_data["next_quarter_consensus_eps"]),
                nq_src or "MarketScreener",
                _source_url(nq_src or "MarketScreener", ticker),
                nq_label, "", "Cascade: MS → Investing → Yahoo",
            ])
        # Implied upside / target reflected in the deck.
        if memo_data.get("implied_upside_pct") is not None:
            rows.append([
                "Slide 1", "Analyst Consensus", "Implied upside %",
                _value_repr(memo_data["implied_upside_pct"]),
                "Derived",
                "(target_mean / quote.price - 1) × 100",
                "", "", "Per-cell formula",
            ])

    # 3. LLM-generated prose (thesis, catalysts, risks, watch_list, highlights).
    # These are not in canonical_store; we surface them explicitly so the
    # analyst sees what the model produced AND that no numerics live here.
    try:
        from src.services.llm_summary import generate_summary
        llm = generate_summary(ticker)
    except Exception:
        llm = None
    if llm:
        as_of = (llm.get("as_of") or "").replace("T", " ")[:19]
        ctx_hash = llm.get("context_hash") or ""
        thesis = (llm.get("thesis_paragraph") or "").strip()
        if thesis:
            rows.append([
                "Slide 2", "Investment Thesis", "Executive summary (4-sentence)",
                thesis[:120] + ("…" if len(thesis) > 120 else ""),
                "LLM (Gemini)",
                _source_url("gemini", ticker),
                "", as_of, f"context_hash={ctx_hash}; numeric-trace validated",
            ])
        for i, c in enumerate(llm.get("catalysts") or [], start=1):
            rows.append([
                "Slide 2", "Catalysts", f"Catalyst #{i}",
                str(c)[:120],
                "LLM (Gemini)",
                _source_url("gemini", ticker),
                "", as_of, f"context_hash={ctx_hash}",
            ])
        for i, c in enumerate(llm.get("risks") or [], start=1):
            rows.append([
                "Slide 2", "Risks", f"Risk #{i}",
                str(c)[:120],
                "LLM (Gemini)",
                _source_url("gemini", ticker),
                "", as_of, f"context_hash={ctx_hash}",
            ])
        for i, c in enumerate(llm.get("watch_list") or [], start=1):
            rows.append([
                "Slide 2", "What to Watch", f"Watch #{i}",
                str(c)[:120],
                "LLM (Gemini)",
                _source_url("gemini", ticker),
                "", as_of, f"context_hash={ctx_hash}",
            ])
        for it in (llm.get("highlights") or []):
            cat = (it.get("category") or "").strip().upper()
            body = (it.get("body") or "").strip()
            if cat and body:
                rows.append([
                    "Slide 1", f"Analyst Highlights — {cat}", "Pill body",
                    body[:120],
                    "LLM (Gemini)",
                    _source_url("gemini", ticker),
                    "", as_of, f"context_hash={ctx_hash}",
                ])

    # Sort by slide then section for a clean read.
    def _slide_sort(r):
        s = (r[0] or "")
        # "Slide 1" < "Slide 2" < ... < "Other"
        if s.startswith("Slide "):
            try:
                return (0, int(s.split(" ", 1)[1]), r[1])
            except (ValueError, IndexError):
                pass
        return (1, 0, r[1])
    rows.sort(key=_slide_sort)
    for r in rows:
        ws.append(r)

    # Column widths sized to typical content. openpyxl doesn't auto-fit.
    widths = [9, 32, 36, 32, 16, 48, 22, 22, 50]
    for col_idx, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = w
    ws.freeze_panes = "A2"

    # Metadata sheet — quick at-a-glance: ticker + run timestamp + counts.
    meta = wb.create_sheet("About")
    meta.append(["Ticker", ticker])
    meta.append(["Generated (UTC)", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")])
    meta.append(["Rows", len(rows)])
    meta.append([])
    meta.append(["Notes", (
        "Every numeric value on the deck should trace to a row here. "
        "LLM-generated prose has no numeric source — its numbers are "
        "validated against the canonical_store anchors at generation time."
    )])
    for col in ("A", "B"):
        meta.column_dimensions[col].width = 28
    meta["A1"].font = Font(bold=True)
    meta["A2"].font = Font(bold=True)
    meta["A3"].font = Font(bold=True)
    meta["A5"].font = Font(bold=True)
    meta["B5"].alignment = Alignment(wrap_text=True, vertical="top")
    meta.row_dimensions[5].height = 60

    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(str(out_path))
    return out_path
