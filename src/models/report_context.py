"""
ReportContext — single source of truth for every number, label, and series
that any slide of the earnings preview deck consumes.

Why this exists
---------------
The legacy `_write_preview_pptx_portrait` function in `generate_report.py`
is ~1,500 lines and computes its own values for the cover, the thesis
prose, the financial table, the cards, and the charts. Each of those
paths reads from a slightly different source — MS consensus box vs.
MS valuation grid vs. memo_computed vs. Yahoo info — and quietly drifts.
That's how the cover ends up showing UPSIDE −13.3 % while the thesis prose
two inches below says "implying −10.0 % upside".

ReportContext fixes this by collapsing all those reads into ONE typed
object built once per run, in `build_report_context.py`. Every slide
function takes `ctx: ReportContext` and only reads from it. If a number
is missing from the context, that slide MUST render "—", not invent a
fallback.

Migration plan
--------------
- Phase A (this commit): introduce the dataclass + builder. Nothing reads
  from it yet.
- Phase B: swap the cover slide to consume ReportContext only.
- Phase C: thesis & cards.
- Phase D: financial table & valuation cards.
- Phase E: charts.
- Phase F: delete the duplicated computation paths in generate_report.py.

Provenance
----------
Every field that can come from multiple providers carries a
`*_source` companion (e.g. `revenue_source: str`) so the deck footer
can declare "Estimates: Bloomberg" vs. "Estimates: MarketScreener".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


# ── Per-period rows (financial snapshot) ──────────────────────────────────

@dataclass(frozen=True)
class PeriodRow:
    """One column on the financial snapshot table.

    `is_estimate` distinguishes (A) vs (E) cells. `announcement_date` is
    the report-released date for actuals; None for estimates.
    """
    label: str                    # "FY2025", "Q1 2026", etc.
    is_estimate: bool
    announcement_date: Optional[str] = None  # ISO date when actual

    revenue:    Optional[float] = None
    ebitda:     Optional[float] = None        # None if MS does not publish
    ebit:       Optional[float] = None
    net_income: Optional[float] = None
    eps:        Optional[float] = None


# ── Cover (slide 1) ───────────────────────────────────────────────────────

@dataclass(frozen=True)
class CoverData:
    """Everything the cover slide needs. No fallback logic in slide code."""
    company_name:   str
    ticker:         str
    sector:         str                       # already mapped per-exchange
    currency:       str                       # ISO-3, e.g. "SAR"
    market_cap:     Optional[float]           # in `currency`, raw units
    report_date:    Optional[str]             # ISO YYYY-MM-DD or None
    period_label:   str                       # "Q1 2026 Preview" / "FY2026 Preview"

    rating:         str                       # "OUTPERFORM" | "—"
    target_price:   Optional[float]
    upside_pct:     Optional[float]           # signed %, e.g. -13.3

    # Provenance — used for the footer "Source: …" line.
    rating_source:        str = ""            # "marketscreener" | "yahoo" | ""
    target_price_source:  str = ""
    market_cap_source:    str = ""


# ── Slide 2: Executive Summary ────────────────────────────────────────────

@dataclass(frozen=True)
class SurpriseHistory:
    """For the thesis and the optional surprise summary box."""
    avg_revenue_surprise_pct: Optional[float] = None
    avg_eps_surprise_pct:     Optional[float] = None
    quarters_observed:        int = 0
    beat_count:               int = 0
    miss_count:               int = 0


@dataclass(frozen=True)
class KeyExpectationCard:
    """One card in the Key Expectations row. `delta_pct` drives sign-color."""
    label:        str                         # "Revenue", "EPS", "EBITDA Margin"
    value_str:    str                         # already formatted
    delta_pct:    Optional[float] = None      # raw signed %; None → no chip
    delta_str:    Optional[str] = None        # already formatted (with "+" or "−")
    unit:         str = ""                    # "SARM", "SAR", "" — rendered as
                                              # a small subscript under value


@dataclass(frozen=True)
class ChartSeries:
    """The Income Statement Evolution chart on slide 2."""
    periods:           list[str] = field(default_factory=list)
    revenue:           list[Optional[float]] = field(default_factory=list)
    ebit:              list[Optional[float]] = field(default_factory=list)
    net_income:        list[Optional[float]] = field(default_factory=list)
    actuals_boundary:  int = -1               # last index that is an actual
    source_label:      str = ""               # "MarketScreener" / "Bloomberg"


@dataclass(frozen=True)
class PEHistory:
    """For the P/E chart on slide 2."""
    periods:        list[str] = field(default_factory=list)
    pe_values:      list[Optional[float]] = field(default_factory=list)
    five_yr_avg:    Optional[float] = None


@dataclass(frozen=True)
class HeadlineRef:
    """One row in the Recent Headlines sidebar."""
    headline:    str
    date:        Optional[str] = None         # ISO YYYY-MM-DD
    source:      str = ""                     # "Reuters", "Argaam", "SCMP"
    url:         str = ""                     # for hover only; not rendered


@dataclass(frozen=True)
class SummaryData:
    """Everything slide 2 reads. Thesis is plain text — generated separately."""
    period_label:        str                  # same as cover, for sub-header
    company_name:        str

    # Investment Thesis prose. Up to 4 sentences. Generated by Gemini under
    # the strict "no invented numbers / facts" prompt; or the analytical
    # fallback when Gemini fails.
    thesis_text:         str
    thesis_source:       str                  # "gemini" | "analytical_fallback"

    surprise:            SurpriseHistory
    cards:               list[KeyExpectationCard]
    income_chart:        Optional[ChartSeries]
    pe_chart:            Optional[PEHistory]

    # Sidebar — bullets generated from news + estimate revisions, capped at 4.
    headlines:           list[HeadlineRef]

    what_to_watch:       list[str]            # 0–4 short bullets; [] hides section
    catalysts:           list[str]            # 0–3
    risks:               list[str]            # 0–3


# ── Slide 3: Financial Snapshot ───────────────────────────────────────────

@dataclass(frozen=True)
class FinancialTable:
    """The financial snapshot table on slide 3.

    Shape contract: `rows` always contains a (prior, est) pair — the legacy
    deck shows two columns (A) and (E) plus a YoY column. `yoy_by_metric`
    keys are: "revenue", "ebitda", "net_income", "eps".
    """
    mode:           str                       # "quarterly" | "annual"
    rows:           list[PeriodRow]
    currency:       str                       # display currency, e.g. "SAR"
    units_label:    str                       # "(SARM)" — used on money rows
    units_label_per_share: str = ""           # "(SAR)"  — used on EPS row
    yoy_by_metric:  dict[str, Optional[float]] = field(default_factory=dict)

    # Footer attribution.
    actuals_source:   str = "Yahoo Finance"   # or "Bloomberg" / "MarketScreener"
    estimates_source: str = "MarketScreener"
    estimates_as_of:  Optional[str] = None    # ISO date of consensus snapshot


@dataclass(frozen=True)
class ValuationSummary:
    pe_fy_e:      Optional[float] = None
    ev_ebitda:    Optional[float] = None      # None for banks/insurers
    pb:           Optional[float] = None
    div_yield:    Optional[float] = None      # already in % (e.g. 4.8)
    bank_disclaimer_needed: bool = False      # → footer star/footnote


@dataclass(frozen=True)
class PriceHistorySeries:
    dates:    list[str] = field(default_factory=list)   # ISO
    prices:   list[float] = field(default_factory=list)


@dataclass(frozen=True)
class FinancialSnapshotData:
    table:           FinancialTable
    valuation:       ValuationSummary
    price_history:   Optional[PriceHistorySeries]


# ── The whole context ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class ReportContext:
    """The root object. Every slide reads from this and ONLY this.

    Build once via `src/services/build_report_context.py::build()`. Pass
    immutably into per-slide render functions. If a value is missing,
    slide code must render the "—" sentinel; it must not invent a fallback.
    """
    # Identity / run metadata
    run_id:               str
    generated_at:         datetime
    ticker:               str
    company_name:         str
    currency:             str

    # Slide payloads
    cover:                CoverData
    summary:              SummaryData
    snapshot:             FinancialSnapshotData

    # Quality flags surfaced on slide 3 footer (e.g. "MS captcha — estimates
    # from Yahoo only"). Empty list = no banner.
    quality_flags:        list[str] = field(default_factory=list)
