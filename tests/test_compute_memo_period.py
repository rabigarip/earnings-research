"""
Regression tests for `_compute_memo`'s next-quarter resolution.

Bug we hit on 2026-05-09 (2020.SR earnings preview audit)
---------------------------------------------------------
When `ms_calendar_events` had no `next_expected_earnings_date`/_label
(MS doesn't always surface one even when the quarterly grid does),
`_compute_memo` fell back to `periods[-1]` of the /finances/ quarterly
grid. For tickers where MS publishes 3+ forward forecasts, that picked
the FURTHEST-OUT quarter (e.g. 2026 Q4) instead of the next-to-be-
reported quarter (e.g. 2026 Q2 when Q1 had just released).

Downstream effects of the wrong next_quarter_label:
  * `out["next_quarter_consensus_revenue"]` looked up the wrong quarter
    (Q4 2715 instead of Q2 3097 for 2020.SR)
  * `out["yoy_revenue_pct_table"]` computed Q4 26E vs Q4 25A (-15%)
    instead of Q2 26E vs Q2 25A (~-5.8%)
  * `out["preview_quarter_label"]` printed "Q4 2026 Earnings Preview"
    on the cover even though the next release was Q2

Meanwhile `_resolve_quarterly_mode` in build_report_context.py
correctly picked Q2 via `_resolve_annual_indices`. Net result: the
deck's slide-2 Revenue card showed 3,097 (Q2 26E) with a -15.0% delta
(Q4-vs-Q4) — value and delta from different quarters.

Fix: pick the first quarter without an announcement_date — same logic
`_resolve_annual_indices` uses — so both code paths agree.

These tests pin the new behaviour against shaped fixture inputs that
exercise:
  * the canonical case (some quarters released, some not) → first
    unreleased period wins
  * the all-released edge (every period has a date) → fall back to
    `periods[-1]` so we don't crash
  * the "no quarterly forecasts at all" case → next_quarter_label stays
    None so downstream code does nothing
"""

from __future__ import annotations

from src.services.build_report_payload import _compute_memo
from src.models.company import CompanyMaster
from src.models.financials import QuoteSnapshot


def _company():
    return CompanyMaster(
        ticker="TEST.SR", company_name="Test Co",
        sector="X", industry="Y", country="SA", currency="SAR", is_bank=False,
    )


def _quote():
    return QuoteSnapshot(ticker="TEST.SR", price=100.0, currency="SAR")


def _ms_q(periods, dates, sales):
    """Build a minimal ms_quarterly_forecasts dict the memo logic accepts."""
    return {
        "quarterly": {
            "periods": periods,
            "announcement_dates": dates,
            "net_sales": sales,
            "ebitda": [None] * len(periods),
            "ebit": [None] * len(periods),
            "net_income": [None] * len(periods),
            "eps": [None] * len(periods),
        }
    }


class TestNextQuarterFromFinancesFallback:
    """When MS calendar has no next-earnings hint, the /finances/ quarterly
    grid is the only source of truth for the next quarter to be reported."""

    def test_picks_first_unreleased_quarter_not_the_last(self):
        """The 2020.SR scenario: Q1 26 just released, Q2/Q3/Q4 26 are
        forward forecasts. The next quarter to report is Q2, not Q4."""
        periods = [
            "2025Q1", "2025Q2", "2025Q3", "2025Q4",
            "2026Q1", "2026Q2", "2026Q3", "2026Q4",
        ]
        dates = [
            "4/27/25", "7/27/25", "10/26/25", "3/3/26",
            "4/23/26", "-", "-", "-",
        ]
        sales = [3074, 3287, 3522, 3194, 2874, 3097, 3227, 2715]
        memo = _compute_memo(
            company=_company(), quote=_quote(),
            quarterly=[], consensus=[], consensus_summary=None,
            ms_annual_forecasts=_ms_q(periods, dates, sales),
            ms_quarterly_forecasts=_ms_q(periods, dates, sales),
            ms_eps_dividend_forecasts={}, ms_calendar_events=None,
            yahoo_earnings_date=None, derived=None,
        )
        assert memo["next_quarter_label"] == "2026 Q2"
        assert memo["preview_quarter_short"] == "2Q26"
        assert memo["prior_quarter_label"] == "2026 Q1"
        assert memo["prior_year_same_quarter_label"] == "2025 Q2"
        # Consensus value lookups should now hit the right column.
        assert memo["next_quarter_consensus_revenue"] == 3097.0

    def test_falls_back_to_last_when_all_released(self):
        """If every quarter has an announcement_date, there is no upcoming
        quarter — the historical fallback to `periods[-1]` is harmless and
        preserves the prior behaviour."""
        periods = ["2025Q1", "2025Q2", "2025Q3"]
        dates = ["4/27/25", "7/27/25", "10/26/25"]
        sales = [100, 110, 120]
        memo = _compute_memo(
            company=_company(), quote=_quote(),
            quarterly=[], consensus=[], consensus_summary=None,
            ms_annual_forecasts=_ms_q(periods, dates, sales),
            ms_quarterly_forecasts=_ms_q(periods, dates, sales),
            ms_eps_dividend_forecasts={}, ms_calendar_events=None,
            yahoo_earnings_date=None, derived=None,
        )
        # Last period as a literal — the regex normalisation only fires
        # for the first-unreleased path; the all-released fallback keeps
        # the raw label.
        assert memo["next_quarter_label"] == "2025Q3"

    def test_none_inputs_do_not_set_label(self):
        memo = _compute_memo(
            company=_company(), quote=_quote(),
            quarterly=[], consensus=[], consensus_summary=None,
            ms_annual_forecasts=None, ms_quarterly_forecasts=None,
            ms_eps_dividend_forecasts=None, ms_calendar_events=None,
            yahoo_earnings_date=None, derived=None,
        )
        assert memo["next_quarter_label"] is None

    def test_calendar_label_takes_precedence(self):
        """When MS calendar surfaces a next-earnings label, the /finances/
        fallback should not run — the calendar wins."""
        # Calendar provides an explicit "Q3 2026" label.
        cal = {"next_expected_earnings_label": "Q3 2026 Earnings Release"}
        periods = ["2026Q1", "2026Q2", "2026Q3", "2026Q4"]
        dates = ["4/23/26", "-", "-", "-"]  # /finances/ would say Q2
        sales = [2874, 3097, 3227, 2715]
        memo = _compute_memo(
            company=_company(), quote=_quote(),
            quarterly=[], consensus=[], consensus_summary=None,
            ms_annual_forecasts=_ms_q(periods, dates, sales),
            ms_quarterly_forecasts=_ms_q(periods, dates, sales),
            ms_eps_dividend_forecasts={}, ms_calendar_events=cal,
            yahoo_earnings_date=None, derived=None,
        )
        # Calendar wins: Q3 2026, not /finances/'s Q2 2026.
        assert memo["next_quarter_label"] == "2026 Q3"

    def test_period_label_normalized_to_spaced_form(self):
        """Output label should use "2026 Q2" (with space) so it matches
        the format used by other code paths in `_compute_memo`."""
        periods = ["2026Q1", "2026Q2"]
        dates = ["4/23/26", "-"]
        sales = [2874, 3097]
        memo = _compute_memo(
            company=_company(), quote=_quote(),
            quarterly=[], consensus=[], consensus_summary=None,
            ms_annual_forecasts=_ms_q(periods, dates, sales),
            ms_quarterly_forecasts=_ms_q(periods, dates, sales),
            ms_eps_dividend_forecasts={}, ms_calendar_events=None,
            yahoo_earnings_date=None, derived=None,
        )
        # MS emits "2026Q2" but our normaliser produces "2026 Q2".
        assert memo["next_quarter_label"] == "2026 Q2"
        assert " Q" in memo["next_quarter_label"]
