"""
Tests for the slide-context builders in `src/services/build_extras_context.py`.

These exercise the contract between MS payload dicts and the typed slide
dataclasses (RatingsData / SectorComparisonData / PriceActionData), with
particular attention to edge cases the live MS site is known to produce:

  * a None payload section (entity-mismatch suppression upstream)
  * empty arrays inside an otherwise-populated dict
  * "-" sentinels that must not flow through as data
  * thinly covered tickers with composite_ratings dict full of None
  * peer rows without a corresponding ESG entry on /ratings/

Builder code is the only place that filters / normalizes these — render
modules trust the dataclass — so coverage here protects every slide.
"""

from __future__ import annotations

import pytest

from src.services.build_extras_context import (
    build_price_action,
    build_ratings,
    build_sector,
)


# ─────────────────────────────────────────────────────────────────────────────
# build_ratings
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildRatings:
    def test_none_input_yields_no_data(self):
        out = build_ratings(None)
        assert out.has_data is False
        assert out.strengths == []
        assert out.weaknesses == []
        # Composites list still defined (renderer iterates without guard).
        assert out.composites == []

    def test_full_payload_round_trip(self):
        payload = {
            "strengths": ["Margins among the highest", "Sound balance sheet"],
            "weaknesses": ["Earnings growth lacks momentum"],
            "composite_ratings": {"Trader": 70, "Investor": 93, "Global": 77, "Quality": 91},
            "esg_msci_rating": "BBB",
        }
        out = build_ratings(payload)
        assert out.has_data is True
        assert out.strengths == ["Margins among the highest", "Sound balance sheet"]
        assert out.weaknesses == ["Earnings growth lacks momentum"]
        # Order is enforced (Trader, Investor, Global, Quality).
        assert [c.label for c in out.composites] == ["Trader", "Investor", "Global", "Quality"]
        assert [c.score for c in out.composites] == [70, 93, 77, 91]
        assert out.esg_msci == "BBB"

    def test_dash_esg_normalized_to_none(self):
        out = build_ratings({"esg_msci_rating": "-"})
        assert out.esg_msci is None

    def test_score_rounded_from_float(self):
        out = build_ratings({"composite_ratings": {"Trader": 70.6, "Investor": None,
                                                    "Global": 0, "Quality": 100}})
        scores = {c.label: c.score for c in out.composites}
        assert scores["Trader"] == 71      # rounded
        assert scores["Investor"] is None  # None passes through
        assert scores["Global"] == 0
        assert scores["Quality"] == 100

    def test_bullets_capped_at_five(self):
        out = build_ratings({"strengths": [f"strength {i}" for i in range(8)]})
        assert len(out.strengths) == 5
        assert out.strengths[0] == "strength 0"
        assert out.strengths[-1] == "strength 4"

    def test_blank_strings_dropped(self):
        out = build_ratings({"strengths": ["  real  ", "   ", "", None, "another"]})
        assert out.strengths == ["real", "another"]

    def test_has_data_false_when_only_dash_esg(self):
        """A ratings page that only has an ESG dash and no other data
        should not push the slide forward — it would show a dash bar
        across the board, which is worse than suppressing."""
        out = build_ratings({
            "strengths": [],
            "weaknesses": [],
            "composite_ratings": {"Trader": None, "Investor": None,
                                   "Global": None, "Quality": None},
            "esg_msci_rating": "-",
        })
        assert out.has_data is False


# ─────────────────────────────────────────────────────────────────────────────
# build_sector
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildSector:
    def test_none_input_yields_no_data(self):
        out = build_sector(None, None, sector_label="Retail")
        assert out.has_data is False
        assert out.rows == []
        assert out.sector_label == "Retail"

    def test_subject_marked_first(self):
        sector = {
            "rows": [
                {"name": "FOO CORP", "market_cap_usd": "1B",
                 "change_ytd_pct": -10.0, "change_1y_pct": -5.0, "change_3y_pct": None},
                {"name": "BAR INC.", "market_cap_usd": "5B",
                 "change_ytd_pct": 5.0, "change_1y_pct": 8.0, "change_3y_pct": 12.0},
            ],
            "summary_rows": {"average": {"change_ytd_pct": -2.5}},
        }
        out = build_sector(sector, None)
        assert out.has_data is True
        assert len(out.rows) == 2
        assert out.rows[0].is_subject is True
        assert out.rows[1].is_subject is False
        assert out.average_ytd_pct == pytest.approx(-2.5)

    def test_esg_join_from_ratings_payload(self):
        sector = {"rows": [
            {"name": "FOO CORP", "market_cap_usd": "1B"},
            {"name": "BAR INC.", "market_cap_usd": "2B"},
        ]}
        ratings = {"peer_esg": [
            {"name": "FOO CORP", "esg_msci": "AA"},
            {"name": "BAR INC.", "esg_msci": "-"},  # dash → None
        ]}
        out = build_sector(sector, ratings)
        assert out.rows[0].esg_msci == "AA"
        assert out.rows[1].esg_msci is None

    def test_peer_table_capped_with_subject_preserved(self):
        rows = [{"name": f"COMPANY {i}", "market_cap_usd": "1B"} for i in range(20)]
        out = build_sector({"rows": rows}, None)
        assert len(out.rows) == 11  # _PEER_TABLE_LIMIT
        # Subject (row 0) is always present.
        assert out.rows[0].is_subject is True
        assert out.rows[0].name == "COMPANY 0"

    def test_duplicate_names_deduped(self):
        sector = {"rows": [
            {"name": "FOO", "market_cap_usd": "1B"},
            {"name": "FOO", "market_cap_usd": "2B"},  # duplicate (case-insensitive)
            {"name": "BAR", "market_cap_usd": "3B"},
        ]}
        out = build_sector(sector, None)
        names = [r.name for r in out.rows]
        assert names == ["FOO", "BAR"]


# ─────────────────────────────────────────────────────────────────────────────
# build_price_action
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildPriceAction:
    def test_none_inputs_yield_no_data(self):
        out = build_price_action(None, None)
        assert out.has_data is False

    def test_perf_grid_only(self):
        perf = {"performance": {
            "perf_1d_pct": -0.85, "perf_1w_pct": -3.33,
            "perf_mtd_pct": -1.69, "perf_1m_pct": -6.45,
            "perf_3m_pct": -26.58, "perf_6m_pct": -27.50,
            "perf_ytd_pct": -23.18,
        }}
        out = build_price_action(perf, None)
        assert out.has_data is True
        assert len(out.performance) == 7
        # Order matches the canonical layout (1D first).
        assert out.performance[0].label == "1 day"
        assert out.performance[0].value_pct == pytest.approx(-0.85)
        assert out.performance[-1].label == "YTD"
        assert out.performance[-1].value_pct == pytest.approx(-23.18)

    def test_course_extremes_with_partial_data(self):
        perf = {"course_extremes": {
            "range_ytd": {"low": 1.12, "high": 1.62},
            "range_1y":  {"low": 1.12, "high": 1.81},
            # range_1w / 1m / 3y / 5y missing → renderer shows them as empty
        }}
        out = build_price_action(perf, None)
        assert out.has_data is True
        ranges = {r.label: r for r in out.course_extremes}
        assert ranges["YTD"].low == pytest.approx(1.12)
        assert ranges["YTD"].high == pytest.approx(1.62)
        assert ranges["1 week"].low is None
        assert ranges["1 week"].high is None

    def test_broker_actions_capped(self):
        items = [{"date": f"day{i}", "headline": f"headline {i}", "source": "MT"}
                 for i in range(10)]
        recs = {"items": items, "covering_brokers": ["JPMorgan", "HSBC", "Citi"]}
        out = build_price_action(None, recs)
        assert out.has_data is True
        assert len(out.broker_actions) == 6  # _MAX_BROKER_ACTIONS
        assert out.broker_actions[0].headline == "headline 0"
        assert out.covering_brokers == ["JPMorgan", "HSBC", "Citi"]

    def test_only_brokers_no_perf(self):
        recs = {"items": [{"date": "Apr 1", "headline": "JP Morgan upgrades", "source": "MT"}]}
        out = build_price_action(None, recs)
        assert out.has_data is True  # broker actions alone is enough
        assert out.broker_actions[0].source == "MT"

    def test_invalid_items_skipped(self):
        recs = {"items": [
            {"date": "Apr 1", "headline": "Real action", "source": "MT"},
            "string-not-dict",
            {"date": "Apr 2", "headline": "Another"},
            None,
        ]}
        out = build_price_action(None, recs)
        assert len(out.broker_actions) == 2
        assert out.broker_actions[0].headline == "Real action"
        assert out.broker_actions[1].headline == "Another"
