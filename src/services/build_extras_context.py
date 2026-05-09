"""
Builders for the MS-extras slide-context dataclasses.

Lives separate from `build_report_context.py` so the existing 1,300-line
file stays readable and the new contract is easy to test in isolation.
Each builder takes the relevant section dict from `ReportPayload` and
returns the typed slide payload. None inputs map to empty/has_data=False
outputs — never to exceptions.
"""

from __future__ import annotations

from typing import Any, Optional

from src.models.report_context import (
    BrokerAction,
    CompositeRating,
    CourseRange,
    PeerRow,
    PerformanceCell,
    PriceActionData,
    RatingsData,
    SectorComparisonData,
)


# ─────────────────────────────────────────────────────────────────────────────
# Slide 4: Ratings & Sentiment
# ─────────────────────────────────────────────────────────────────────────────

# MS publishes the four composite ratings in this order on /ratings/. We
# render them in the same order so the slide visually matches the website.
_COMPOSITE_ORDER = ("Trader", "Investor", "Global", "Quality")

# Cap bullet counts: MS sometimes returns 7+ strengths and the slide can't
# fit more than five comfortably without shrinking the font. The trim is
# applied in the builder so renderers stay layout-only.
_MAX_BULLETS = 5


def build_ratings(ms_ratings: dict | None) -> RatingsData:
    """Build slide 4 payload from `payload.ms_ratings`.

    The dict shape comes from `marketscreener_pages.fetch_ratings_page`.
    `has_data` is True iff at least one bullet OR one composite score is
    populated; the deck builder uses this to decide whether to render the
    slide at all (suppress > render-empty).
    """
    if not ms_ratings or not isinstance(ms_ratings, dict):
        return RatingsData(has_data=False)

    raw_strengths = ms_ratings.get("strengths") or []
    raw_weaknesses = ms_ratings.get("weaknesses") or []
    composite_dict = ms_ratings.get("composite_ratings") or {}
    esg = ms_ratings.get("esg_msci_rating") or None

    strengths = [s.strip() for s in raw_strengths if isinstance(s, str) and s.strip()][:_MAX_BULLETS]
    weaknesses = [w.strip() for w in raw_weaknesses if isinstance(w, str) and w.strip()][:_MAX_BULLETS]

    composites: list[CompositeRating] = []
    for label in _COMPOSITE_ORDER:
        raw_score = composite_dict.get(label)
        score: Optional[int] = None
        if isinstance(raw_score, (int, float)) and not isinstance(raw_score, bool):
            try:
                score = int(round(float(raw_score)))
            except (TypeError, ValueError):
                score = None
        composites.append(CompositeRating(label=label, score=score))

    has_data = bool(strengths or weaknesses or any(c.score is not None for c in composites))
    # Normalize the ESG dash to None for cleaner downstream rendering.
    esg_clean = (esg or "").strip()
    if esg_clean in {"", "-", "N/A"}:
        esg_clean = None

    return RatingsData(
        strengths=strengths,
        weaknesses=weaknesses,
        composites=composites,
        esg_msci=esg_clean,
        has_data=has_data,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Slide 5: Sector Comparison
# ─────────────────────────────────────────────────────────────────────────────

_PEER_TABLE_LIMIT = 11  # subject + top 10 peers; PDF page 4 shows ~16 but
                       # vertical real estate caps us at ~12 readable rows


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _normalize_peer_name(name: str) -> str:
    """Lowercase + strip for cross-table matching (sector ↔ ratings ESG)."""
    return (name or "").strip().lower()


def build_sector(
    ms_sector_peers: dict | None,
    ms_ratings: dict | None,
    *,
    sector_label: str = "",
) -> SectorComparisonData:
    """Build slide 5 payload from `ms_sector_peers` joined with `ms_ratings.peer_esg`.

    The peer table on /sector/ has rich performance + market cap; the
    /ratings/ page peer table contributes the ESG MSCI letter for each
    peer. Names are normalized for the join. Missing ESG → None
    (renderer shows "—").
    """
    if not ms_sector_peers or not isinstance(ms_sector_peers, dict):
        return SectorComparisonData(has_data=False, sector_label=sector_label)

    raw_rows = ms_sector_peers.get("rows") or []
    summary_rows = ms_sector_peers.get("summary_rows") or {}

    # Build the peer-ESG lookup from the ratings page (when available).
    esg_by_name: dict[str, Optional[str]] = {}
    if ms_ratings and isinstance(ms_ratings, dict):
        for r in (ms_ratings.get("peer_esg") or []):
            if not isinstance(r, dict):
                continue
            name = _normalize_peer_name(r.get("name") or "")
            if not name:
                continue
            esg = (r.get("esg_msci") or "").strip()
            esg_by_name[name] = esg if esg and esg != "-" else None

    rows: list[PeerRow] = []
    seen_names: set[str] = set()
    for r in raw_rows:
        if not isinstance(r, dict):
            continue
        name = (r.get("name") or "").strip()
        if not name:
            continue
        norm = _normalize_peer_name(name)
        if norm in seen_names:
            continue
        seen_names.add(norm)
        rows.append(PeerRow(
            name=name,
            market_cap_usd=r.get("market_cap_usd") or None,
            change_ytd_pct=_to_float(r.get("change_ytd_pct")),
            change_1y_pct=_to_float(r.get("change_1y_pct")),
            change_3y_pct=_to_float(r.get("change_3y_pct")),
            esg_msci=esg_by_name.get(norm),
            is_subject=(r is raw_rows[0]),  # MS always lists the subject first
        ))

    # Trim to the visible window. Always keep the subject row; if it would
    # fall outside the cap (rare — MS sorts by mcap and the subject is
    # often a smallcap), surface it via insertion at index 0.
    if len(rows) > _PEER_TABLE_LIMIT:
        subject = next((r for r in rows if r.is_subject), None)
        rows = rows[:_PEER_TABLE_LIMIT]
        if subject and subject not in rows:
            rows = [subject] + rows[: _PEER_TABLE_LIMIT - 1]

    # MS publishes an "Average" summary row alongside the peer table; we
    # surface only its YTD figure on the slide footer for context.
    average_ytd: Optional[float] = None
    avg_row = (summary_rows or {}).get("average") if isinstance(summary_rows, dict) else None
    if isinstance(avg_row, dict):
        average_ytd = _to_float(avg_row.get("change_ytd_pct"))

    return SectorComparisonData(
        sector_label=sector_label.strip(),
        rows=rows,
        average_ytd_pct=average_ytd,
        has_data=bool(rows),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Slide 6: Price Action & Broker Activity
# ─────────────────────────────────────────────────────────────────────────────

# Order matches the PDF layout. The renderer respects this order; the
# fetcher emits a dict so we re-key explicitly here for stable output.
_PERFORMANCE_LABELS = (
    ("perf_1d_pct",  "1 day"),
    ("perf_1w_pct",  "1 week"),
    ("perf_mtd_pct", "MTD"),
    ("perf_1m_pct",  "1 month"),
    ("perf_3m_pct",  "3 months"),
    ("perf_6m_pct",  "6 months"),
    ("perf_ytd_pct", "YTD"),
)

_RANGE_LABELS = (
    ("range_1w",  "1 week"),
    ("range_1m",  "1 month"),
    ("range_ytd", "YTD"),
    ("range_1y",  "1 year"),
    ("range_3y",  "3 years"),
    ("range_5y",  "5 years"),
)

_MAX_BROKER_ACTIONS = 6  # most-recent slice fits the panel without scroll


def build_price_action(
    ms_price_performance: dict | None,
    ms_analyst_recommendations: dict | None,
) -> PriceActionData:
    """Build slide 6 payload from price-perf + analyst-recs sections.

    Either source alone is enough to render the slide. has_data is True
    when at least one panel has content (perf grid OR broker actions).
    """
    perf_cells: list[PerformanceCell] = []
    if isinstance(ms_price_performance, dict):
        perf_dict = ms_price_performance.get("performance") or {}
        for key, label in _PERFORMANCE_LABELS:
            perf_cells.append(PerformanceCell(label=label, value_pct=_to_float(perf_dict.get(key))))

    extremes: list[CourseRange] = []
    if isinstance(ms_price_performance, dict):
        ext_dict = ms_price_performance.get("course_extremes") or {}
        for key, label in _RANGE_LABELS:
            entry = ext_dict.get(key)
            if isinstance(entry, dict):
                extremes.append(CourseRange(
                    label=label,
                    low=_to_float(entry.get("low")),
                    high=_to_float(entry.get("high")),
                ))
            else:
                extremes.append(CourseRange(label=label))

    broker_actions: list[BrokerAction] = []
    covering_brokers: list[str] = []
    if isinstance(ms_analyst_recommendations, dict):
        for item in (ms_analyst_recommendations.get("items") or [])[:_MAX_BROKER_ACTIONS]:
            if not isinstance(item, dict):
                continue
            broker_actions.append(BrokerAction(
                date=(item.get("date") or "").strip(),
                headline=(item.get("headline") or "").strip(),
                source=(item.get("source") or "").strip(),
            ))
        for name in (ms_analyst_recommendations.get("covering_brokers") or []):
            if isinstance(name, str) and name.strip():
                covering_brokers.append(name.strip())

    has_perf = any(c.value_pct is not None for c in perf_cells)
    has_actions = bool(broker_actions)
    has_extremes = any(r.low is not None or r.high is not None for r in extremes)

    return PriceActionData(
        performance=perf_cells,
        course_extremes=extremes,
        broker_actions=broker_actions,
        covering_brokers=covering_brokers,
        has_data=has_perf or has_actions or has_extremes,
    )
