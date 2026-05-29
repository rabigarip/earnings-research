"""Read-side adapter for data/tickers.json.

One process-lifetime cache (the file is ~300KB and static for the run).
Returns a typed dict per ticker; callers downstream import:

    from src.services.ticker_registry import get_ticker_info

The registry feeds:
  - build_report_payload.run: template_family + currency_unit_scale
  - render_jabal_*: peer_set defaults + is_bank flag derivation
  - render_provenance: BR/SIC routing for fundamentals attribution
  - LLM context: sector classification

When the registry doesn't carry a ticker (e.g. one-off analyst ticker
not in the 500-name universe), `get_ticker_info` returns a sensible
default record so downstream code never crash-loops on a missing key.
"""
from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_REGISTRY_PATH = Path(__file__).resolve().parents[2] / "data" / "tickers.json"


@lru_cache(maxsize=1)
def _registry_index() -> dict[str, dict]:
    """Load the full registry once, return a ticker→record index."""
    if not _REGISTRY_PATH.is_file():
        log.warning("Ticker registry missing at %s; downstream will use defaults",
                    _REGISTRY_PATH)
        return {}
    try:
        recs = json.loads(_REGISTRY_PATH.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("Ticker registry parse failed: %s", exc)
        return {}
    return {r["ticker"]: r for r in recs if "ticker" in r}


def get_ticker_info(ticker: str) -> dict[str, Any]:
    """Return the registry record for `ticker`, or a sensible default.

    Default record covers the keys downstream code reads — currency
    unit scale 1, template family 'other', empty peer set — so a
    missing ticker produces a usable but bare deck rather than a
    KeyError trace.
    """
    idx = _registry_index()
    if ticker in idx:
        return idx[ticker]
    return {
        "ticker": ticker,
        "company_name": ticker,
        "exchange": "",
        "exchange_country": "",
        "currency": "",
        "currency_unit_scale": 1,
        "reporting_currency": "",
        "sector": "Other",
        "industry": "Other",
        "template_family": "other",
        "market_cap_local": None,
        "market_cap_usd": None,
        "is_canonical": True,
        "company_group": "",
        "siblings": [],
        "is_depositary_receipt": False,
        "underlying_ticker": None,
        "dr_fundamentals_source": None,
        "peer_set": [],
        "providers": {
            "yfinance": "supported", "marketscreener": "supported",
            "investing": "supported", "bloomberg_ticker": None,
        },
        "ir_portal_url": None,
        "disclosure_feed": None,
        "fiscal_year_end_month": 12,
        "active": True,
        "notes": "",
    }


def is_bank(ticker: str) -> bool:
    """Convenience derivation — true when template_family == 'bank'.
    The pipeline historically uses `is_bank` for table-schema dispatch;
    keep that contract but compute from the registry."""
    return get_ticker_info(ticker).get("template_family") == "bank"


def registry_peer_set(ticker: str) -> list[str]:
    """Convenience for callers that want the default peer list."""
    return list(get_ticker_info(ticker).get("peer_set") or [])


def reset_cache() -> None:
    """Used by tests to force a re-read after fixture edits."""
    _registry_index.cache_clear()
