"""
Service: resolve_mapping

Looks up the ticker (yfinance ticker) in the identifier store (company_master).
Canonical flow: ticker → exact ISIN → MarketScreener entity (by ISIN); company name
is for validation/fallback only. Mappings are curated; missing tickers get a clear
instruction to add to data/company_master.json and re-seed.
"""

from __future__ import annotations
from src.models.company import CompanyMaster
from src.models.step_result import Status, StepResult, StepTimer
from src.storage.db import load_company
from src.services.entity_resolution import ensure_marketscreener_cached

STEP = "resolve_mapping"


def run(ticker: str) -> StepResult:
    with StepTimer() as t:
        row = load_company(ticker)

    if row is None:
        return StepResult(
            step_name=STEP, status=Status.FAILED, source="local",
            message=(
                f"No mapping for '{ticker}'. "
                f"Add it to data/company_master.json then run: "
                f"python -m src.main --init-db"
            ),
            elapsed_seconds=t.elapsed,
        )

    # Ensure MarketScreener URL cached (resolve by ISIN if missing/stale)
    updated = ensure_marketscreener_cached(ticker, company=row)
    if updated is not None:
        row = updated
    company = CompanyMaster(**row)

    gaps = []
    if not company.isin:
        gaps.append("ISIN")
    # MS: consider filled if we have a valid cached URL (ISIN-based), not just slug
    has_valid_ms = (
        (company.marketscreener_company_url or "").strip() and
        (company.marketscreener_status or "").strip().lower() == "ok"
    )
    if not has_valid_ms and not company.marketscreener_id:
        gaps.append("MarketScreener ID")
    if not company.zawya_slug:
        gaps.append("Zawya slug")

    status = Status.SUCCESS if not gaps else Status.PARTIAL
    gap_msg = f" — missing: {', '.join(gaps)}" if gaps else ""

    return StepResult(
        step_name=STEP, status=status, source="local",
        message=(
            f"Mapped: {company.company_name} | "
            f"ISIN={company.isin or '—'} | "
            f"bank={company.is_bank}{gap_msg}"
        ),
        data=company, elapsed_seconds=t.elapsed,
    )
