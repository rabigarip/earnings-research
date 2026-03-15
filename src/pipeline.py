"""Pipeline orchestrator for earnings preview mode.

Steps 1-2 are CRITICAL (halt on failure). Steps 3+ are RESILIENT (log and continue).
Outputs: one memo .docx + one QA .docx per ticker.
"""

from __future__ import annotations
import logging
import uuid
from copy import deepcopy
from datetime import datetime, timezone

from src.models.step_result import Status, StepResult
from src.services import (
    resolve_mapping, fetch_marketscreener_pages,
    summarize_news, build_report_payload, generate_report,
)
from src.services.pipeline_steps import (
    validate_ticker, fetch_quote, fetch_financials,
    fetch_consensus, fetch_news, reconcile, qa_validate,
)
from src.services.build_report_payload import get_memo_computed_for_preview
from src.services.ms_payload_fingerprint import save_fingerprint as save_ms_fingerprint
from src.storage.db import save_run

logger = logging.getLogger(__name__)


def run_preview(ticker: str, *, skip_llm: bool = False) -> list[StepResult]:
    run_id = uuid.uuid4().hex[:8]
    t0 = datetime.now(timezone.utc)
    results: list[StepResult] = []

    _banner(ticker, run_id, t0)

    # ── 1. Validate ticker (CRITICAL) ─────────────────────────
    r = validate_ticker(ticker)
    _collect(r, results)
    if r.status == Status.FAILED:
        _finish(run_id, ticker, t0, results)
        return results

    # ── 2. Resolve company mapping (CRITICAL) ─────────────────
    r = resolve_mapping.run(ticker)
    _collect(r, results)
    if r.status == Status.FAILED:
        _finish(run_id, ticker, t0, results)
        return results
    company = r.data

    # ── 3. Fetch quote ────────────────────────────────────────
    r = fetch_quote(ticker)
    _collect(r, results)
    quote = r.data if r.status != Status.FAILED else None

    # ── 4. Fetch financials ───────────────────────────────────
    r = fetch_financials(ticker, company)
    _collect(r, results)
    quarterly = r.data.get("quarterly", []) if r.data else []
    annual    = r.data.get("annual", [])    if r.data else []

    # ── 5. Fetch consensus ────────────────────────────────────
    r = fetch_consensus(ticker, company)
    _collect(r, results)
    consensus = r.data if isinstance(r.data, list) else []

    # ── 5b. Fetch MarketScreener pages ────────────────────────
    r = fetch_marketscreener_pages.run(ticker, company)
    _collect(r, results)
    ms_blocks = deepcopy(r.data) if isinstance(r.data, dict) else {}

    # ── 6. Fetch news ─────────────────────────────────────────
    r = fetch_news(ticker, company)
    _collect(r, results)
    news_data = r.data if isinstance(r.data, dict) else {}
    news_items = news_data.get("items") or (r.data if isinstance(r.data, list) else [])

    # ── 7. Reconcile + derived metrics ────────────────────────
    r = reconcile(ticker, company, quarterly, consensus)
    _collect(r, results)
    derived = r.data if r.status != Status.FAILED else None

    # ── 8. Summarize news (LLM) ──────────────────────────────
    memo_fact_pack = None
    if not skip_llm:
        try:
            memo_fact_pack = get_memo_computed_for_preview(
                company=company, quote=quote, quarterly=quarterly,
                consensus=consensus,
                consensus_summary=ms_blocks.get("consensus_summary"),
                ms_annual_forecasts=ms_blocks.get("ms_annual_forecasts"),
                ms_quarterly_forecasts=ms_blocks.get("ms_quarterly_forecasts"),
                ms_eps_dividend_forecasts=ms_blocks.get("ms_eps_dividend_forecasts"),
                ms_calendar_events=ms_blocks.get("ms_calendar_events"),
                derived=derived,
            )
        except Exception:
            pass
    if skip_llm:
        r = StepResult(
            step_name="summarize_news", status=Status.SKIPPED,
            source="gemini", message="Skipped (--skip-llm)",
        )
    else:
        r = summarize_news.run(news_items, company.company_name, memo_fact_pack=memo_fact_pack)
    _collect(r, results)
    summary = r.data

    # ── 9. Build report payload ───────────────────────────────
    r = build_report_payload.run(
        run_id=run_id, company=company, quote=quote,
        quarterly=quarterly, annual=annual, consensus=consensus,
        consensus_summary=ms_blocks.get("consensus_summary"),
        ms_lineage=ms_blocks.get("ms_lineage"),
        ms_summary=ms_blocks.get("ms_summary"),
        ms_annual_forecasts=ms_blocks.get("ms_annual_forecasts"),
        ms_quarterly_forecasts=ms_blocks.get("ms_quarterly_forecasts"),
        ms_eps_dividend_forecasts=ms_blocks.get("ms_eps_dividend_forecasts"),
        ms_income_statement_actuals=ms_blocks.get("ms_income_statement_actuals"),
        ms_valuation_multiples=ms_blocks.get("ms_valuation_multiples"),
        ms_calendar_events=ms_blocks.get("ms_calendar_events"),
        ms_quarterly_results_table=ms_blocks.get("ms_quarterly_results_table"),
        derived=derived, news_items=news_items, news_summary=summary,
        duplicate_screening_log=news_data.get("duplicate_screening_log") or [],
        step_log=[s.to_log_dict() for s in results],
        recent_context_query_log=news_data.get("recent_context_query_log") or [],
        recent_context_candidate_count=news_data.get("recent_context_candidate_count") or 0,
        recent_context_valid_count=news_data.get("recent_context_valid_count") or 0,
        recent_context_rejected_reasons=news_data.get("recent_context_rejected_reasons") or [],
        candidate_valid_basic=news_data.get("candidate_valid_basic", False),
        candidate_has_date_before_enrichment=news_data.get("candidate_has_date_before_enrichment", 0),
        candidate_has_extracted_fact=news_data.get("candidate_has_extracted_fact", 0),
        final_article_valid_count=news_data.get("final_article_valid_count", 0),
        date_parse_attempted=news_data.get("date_parse_attempted", 0),
        date_parse_source=news_data.get("date_parse_source") or [],
        date_parse_success=news_data.get("date_parse_success", 0),
        candidates_rejected_for_missing_date=news_data.get("candidates_rejected_for_missing_date", 0),
        candidates_recovered_after_article_fetch=news_data.get("candidates_recovered_after_article_fetch", 0),
        recent_context_enrichment_log=news_data.get("recent_context_enrichment_log") or [],
        rejected_candidates_top_10=news_data.get("rejected_candidates_top_10") or [],
        recent_context_articles_qa=news_data.get("recent_context_articles_qa") or [],
    )
    _collect(r, results)
    if r.status == Status.FAILED:
        _finish(run_id, ticker, t0, results)
        return results
    payload = r.data

    # Persist MS fingerprint for cross-company contamination checks
    try:
        fp = getattr(payload, "ms_payload_fingerprint", "") or ""
        if fp and not getattr(payload, "cross_company_contamination_detected", True):
            save_ms_fingerprint(ticker, run_id, fp)
    except Exception as exc:
        logger.warning("Could not save MS fingerprint: %s", exc)

    # ── 10. QA validate ───────────────────────────────────────
    r = qa_validate(payload)
    _collect(r, results)
    memo_data = None
    qa_audit = None
    if r.status == Status.SUCCESS and isinstance(r.data, dict):
        memo_data = r.data.get("memo_data")
        qa_audit = r.data.get("qa_audit")

    # ── 11. Generate report (.docx + QA .docx) ───────────────
    r = generate_report.run(payload, memo_data=memo_data, qa_audit=qa_audit)
    _collect(r, results)

    _finish(run_id, ticker, t0, results)
    return results


# ── Helpers ───────────────────────────────────────────────────

def _collect(r: StepResult, results: list[StepResult]) -> None:
    r.print_box()
    results.append(r)


def _overall(results: list[StepResult]) -> str:
    statuses = {r.status for r in results}
    if Status.FAILED in statuses or Status.PARTIAL in statuses:
        return "partial"
    return "success"


def _banner(ticker: str, run_id: str, t0: datetime) -> None:
    print(f"\n{'█' * 66}")
    print(f"  EARNINGS PREVIEW PIPELINE")
    print(f"  Ticker:   {ticker}")
    print(f"  Run ID:   {run_id}")
    print(f"  Started:  {t0:%Y-%m-%d %H:%M:%S} UTC")
    print(f"{'█' * 66}")


def _finish(run_id: str, ticker: str, t0: datetime,
            results: list[StepResult]) -> None:
    from pathlib import Path
    t1 = datetime.now(timezone.utc)
    overall = _overall(results)
    failed = sum(1 for r in results if r.status == Status.FAILED)
    elapsed = (t1 - t0).total_seconds()

    memo_path = None
    for r in results:
        if r.step_name == "generate_report" and r.status == Status.SUCCESS and r.data:
            memo_path = Path(str(r.data)).name
            break

    try:
        save_run(run_id, ticker, "preview",
                 t0.isoformat(), t1.isoformat(),
                 overall, [r.to_log_dict() for r in results], memo_path=memo_path)
    except Exception as exc:
        logger.warning("Could not save run to DB: %s", exc)

    print(f"\n{'█' * 66}")
    print(f"  PIPELINE COMPLETE — {overall.upper()}")
    print(f"  Steps: {len(results)}  |  Failed: {failed}  |  {elapsed:.1f}s total")
    print(f"{'█' * 66}\n")
