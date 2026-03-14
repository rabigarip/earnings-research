"""
CLI entry point.

Usage:
    # Initialize database (run once, or after editing company_master.json)
    python -m src.main --init-db

    # Earnings preview — SABIC (industrial, non-bank)
    python -m src.main --ticker 2010.SR --mode preview --skip-llm

    # Earnings preview — Al Rajhi Bank (bank, EBITDA skipped)
    python -m src.main --ticker 1120.SR --mode preview --skip-llm

    # Test failure path — invalid ticker
    python -m src.main --ticker ZZZZ.SR --mode preview

    # Test failure path — valid ticker, not in company master
    python -m src.main --ticker 2350.SR --mode preview

    # With Gemini news summarization (needs GEMINI_API_KEY in .env)
    python -m src.main --ticker 2010.SR --mode preview

Flags:
    --ticker     Yahoo-format ticker symbol (required for preview)
    --mode       "preview" (default) | "calendar"
    --skip-llm   Skip Gemini summarization step
    --init-db    Seed database and exit
"""

from __future__ import annotations
import argparse
import sys


def main() -> None:
    # Load .env if present (for GEMINI_API_KEY etc.)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    ap = argparse.ArgumentParser(
        description="Earnings Research Pipeline — Backend MVP",
    )
    ap.add_argument("--ticker",   type=str, help="Yahoo-format ticker")
    ap.add_argument("--mode",     type=str, default="preview",
                    choices=["preview", "calendar"])
    ap.add_argument("--skip-llm", action="store_true",
                    help="Skip Gemini summarization")
    ap.add_argument("--init-db",  action="store_true",
                    help="Initialize DB + seed companies, then exit")
    args = ap.parse_args()

    # ── DB init ───────────────────────────────────────────────
    from src.storage.db import init_db, seed_companies, _db_path

    if args.init_db:
        init_db()
        n = seed_companies()
        print(f"\nDone. {n} companies seeded. DB at {_db_path()}")
        sys.exit(0)

    # Auto-init on first run
    if not _db_path().exists():
        print("[auto] DB not found — initializing …")
        init_db()
        seed_companies()

    # ── Route ─────────────────────────────────────────────────
    if args.mode == "preview":
        if not args.ticker:
            ap.error("--ticker is required for preview mode")
        from src.pipeline import run_preview
        results = run_preview(args.ticker, skip_llm=args.skip_llm)

        from src.models.step_result import Status
        any_fail = any(r.status == Status.FAILED for r in results)
        sys.exit(1 if any_fail else 0)

    elif args.mode == "calendar":
        # TODO: list earnings dates for all seeded companies (yfinance calendar, sort, output JSON/table)
        print("[calendar] Not yet implemented.")
        sys.exit(0)


if __name__ == "__main__":
    main()
