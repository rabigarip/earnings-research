#!/usr/bin/env python3
"""Auto-extract FY grounding for one or more tickers into the staging dir.

This is the scale tool: point it at any ticker that has a MarketScreener
cache and it writes a CANDIDATE data/disclosed/_staging/<ticker>.json with
FY actuals on the ticker's sector schema. A human reviews each staging file
(checking the `_extractor.confidence`, `provenance`, and `needs_ir` fields)
and moves it to data/disclosed/ to promote it.

Usage:
  python3 scripts/extract_grounding.py 2010.SR EMAAR.AE OQEP.OM
  python3 scripts/extract_grounding.py --all-cached      # every ticker with MS cache
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.services.grounding_extractor import extract_grounding, write_staging  # noqa: E402


def _all_cached_tickers() -> list[str]:
    """Every ticker that has an income_statement snapshot, from both the live
    cache and the repo-tracked snapshot dir. Reverse-maps the filename grammar
    back to a TICKER (best-effort — short snapshot form only)."""
    from src.config import root
    import re
    seen = set()
    for d in (root() / "cache", root() / "data" / "marketscreener"):
        if not d.is_dir():
            continue
        for f in d.glob("ms_*_income_statement.html"):
            m = re.match(r"ms_([A-Z0-9]+_[A-Z]{2})_income_statement\.html$", f.name)
            if m:
                seen.add(m.group(1).replace("_", ".", 1) if "_" in m.group(1) else m.group(1))
    return sorted(seen)


def main() -> int:
    args = [a for a in sys.argv[1:] if a != "--all-cached"]
    if "--all-cached" in sys.argv:
        args = _all_cached_tickers()
        print(f"Found {len(args)} cached tickers")
    if not args:
        print(__doc__)
        return 1

    promotable = review = skipped = 0
    for ticker in args:
        res = extract_grounding(ticker)
        status = res.get("_status")
        if status != "auto_unverified":
            print(f"  {ticker:14} SKIP ({status})")
            skipped += 1
            continue
        ext = res["_extractor"]
        p = write_staging(ticker, res)
        conf = ext["confidence"]
        if conf == "high":
            promotable += 1
        else:
            review += 1
        print(f"  {ticker:14} {ext['anchor_period']:8} "
              f"{ext['metrics_emitted']} metrics  conf={conf:24} "
              f"-> {p.relative_to(p.parents[3])}")
        if ext["warnings"]:
            for w in ext["warnings"]:
                print(f"  {'':14} ⚠ {w}")

    print(f"\n{promotable} high-confidence, {review} need-review, "
          f"{skipped} skipped. Staging files in data/disclosed/_staging/. "
          f"Review then move to data/disclosed/ to promote.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
