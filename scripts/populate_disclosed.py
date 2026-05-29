"""Auto-populate data/disclosed/{ticker}.json from a ticker's IR portal.

Usage:
    python scripts/populate_disclosed.py BKMB.OM

Reads the ticker's `ir_portal_url` from data/tickers.json; downloads
the most recent N quarterly PDFs from the IR portal; runs the generic
IFRS interim-statement parser; writes the JSON shape that
`disclosed_loader.py` already consumes.

Per-ticker URL pattern discovery is the only piece NOT yet generic.
We carry a small KNOWN_IR_PATTERNS table here for tickers we've
verified; new tickers need one row added (typically 5 minutes per
ticker, then automated forever). When the pattern is unknown, the
script prints a TODO message so the analyst knows what's missing.

This is Phase 2 of the disclosed-source pipeline. Phase 3 will
subscribe to exchange disclosure feeds (Tadawul, MSX, ADX, DFM, QSE)
for automatic URL discovery; Phase 4 will add LLM fallback for the
~5% of layouts the regex parser can't handle.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import urllib.request
from datetime import date
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


ROOT = Path(__file__).resolve().parents[1]
DISCLOSED_DIR = ROOT / "data" / "disclosed"
DOWNLOAD_DIR = ROOT / "data" / "_ir_pdfs"   # cache dir for downloaded PDFs


# Per-ticker URL pattern. Each entry knows how to produce the URL for a
# specific quarterly period. Add new tickers as you encounter them.
#
# Function signature: (yyyy: int, qq: int) -> Optional[str]
#   yyyy   — fiscal year
#   qq     — quarter (1, 2, 3, 4)
#   returns the URL or None when the IR portal hasn't published that
#   period yet (e.g. FY25 reports out in early March).

def _bkmb_url(yyyy: int, qq: int) -> Optional[str]:
    """Bank Muscat: MSM_<MMYY>.pdf where MM is the period-end month
    (03 / 06 / 09 / 12) and YY is the 2-digit fiscal year."""
    month_for_q = {1: "03", 2: "06", 3: "09", 4: "12"}.get(qq)
    if not month_for_q: return None
    yy = str(yyyy)[-2:]
    return f"https://www.bankmuscat.om/en/investorrelations/QuarterlyReports/MSM_{month_for_q}{yy}.pdf"


KNOWN_IR_PATTERNS: dict[str, callable] = {
    "BKMB.OM": _bkmb_url,
    # Add more tickers here as their IR portals are discovered.
    # Each pattern is one small function — keeps onboarding ~5 min.
}


def _download_pdf(url: str, dest: Path) -> bool:
    """Stream the URL to `dest`. Skips if already cached. Returns True
    on success; False on network errors or 404s."""
    if dest.is_file() and dest.stat().st_size > 1024:
        return True
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; JabalResearch/1.0)",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read()
        if len(data) < 1024:   # FY25 reports often 302 to an HTML page
            log.warning("%s returned %d bytes — likely not a PDF yet", url, len(data))
            return False
        # Quick PDF sanity check (magic header).
        if not data.startswith(b"%PDF"):
            log.warning("%s is not a PDF (header=%r)", url, data[:8])
            return False
        dest.write_bytes(data)
        log.info("Downloaded %d KB → %s", len(data) // 1024, dest.name)
        return True
    except Exception as exc:
        log.warning("Download failed for %s: %s", url, exc)
        return False


def populate_ticker(ticker: str, n_recent_quarters: int = 6) -> int:
    """Populate data/disclosed/{ticker}.json with extracted quarters.

    Walks BACKWARD from the current quarter, attempting up to
    `n_recent_quarters` periods. Stops cleanly when downloads fail
    (e.g. an unreleased FY report).
    """
    pattern = KNOWN_IR_PATTERNS.get(ticker)
    if not pattern:
        log.error("No URL pattern known for %s. Add a function to "
                  "KNOWN_IR_PATTERNS in this script and retry.", ticker)
        return 1

    # Load ticker registry for company name + IR URL.
    reg_path = ROOT / "data" / "tickers.json"
    try:
        recs = json.loads(reg_path.read_text())
        reg = {r["ticker"]: r for r in recs if "ticker" in r}
    except (OSError, json.JSONDecodeError):
        reg = {}
    info = reg.get(ticker, {})

    # Build the list of (year, quarter) tuples newest-first to attempt.
    today = date.today()
    cur_q = (today.month - 1) // 3 + 1
    cur_y = today.year
    attempts: list[tuple[int, int]] = []
    yy, qq = cur_y, cur_q
    for _ in range(n_recent_quarters):
        attempts.append((yy, qq))
        qq -= 1
        if qq == 0:
            qq = 4; yy -= 1

    from src.services.pdf_interim_parser import (
        extract_interim_quarter, to_disclosed_quarterly_record,
    )

    quarterly: list[dict] = []
    source_docs: dict[str, str] = {}
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    for yyyy, qq in attempts:
        url = pattern(yyyy, qq)
        if not url: continue
        local_name = f"{ticker.replace('.', '_')}_{yyyy}Q{qq}.pdf"
        local_path = DOWNLOAD_DIR / local_name
        if not _download_pdf(url, local_path):
            continue
        ext = extract_interim_quarter(local_path)
        if not ext:
            log.warning("Parser returned nothing for %s", local_path.name)
            continue
        if ext.extraction_confidence == "low":
            log.warning("Low-confidence extraction for %s — review before trust",
                        local_path.name)
        rec = to_disclosed_quarterly_record(ext, Path(url).name)
        quarterly.append(rec)
        source_docs[ext.period] = url

    if not quarterly:
        log.error("No quarters extracted for %s", ticker)
        return 2

    # Compose the output JSON. Schema matches what disclosed_loader expects.
    out = {
        "ticker": ticker,
        "company": info.get("company_name", ""),
        "currency": info.get("currency", ""),
        "units": "thousands",   # IFRS interim PDFs publish in 'RO 000 / SAR M 000 etc.
        "_comment": (
            f"Auto-extracted by scripts/populate_disclosed.py from the "
            f"company IR portal. Re-run quarterly when new reports drop."
        ),
        "_source_documents": source_docs,
        "quarterly": quarterly,
    }
    DISCLOSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DISCLOSED_DIR / f"{ticker}.json"
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    log.info("Wrote %s (%d quarters)", out_path, len(quarterly))
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ticker", help="Ticker to populate (e.g. BKMB.OM)")
    ap.add_argument("--quarters", type=int, default=6,
                    help="How many recent quarters to attempt (default 6)")
    args = ap.parse_args()
    return populate_ticker(args.ticker, args.quarters)


if __name__ == "__main__":
    sys.exit(main())
