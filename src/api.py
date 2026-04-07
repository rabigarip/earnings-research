"""
FastAPI web app for Render + frontend.

Endpoints:
  GET  /health              — Health check (Render)
  GET  /api/companies       — List companies (ticker, company_name, exchange, country)
  GET  /api/tickers/search  — Search companies by ticker or name (?q=)
  GET  /api/reports         — List pipeline runs (for earnings-preview frontend)
  POST /api/reports         — Create run (run preview), returns report row + payload
  GET  /api/reports/:id     — Get one run (steps, no payload)
  GET  /api/reports/:id/download — Download preview .pptx file
  POST /api/reports/:id/rerun — Rerun preview for that ticker
  POST /api/preview         — Run earnings preview; returns step results + report payload
"""

from __future__ import annotations
import os
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# When frontend is built into static/, we serve it at / (one site on Render)
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
STATIC_INDEX = STATIC_DIR / "index.html"
SERVE_FRONTEND = STATIC_INDEX.exists()

# Load env before importing pipeline (needs GEMINI_API_KEY etc.)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _ensure_db() -> None:
    """Create DB if missing, run migrations, then always merge company_master.json (Render + local).

    Without re-seed, production SQLite keeps stale rows when git adds marketscreener_id slugs.
    seed_companies uses ON CONFLICT DO UPDATE — safe on every process start (~500 upserts).
    """
    from src.storage.db import init_db, seed_companies, ensure_migrations, _db_path
    if not _db_path().exists():
        init_db()
    else:
        ensure_migrations()
    seed_companies()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _ensure_db()
    yield


app = FastAPI(
    title="Earnings Research API",
    description="Backend for earnings preview pipeline; use with a frontend.",
    lifespan=lifespan,
)

# Allow frontend from any origin when developing; restrict in production if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# ─── Request/response models ─────────────────────────────────────────────

class PreviewRequest(BaseModel):
    ticker: str = Field(..., description="Yahoo-format ticker, e.g. 2010.SR")
    skip_llm: bool = Field(True, description="Skip Gemini news summarization (faster, no API key)")


class PreviewResponse(BaseModel):
    run_id: str
    overall: str  # success | partial
    steps: list[dict]
    payload: dict | None = None  # Full report payload when build_report_payload succeeded


class BatchPreviewRequest(BaseModel):
    tickers: list[str] = Field(..., description="List of Yahoo-format tickers (e.g. ['2010.SR','1120.SR'])")
    skip_llm: bool = Field(True, description="Skip Gemini summarization for faster batch runs")


# ─── Routes ───────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    try:
        from src.config import cfg
        v = cfg().get("general", {}).get("version", "0.1.0")
        return {"status": "ok", "version": v}
    except Exception:
        return {"status": "ok"}


class LoginRequest(BaseModel):
    accessCode: str = ""


@app.post("/api/auth/login")
def auth_login(req: LoginRequest):
    """Stub: accept any access code so the frontend login works (one-site mode). Replace with real auth if needed."""
    return {"token": "ok", "message": "ok"}


@app.get("/")
def root():
    if SERVE_FRONTEND:
        return FileResponse(STATIC_INDEX, media_type="text/html")
    return {"service": "earnings-research-api", "docs": "/docs"}


@app.get("/api/companies")
def list_companies():
    from src.storage.db import list_companies as _list
    return _list()


@app.get("/api/tickers/search")
def search_tickers(q: str = ""):
    """Search companies by ticker or company name. Returns { results: [{ ticker, company, country }] }."""
    from src.storage.db import list_companies as _list
    companies = _list()
    query = (q or "").strip().lower()
    if not query:
        return {"results": [{"ticker": c["ticker"], "company": c["company_name"], "country": c["country"]} for c in companies[:100]]}
    out = []
    for c in companies:
        if query in (c.get("ticker") or "").lower() or query in (c.get("company_name") or "").lower():
            out.append({"ticker": c["ticker"], "company": c["company_name"], "country": c["country"]})
    return {"results": out[:50]}


def _run_to_report_row(run: dict) -> dict:
    """Map DB run to frontend report row: id, ticker, company, country, status, created, warnings."""
    status = (run.get("overall_status") or "").lower()
    if status in ("success", "partial"):
        frontend_status = "COMPLETED"
    elif status == "failed":
        frontend_status = "FAILED"
    else:
        frontend_status = "COMPLETED" if status else "FAILED"
    return {
        "id": run["run_id"],
        "ticker": run["ticker"],
        "company": run.get("company_name") or run["ticker"],
        "country": run.get("country") or "",
        "status": frontend_status,
        "created": run.get("started_at") or "",
        "warnings": run.get("warnings", 0),
    }


@app.get("/api/reports")
def list_reports():
    """List pipeline runs for frontend reports table."""
    from src.storage.db import list_runs
    runs = list_runs()
    return {"reports": [_run_to_report_row(r) for r in runs]}


@app.get("/api/reports/{run_id}")
def get_report(run_id: str):
    """Get one run (steps, no payload)."""
    from src.storage.db import load_run
    run = load_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Report not found")
    row = _run_to_report_row(run)
    row["steps"] = run.get("step_results", [])
    return row


@app.get("/api/reports/{run_id}/download")
def download_report(run_id: str):
    """Download the earnings preview file for this run (.pptx). Returns 404 if file not found (e.g. after Render redeploy)."""
    from src.storage.db import load_run
    from src.config import root, cfg, report_output_dir
    run = load_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Report not found")
    memo_filename = (run.get("memo_path") or "").strip()
    if not memo_filename or ".." in memo_filename:
        raise HTTPException(status_code=404, detail="No report file for this run")
    out_dir = report_output_dir()
    file_path = out_dir / memo_filename
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="Report file no longer available (may have been removed)")
    media = (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        if memo_filename.lower().endswith(".pptx")
        else "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
    response = FileResponse(path=str(file_path), media_type=media, filename=memo_filename)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def _readiness_error_detail(results: list) -> dict | None:
    """If report_readiness failed, return a structured error for HTTP 422."""
    from src.models.step_result import Status

    for r in results:
        if r.step_name == "report_readiness" and r.status == Status.FAILED:
            d = r.data if isinstance(r.data, dict) else {}
            return {
                "error": "report_not_ready",
                "summary": d.get("summary") or (r.error_detail or "") or r.message,
                "reasons": d.get("reasons", []),
                "step_failures": d.get("step_failures", []),
            }
    return None


def _run_preview_and_response(ticker: str, skip_llm: bool = True, *, raise_on_readiness: bool = True) -> dict:
    """Run pipeline and return frontend-shaped response: report row + payload + steps."""
    from src.pipeline import run_preview
    from src.models.step_result import Status
    from src.storage.db import load_company, load_run

    run_id, results = run_preview(ticker, skip_llm=skip_llm)
    err = _readiness_error_detail(results)
    if err and raise_on_readiness:
        raise HTTPException(status_code=422, detail=err)
    statuses = {r.status for r in results}
    overall = "partial" if (Status.FAILED in statuses or Status.PARTIAL in statuses) else "success"
    # run_id comes directly from pipeline (no fragile extraction needed)
    payload = None
    for r in results:
        if r.step_name == "build_report_payload" and r.data is not None and r.status != Status.FAILED:
            try:
                payload = r.data.model_dump(mode="json")
            except Exception:
                payload = None

    steps = [r.to_log_dict() for r in results]

    # Do not return 200 if the client expects a file: generate_report failed → download would 404.
    if raise_on_readiness:
        gen = next((s for s in steps if s.get("step_name") == "generate_report"), None)
        if gen and gen.get("status") == "failed":
            reasons = []
            if gen.get("error_detail"):
                reasons.append(str(gen["error_detail"]))
            if gen.get("message") and gen.get("message") not in reasons:
                reasons.append(str(gen["message"]))
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "pptx_generation_failed",
                    "summary": gen.get("message") or "PPTX file was not created",
                    "reasons": reasons or ["Report generation step failed"],
                    "step_failures": [],
                },
            )

    run = load_run(run_id) if run_id else None
    company = load_company(ticker) if ticker else None
    row = _run_to_report_row(run) if run else {
        "id": run_id,
        "ticker": ticker,
        "company": (company or {}).get("company_name") or ticker,
        "country": (company or {}).get("country") or "",
        "status": "COMPLETED" if overall in ("success", "partial") else "FAILED",
        "created": "",
        "warnings": sum(1 for s in steps if s.get("status") in ("partial", "failed")),
    }
    if run:
        row["created"] = run.get("started_at") or row["created"]
    out: dict = {"report": row, "payload": payload, "steps": steps}
    if not raise_on_readiness:
        out["readiness_error"] = err
    return out


class CreateReportRequest(BaseModel):
    ticker: str = Field(..., description="Yahoo-format ticker, e.g. 2010.SR")
    skip_llm: bool = Field(True, description="Skip Gemini news summarization")


import re as _re

_TICKER_RE = _re.compile(r"^[A-Z0-9\.\-]{1,20}$")


@app.post("/api/reports")
def create_report(req: CreateReportRequest):
    """Create a report synchronously. Returns report row + payload + steps."""
    ticker = (req.ticker or "").strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker format")
    try:
        data = _run_preview_and_response(ticker, skip_llm=req.skip_llm)
        return data
    except HTTPException:
        raise
    except Exception as e:
        msg = str(e).strip() or repr(e)
        raise HTTPException(status_code=500, detail=f"Report creation failed: {msg}")


@app.post("/api/reports/{run_id}/rerun")
def rerun_report(run_id: str):
    """Rerun preview for the same ticker. Returns report row + payload + steps."""
    from src.storage.db import load_run
    run = load_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Report not found")
    ticker = run.get("ticker", "").strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker not found for run")
    try:
        return _run_preview_and_response(ticker, skip_llm=True)
    except HTTPException:
        raise
    except Exception as e:
        msg = str(e).strip() or repr(e)
        raise HTTPException(status_code=500, detail=f"Rerun failed: {msg}")


@app.post("/api/preview", response_model=PreviewResponse)
def run_preview_api(req: PreviewRequest):
    from src.pipeline import run_preview
    from src.models.step_result import Status

    ticker = (req.ticker or "").strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")

    run_id, results = run_preview(ticker, skip_llm=req.skip_llm)
    err = _readiness_error_detail(results)
    if err:
        raise HTTPException(status_code=422, detail=err)

    statuses = {r.status for r in results}
    overall = "partial" if (Status.FAILED in statuses or Status.PARTIAL in statuses) else "success"
    payload = None
    for r in results:
        if r.step_name == "build_report_payload" and r.status != Status.FAILED and r.data is not None:
            try:
                payload = r.data.model_dump(mode="json")
            except Exception:
                payload = None

    steps = [r.to_log_dict() for r in results]
    return PreviewResponse(run_id=run_id, overall=overall, steps=steps, payload=payload)


@app.post("/api/batch")
def batch_preview_api(req: BatchPreviewRequest):
    """Run previews for multiple tickers (PPTX output per ticker). Max 20."""
    tickers = [t.strip().upper() for t in (req.tickers or []) if (t or "").strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="tickers is required")
    if len(tickers) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 tickers per batch")

    results = []
    for t in tickers:
        results.append(_run_preview_and_response(t, skip_llm=req.skip_llm, raise_on_readiness=False))
    return {"results": results}


# ─── One site: serve frontend from static/ when present ─────────────────────

if SERVE_FRONTEND:
    assets_dir = STATIC_DIR / "assets"
    if assets_dir.is_dir():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/{path:path}")
    def serve_spa(path: str):
        """SPA fallback: serve index.html for non-API routes (e.g. /reports/123)."""
        if path.startswith("api/") or path == "api":
            raise HTTPException(status_code=404, detail="Not found")
        if path.startswith("docs") or path.startswith("openapi") or path == "health":
            raise HTTPException(status_code=404, detail="Not found")
        return FileResponse(STATIC_INDEX, media_type="text/html")
