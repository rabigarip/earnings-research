"""
FastAPI web app for Render + frontend.

Endpoints:
  GET  /health              — Health check (Render)
  GET  /api/companies       — List companies (ticker, company_name, exchange, country)
  GET  /api/tickers/search  — Search companies by ticker or name (?q=)
  GET  /api/reports         — List pipeline runs (for earnings-preview frontend)
  POST /api/reports         — Create run (run preview), returns report row + payload
  GET  /api/reports/:id     — Get one run (steps, no payload)
  GET  /api/reports/:id/download — Download memo .docx file
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
    from src.storage.db import init_db, seed_companies, ensure_migrations, _db_path
    if not _db_path().exists():
        init_db()
        seed_companies()
    else:
        ensure_migrations()


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


# ─── Routes ───────────────────────────────────────────────────────────────

@app.get("/health")
def health():
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
    """Download the memo .docx file for this run. Returns 404 if file not found (e.g. after Render redeploy)."""
    from src.storage.db import load_run
    from src.config import root, cfg, report_output_dir
    run = load_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Report not found")
    memo_filename = (run.get("memo_path") or "").strip()
    if not memo_filename or ".." in memo_filename:
        raise HTTPException(status_code=404, detail="No memo file for this run")
    out_dir = report_output_dir()
    file_path = out_dir / memo_filename
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="Memo file no longer available (may have been removed)")
    return FileResponse(
        path=str(file_path),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename=memo_filename,
    )


def _run_preview_and_response(ticker: str, skip_llm: bool = True) -> dict:
    """Run pipeline and return frontend-shaped response: report row + payload + steps."""
    from src.pipeline import run_preview
    from src.models.step_result import Status
    from src.storage.db import load_company, load_run

    results = run_preview(ticker, skip_llm=skip_llm)
    statuses = {r.status for r in results}
    overall = "partial" if (Status.FAILED in statuses or Status.PARTIAL in statuses) else "success"
    run_id = ""
    payload = None
    for r in results:
        if r.step_name == "build_report_payload" and r.status != Status.FAILED and r.data is not None:
            run_id = getattr(r.data, "run_id", "") or run_id
            try:
                payload = r.data.model_dump(mode="json")
            except Exception:
                payload = None
        if not run_id and getattr(r, "data", None) and hasattr(r.data, "run_id"):
            run_id = getattr(r.data, "run_id", "")
    if not run_id:
        for r in results:
            d = getattr(r.data, "step_results", []) if getattr(r, "data", None) else []
            for s in (d or []):
                if isinstance(s, dict) and s.get("run_id"):
                    run_id = s.get("run_id", "")
                    break
            if run_id:
                break

    steps = [r.to_log_dict() for r in results]
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
    return {"report": row, "payload": payload, "steps": steps}


class CreateReportRequest(BaseModel):
    ticker: str = Field(..., description="Yahoo-format ticker, e.g. 2010.SR")
    skip_llm: bool = Field(True, description="Skip Gemini news summarization")


@app.post("/api/reports")
def create_report(req: CreateReportRequest):
    """Create a report (run preview). Returns report row + payload + steps for frontend."""
    ticker = (req.ticker or "").strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")
    data = _run_preview_and_response(ticker, skip_llm=req.skip_llm)
    return data


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
    return _run_preview_and_response(ticker, skip_llm=True)


@app.post("/api/preview", response_model=PreviewResponse)
def run_preview_api(req: PreviewRequest):
    from src.pipeline import run_preview
    from src.models.step_result import Status

    ticker = (req.ticker or "").strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")

    results = run_preview(ticker, skip_llm=req.skip_llm)

    # Overall status
    statuses = {r.status for r in results}
    overall = "partial" if (Status.FAILED in statuses or Status.PARTIAL in statuses) else "success"

    # Run ID from first step or from build_report_payload step
    run_id = ""
    payload = None
    for r in results:
        if r.step_name == "build_report_payload" and r.status != Status.FAILED and r.data is not None:
            run_id = getattr(r.data, "run_id", "") or run_id
            try:
                payload = r.data.model_dump(mode="json")
            except Exception:
                payload = None
        if not run_id and hasattr(r, "data") and r.data and hasattr(r.data, "run_id"):
            run_id = getattr(r.data, "run_id", "")

    if not run_id and results:
        # Fallback: use step_results from last step if they contain run_id
        for r in results:
            d = getattr(r.data, "step_results", []) if r.data else []
            for s in (d or []):
                if isinstance(s, dict) and s.get("run_id"):
                    run_id = s.get("run_id", "")
                    break
            if run_id:
                break

    steps = [r.to_log_dict() for r in results]
    return PreviewResponse(run_id=run_id, overall=overall, steps=steps, payload=payload)


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
