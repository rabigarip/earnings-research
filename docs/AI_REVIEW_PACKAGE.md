# AI agent — backend pipeline review package

This archive is a **minimal slice** of the repo for **code review** (pipeline logic, data flow, failure modes). It is **not** a runnable clone by itself unless you add secrets and data.

## What’s included

| Path | Purpose |
|------|---------|
| `src/` | All Python: `pipeline.py`, `api.py`, `services/*`, `providers/*`, `models/*`, `storage/*`, etc. |
| `config/` | `settings.toml`, QA config |
| `tests/` | Existing pytest files |
| `requirements.txt` | Python dependencies |
| `README.md` | Project overview & CLI/API commands |
| `docs/*.md` | Design / ops notes (IV flow, Render, local testing, …) |

## Deliberately excluded (smaller zip, no secrets)

- **`.env`** — never share API keys; use `.env.example` in your real repo only.
- **`data/company_master.json`** — large mapping DB; not needed to read pipeline logic.
- **`frontend/`**, **`node_modules/`**, **`static/`** — UI build artifacts; backend review starts at `src/pipeline.py` + `src/api.py`.
- **`.git/`**, **`cache/`**, **`outputs/`** — bulky or environment-specific.
- **`__pycache__/`**, **`*.pyc`**

## Suggested review order

1. **`src/pipeline.py`** — `run_preview`: list steps in order, note which are CRITICAL vs resilient.
2. **`src/services/build_report_payload.py`** — how step outputs become `ReportPayload`; entity/MS suppression.
3. **`src/models/report_payload.py`** — data contract fields.
4. **`src/services/generate_report.py`** — PPTX output + IV fallback + sector helpers.
5. **`src/services/qa_engine.py`** — validation / guardrails (if present in tree).
6. **`src/api.py`** — how runs are persisted and downloads work.
7. **Providers** under `src/providers/` — external IO, caching, error handling.

## What to produce (for the human)

- **Step table:** step name → module/function → main inputs → main outputs → behavior on failure.
- **Risks:** P0/P1/P2 with **file paths** (and line numbers if possible).
- **Ambiguities:** places ticker/entity mismatch could leak wrong-company data.
- **Tests:** gaps and 3–5 concrete pytest ideas (no need to implement unless asked).

## Optional local run (reviewer)

```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pytest tests/ -v
```

Full pipeline needs DB init, `data/company_master.json`, and API keys — **not required** for static code review.
