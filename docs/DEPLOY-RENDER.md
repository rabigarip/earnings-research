# Deploy to Render + Connect a Frontend

## 1. Push code to GitHub

Ensure your project is in a Git repo and pushed to GitHub (or GitLab):

```bash
git init
git add .
git commit -m "Add FastAPI API and Render config"
git remote add origin https://github.com/YOUR_USERNAME/earnings-research.git
git push -u origin main
```

Make sure `data/company_master.json` is committed (it’s the seed for the DB). Keep `.env` out of the repo (it’s in `.gitignore`).

---

## 2. Deploy on Render

1. Go to [render.com](https://render.com) and sign in (or sign up with GitHub).
2. **New → Web Service**.
3. Connect your GitHub account and select the `earnings-research` repo.
4. Render can pick up `render.yaml` automatically, or you can set:
   - **Runtime:** Python 3
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `uvicorn src.api:app --host 0.0.0.0 --port $PORT`
5. **Environment variables** (in Render dashboard):
   - `GEMINI_API_KEY` — optional; only if you want news summarization (otherwise use `skip_llm: true` from the frontend).
   - `CORS_ORIGINS` — optional; e.g. `https://your-frontend.vercel.app` to restrict CORS (default `*` allows any origin).
6. Create the service. Render will build and deploy. The API will be at `https://<your-service-name>.onrender.com`.

**Note:** Render’s disk is ephemeral. On each deploy the app runs `init_db()` and `seed_companies()` on startup, so the DB is recreated from `data/company_master.json`. No persistent DB between deploys unless you add an external database later.

---

## 3. API endpoints for your frontend

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check (for Render and monitoring). |
| GET | `/api/companies` | List all companies: `[{ ticker, company_name, exchange, country, currency }, ...]`. |
| POST | `/api/preview` | Run earnings preview. Body: `{ "ticker": "2010.SR", "skip_llm": true }`. Returns `{ run_id, overall, steps, payload }`. |

**Preview response:**

- `run_id` — ID for this run.
- `overall` — `"success"` or `"partial"`.
- `steps` — Array of step results (step_name, status, message, etc.).
- `payload` — Full report payload (company, quote, financials, consensus, news summary, etc.) when the pipeline succeeded; `null` if build failed.

Interactive docs: `https://<your-service>.onrender.com/docs`.

---

## 4. Connecting a frontend (earnings-preview)

The **earnings-preview** frontend is wired to this API:

- **Local dev:** In the frontend repo, leave `VITE_API_BASE_URL` unset. Vite proxies `/api` to `http://localhost:8000`.
- **Production:** In the frontend build (e.g. Vercel / Netlify), set `VITE_API_BASE_URL=https://<your-render-service>.onrender.com`.

**Endpoints the frontend uses:**

| Frontend action   | API call |
|-------------------|----------|
| List reports      | `GET /api/reports` |
| Search tickers    | `GET /api/tickers/search?q=...` |
| Create report     | `POST /api/reports` with `{ ticker, skip_llm: true }` |
| Report detail     | `GET /api/reports/:id` |
| Rerun             | `POST /api/reports/:id/rerun` |

**CORS:** The API allows all origins by default. For production, set `CORS_ORIGINS` on Render to your frontend origin(s), comma-separated.

**Timeouts:** Preview can take 30–90 s with LLM, or ~15–30 s with `skip_llm: true`. Render free tier has a 30 s request limit; use `skip_llm: true` or a paid plan for longer runs.

---

## 5. Run the API locally (same as Render)

```bash
pip install -r requirements.txt
uvicorn src.api:app --reload --port 8000
```

Then open http://localhost:8000/docs and http://localhost:8000/api/companies. Point your frontend’s API URL to `http://localhost:8000` for local development.
