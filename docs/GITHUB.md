# Push to GitHub and deploy on Render

## 1. Create a new repo on GitHub

1. Go to [github.com/new](https://github.com/new).
2. **Repository name:** `earnings-research` (or any name you like).
3. Choose **Public**, leave **Add a README** unchecked (you already have one).
4. Click **Create repository**.

## 2. Push this project to GitHub

In your terminal, from the `earnings-research` folder:

```bash
cd /Users/rabigaarip/Desktop/earnings-research

# Add GitHub as remote (replace YOUR_USERNAME with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/earnings-research.git

# Push the main branch
git push -u origin main
```

If your default branch is `master` instead of `main`:

```bash
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/earnings-research.git
git push -u origin main
```

## 3. Deploy on Render

1. Go to [dashboard.render.com](https://dashboard.render.com).
2. **New → Web Service**.
3. Connect your GitHub account and select the **earnings-research** repo.
4. Render can auto-detect from `render.yaml`. Or set:
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `uvicorn src.api:app --host 0.0.0.0 --port $PORT`
5. Click **Create Web Service**. After the build, your API will be at `https://<service-name>.onrender.com`.
6. Optional: in **Environment**, add `GEMINI_API_KEY` and/or `CORS_ORIGINS` if you use the frontend.

See **DEPLOY-RENDER.md** for frontend wiring and API details.
