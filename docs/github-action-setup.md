# Set up the daily Investing.com cache refresh (GitHub Actions)

## Why this exists

Cloudflare blocks Render's egress IPs from reaching Investing.com.
The deployed runtime can't fetch live data — it reads snapshots from
`data/investing/<slug>__<kind>.json`. Those snapshots stay fresh only
if someone re-runs the fetch periodically.

GitHub Actions runners use residential-ish IP ranges that aren't
currently Cloudflare-flagged for Investing.com. The workflow below
runs the refresh daily, commits the diff, and pushes — Render
auto-redeploys with the new data within ~5 minutes.

## One-time setup

### Step 1 — Grant the workflow permission to push

GitHub Actions can push to the repo by default, but `contents: write`
must be enabled on the workflow (already declared in the YAML) AND
the repo settings must allow it.

1. Repo → Settings → Actions → General
2. Under **Workflow permissions**, select **Read and write
   permissions**
3. Save

### Step 2 — Add the workflow file

Either:

**Option A (CLI):** copy `/tmp/refresh-investing-cache.yml` from
the local checkout into `.github/workflows/refresh-investing-cache.yml`,
commit, and push. Requires a Personal Access Token with `workflow`
scope.

**Option B (GitHub UI):**
1. Repo → Actions → "New workflow" → "Set up a workflow yourself"
2. Name the file `refresh-investing-cache.yml`
3. Paste the YAML below
4. Commit directly to `main`

### Step 3 — Verify

Repo → Actions → "Refresh Investing.com cache" → "Run workflow"
(branch: main). The first run should succeed in ~2-3 minutes and
commit any diff to `data/investing/`. Render redeploys automatically.

## The YAML

```yaml
name: Refresh Investing.com cache

on:
  schedule:
    - cron: "30 4 * * *"   # 04:30 UTC daily — ahead of MENA market open
  workflow_dispatch:

permissions:
  contents: write

concurrency:
  group: refresh-investing
  cancel-in-progress: true

jobs:
  refresh:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - name: Checkout main
        uses: actions/checkout@v4
        with:
          ref: main
          fetch-depth: 1

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install "curl_cffi>=0.7" requests

      - name: Run Investing.com refresh
        run: python -m scripts.refresh_investing_cache --delay 4

      - name: Commit and push if changed
        run: |
          if [ -z "$(git status --porcelain data/investing/)" ]; then
            echo "No snapshot changes — nothing to commit."
            exit 0
          fi
          git config user.name  "investing-cache-bot"
          git config user.email "investing-cache-bot@users.noreply.github.com"
          git add data/investing/
          git commit -m "chore(cache): daily Investing.com snapshot refresh"
          git push origin main
```

## Failure modes

- **GHA runner gets Cloudflare-blocked**: workflow exits 0 with no
  diff; existing snapshots stay live. Need to switch to a paid
  residential-IP proxy (ScrapingBee, Bright Data) — see the
  `scripts/refresh_investing_cache.py` script for the integration
  point.
- **A new ticker has no slug**: workflow logs "[skip] TICKER: no slug
  in _SLUGS — add it first". Edit `src/providers/probe_investing.py`
  to add the slug, then re-run.
- **Push rejected by branch protection**: the workflow uses
  `GITHUB_TOKEN` which bypasses branch protection for write actions
  configured under repo settings. If your branch protection blocks
  bot pushes, either grant `contents: write` to the workflow's token
  via repo settings, or switch the push to a dedicated PAT stored in
  `secrets.CACHE_PUSH_TOKEN`.

## Cost

Free. The job runs ~2 minutes/day on a public runner, well under the
GitHub Actions free-tier budget (3,000 minutes/month on private repos;
unlimited on public).
