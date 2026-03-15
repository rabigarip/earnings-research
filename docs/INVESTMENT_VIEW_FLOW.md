# Investment View (IV) flow

Single source of truth for how the memo’s two-paragraph Investment View is produced and validated.

## Data flow

1. **Evidence** — `src/providers/gemini.py`: `_build_evidence_brief(company_name, memo_fact_pack, articles)` builds the evidence brief (company, sector, quarter, consensus, execution, news). Data density is classified as `rich` / `moderate` / `sparse`.

2. **Prompt** — Same module: `_build_main_prompt(...)` adds sector rules (`_sector_instruction`), sparse-data block when needed, and task/format instructions. Model is selected via `_get_iv_model()` (config: `investment_view_model`, e.g. `gemini-2.5-pro`).

3. **Call** — `_call_gemini(prompt, for_investment_view=True)` → Gemini API. Exceptions (e.g. missing key, network) propagate; empty or non-JSON response returns `None`.

4. **Parse & sanitize** — `_normalize_summary_out(out)` runs `_sanitize_iv_paragraph()` on both IV paragraphs (strips placeholder/instruction leakage).

5. **Validate** — `_validate_iv_output(p1, p2, ...)` checks: banned phrases, confidence words without numbers, word count (config: `iv_min_total_words`, `iv_max_total_words`, `iv_min_paragraph_words`), company-specific content, reaction framing, stance in p1, evidence/tone match. Bounds and banned lists live in `src/constants/iv_quality.py`.

6. **Retry** — One optional retry with `_build_retry_prompt(..., issues)`; replacement only if the new output has fewer issues.

7. **Guardrail** — `src/services/summarize_news.py`: after building `NewsSummary`, applies `guardrail_paragraphs(p1, p2)` from `qa_engine` so the payload carries clean IV. Same guardrail (shared patterns from `iv_quality`) is available at render for defense-in-depth.

8. **Payload** — Pipeline step 9 builds `ReportPayload(news_summary=NewsSummary(...))` with the guardrailed IV.

9. **Render** — `src/services/generate_report.py`: reads `payload.news_summary.investment_view_paragraph_1/2`. If long enough and not error-like, renders with optional inline citations; else builds fallback IV from memo + sector (`_sector_operating_kpis_and_what_matters`) and recent-context injection when required.

## Key files

| File | Role |
|------|------|
| `src/constants/iv_quality.py` | Banned phrases, reaction markers, word bounds, guardrail regexes |
| `src/providers/gemini.py` | Evidence brief, prompt, call, sanitize, validate, retry |
| `src/services/qa_engine.py` | `guardrail_paragraphs` (uses `iv_quality`), `guardrail_paragraphs` used in summarize_news and at render |
| `src/services/summarize_news.py` | Calls Gemini, builds NewsSummary, applies guardrail before returning |
| `src/services/generate_report.py` | Renders IV or fallback; `_sector_operating_kpis_and_what_matters` for sector fallback prose |
| `config/settings.toml` | `[gemini]`: `investment_view_model`, `iv_min_total_words`, `iv_max_total_words`, `iv_min_paragraph_words` |

## Config

- **Word bounds** — `config/settings.toml` → `[gemini]` → `iv_min_total_words`, `iv_max_total_words`, `iv_min_paragraph_words`. Defaults in `src/constants/iv_quality.py` if not set.
