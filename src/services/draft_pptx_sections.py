"""
Service: draft_pptx_sections

Runs Gemini *after* numbers/QA are finalized to draft short text blocks used in
the PPTX: investment thesis, what-to-watch, catalysts, and risks.

Hard rule: the LLM must not introduce or compute financial numbers.
"""

from __future__ import annotations

from src.models.step_result import Status, StepResult, StepTimer

STEP = "draft_pptx_sections"


def _safe_lines(xs, limit: int) -> list[str]:
    out: list[str] = []
    for x in xs or []:
        s = (str(x) if x is not None else "").strip()
        if not s:
            continue
        out.append(s)
        if len(out) >= limit:
            break
    return out


def _build_prompt(company_name: str, *, ticker: str = "", sector: str = "", quarter: str = "", memo_data: dict | None = None, headlines: list[str] | None = None) -> str:
    md = memo_data or {}
    header = md.get("header") or {}
    rec = header.get("recommendation") or {}
    rec = (rec.get("display_value") if isinstance(rec, dict) else rec) or ""

    facts: list[str] = []
    if quarter:
        facts.append(f"- Quarter: {quarter}")
    if sector:
        facts.append(f"- Sector/Industry: {sector}")
    if rec:
        facts.append(f"- Street consensus rating (verbatim): {rec}")
    fx = md.get("quality_flags") or []
    if isinstance(fx, list) and fx:
        facts.append("- Data-quality flags (FYI): " + "; ".join(_safe_lines(fx, 6)))

    h = _safe_lines(headlines, 6)
    headlines_block = "\n".join([f"- {x}" for x in h]) if h else "- (none provided)"

    return f"""You are a disciplined equity research analyst writing slide-ready text.

Company: {company_name}
Ticker: {ticker}

Context facts (do not invent beyond these):
{chr(10).join(facts) if facts else "- (no additional facts provided)"}

Recent headlines (use only as qualitative context; do NOT quote URLs):
{headlines_block}

STRICT RULES:
- Do NOT include ANY numbers, percentages, currency amounts, dates, or tickers in your output (spell nothing with digits).
- Do NOT speculate about financial results; keep it qualitative.
- Keep phrases company-specific (avoid generic filler).
- Keep bullets short enough to fit without wrapping.
- For `investment_thesis`, write EXACTLY FOUR sentences total (no line breaks).
- Sentence one: clear stance on valuation and earnings quality (qualitative).
- Sentence two: three company-specific drivers that support the view.
- Sentence three: what will drive the stock reaction in the near term (qualitative).
- Sentence four: one key uncertainty and how it could change the narrative.
- Output must be ONLY a JSON object with these keys and types:
  - investment_thesis: string (exactly four sentences, around one hundred to one hundred and thirty words total)
  - what_to_watch: array of exactly 4 short bullet strings (each <= 6 words, no wrapping)
  - catalysts: array of exactly 3 short bullet strings (each <= 5 words, no wrapping)
  - risks: array of exactly 3 short bullet strings (each <= 5 words, no wrapping)
"""


def run(company_name: str, *, ticker: str = "", sector: str = "", quarter: str = "", memo_data: dict | None = None, news_headlines: list[str] | None = None) -> StepResult:
    with StepTimer() as t:
        try:
            from src.providers.gemini import _call_gemini  # type: ignore

            prompt = _build_prompt(
                company_name,
                ticker=ticker,
                sector=sector,
                quarter=quarter,
                memo_data=memo_data,
                headlines=news_headlines,
            )
            out = _call_gemini(prompt, for_investment_view=True) or {}
            if not isinstance(out, dict):
                out = {}

            thesis = (out.get("investment_thesis") or "").strip()
            wtw = out.get("what_to_watch") if isinstance(out.get("what_to_watch"), list) else []
            cat = out.get("catalysts") if isinstance(out.get("catalysts"), list) else []
            risks = out.get("risks") if isinstance(out.get("risks"), list) else []

            sections = {
                "investment_thesis": thesis,
                "what_to_watch": _safe_lines(wtw, 4),
                "catalysts": _safe_lines(cat, 3),
                "risks": _safe_lines(risks, 3),
            }
            ok = bool(sections["investment_thesis"]) and len(sections["what_to_watch"]) == 4 and len(sections["catalysts"]) == 3 and len(sections["risks"]) == 3
            return StepResult(
                step_name=STEP,
                status=Status.SUCCESS if ok else Status.PARTIAL,
                source="gemini",
                message="Drafted PPTX sections" if ok else "Drafted PPTX sections (incomplete)",
                data=sections,
                elapsed_seconds=t.elapsed,
            )
        except Exception as exc:
            return StepResult(
                step_name=STEP,
                status=Status.FAILED,
                source="gemini",
                message="Gemini draft sections failed",
                error_detail=str(exc),
                elapsed_seconds=t.elapsed,
            )

