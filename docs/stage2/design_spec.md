# Jabal Asset Management — Deck Design Spec

Captured from the reference deck `JAM_SABIC_AgriNutrients_Q2_2026_Preview (3).pptx`.
This is the canonical visual specification for the new report renderer.

## Page geometry

- **Aspect ratio**: portrait
- **Slide size**: 7.50" × 13.32" (matches the existing pipeline geometry — no template change needed)
- **Page count**: 3 (Snapshot / Thesis & Expectations / Valuation & Positioning)
- **Outer margin**: 0.45" left/right, ~0.30" top, 0.20" bottom-bar

## Typography

Two-family system: **serif for display, sans for everything else**.

| Use | Font | Sizes (pt) |
|---|---|---|
| Company name (slide 1 hero) | Georgia | 26 |
| Section page title (slide 2/3 hero, e.g. "Executive Summary") | Georgia | 17 |
| Page section labels (e.g. "ANALYST CONSENSUS") | Calibri | 10.5, all caps, letter-spaced |
| Metric values (e.g. "SAR 144.20") | Calibri | 14–15 |
| Metric labels (e.g. "LAST CLOSE") | Calibri | 8.5, all caps |
| Body copy | Calibri | 10 |
| Footer line | Calibri | 8 |
| Tab numbers ("1 / 3") | Calibri | 8.5 |
| Page header line (e.g. "PAGE 1 · SNAPSHOT") | Calibri | 10, letter-spaced |

## Color palette

```
PRIMARY
  --jabal-black     #1A1A1A    primary text, hero numbers
  --jabal-gray      #5C5C5C    secondary text
  --jabal-muted     #9A9A9A    tertiary / divider labels
  --jabal-gold      #A28860    accent — left rules, watermark
  --jabal-gold-dk   #7E6849    secondary accent

SEMANTIC
  --jabal-pos       #2F7D4F    positive deltas, "Buy" pill
  --jabal-neg       #B83227    negative deltas, "Risk" pill

SURFACES
  --jabal-page      #FAF8F4    page background (warm off-white)
  --jabal-card      #EFE8DC    card fill (cream)
  --white           #FFFFFF
```

## Visual primitives

- **Section header**: 10.5pt all-caps Calibri in `--jabal-gray`, with a 0.5pt horizontal rule above (full-bleed within page margins) in `--jabal-muted`.
- **Card**: 0.5pt 1px border in `--jabal-muted`, 0.05" thick left-edge accent strip in `--jabal-gold`, fill `--jabal-card` or white. Vertical padding 0.10".
- **Metric block**: label (8.5pt all-caps `--jabal-muted`) over value (14–15pt `--jabal-black`). Stacked tight, no separator.
- **Range bar (52-week)**: horizontal track 0.06" tall, `--jabal-muted`. Filled segment in `--jabal-gold` from low to current. Small `--jabal-black` diamond at current position. Labels (low/high) at ends in 10pt.
- **Bullet row (highlights list)**: left-side category pill in 8pt caps (`EARNINGS`/`VALUATION`/`POSITIONING`/`WATCH`) over body copy. Pill background `--jabal-card`. ~0.42" row height.
- **Numbered list**: small numeral in `--jabal-gold` (10pt) at left, body in 10pt black to the right. Used for "What to watch on the print."
- **Tables**: borderless, alternating row tint `--jabal-card` (light), 1pt baseline rule below header in `--jabal-muted`. Right-aligned numerics with thin variable-width spacing.
- **Bar chart** (P/E range): horizontal segment in `--jabal-card` (range), highlighted segment in `--jabal-gold` (current). FY labels left, axis ticks below.

## Slide-by-slide structure

### Slide 1 — Snapshot
1. **Header strip**: JABAL / ASSET MANAGEMENT (left), PAGE 1 · SNAPSHOT / INSTITUTIONAL RESEARCH · EQUITY (right).
2. **Title block**: kicker (`EARNINGS PREVIEW NOTE`) → company name (Georgia 26) → meta line (ticker · sector · industry · exchange) → period subtitle (`Q2 2026 Earnings Preview`).
3. **Analyst consensus row**: three side-by-side cards — Rating + analyst count / Target price / Upside-to-target.
4. **Key data row**: 6 metric blocks across — Last Close / Market Cap / Report Date / P/E (FY est) / Div. Yield / Currency.
5. **Recent performance row**: 6 metric blocks across — 1D / 1W / 1M / 3M / 6M / YTD. Colored green/red.
6. **52-week range**: horizontal bar viz with low/high labels and current marker.
7. **Analyst highlights (key points)**: 5 rows, each a category pill (EARNINGS/EARNINGS/VALUATION/POSITIONING/WATCH) + one-line summary.
8. **Footer**: sources line / page-tabs / confidentiality line.

### Slide 2 — Thesis & Expectations
1. Header strip (PAGE 2 · THESIS & EXPECTATIONS).
2. **Executive Summary card**: section label, Georgia 17 title, body paragraph (~80–110 words).
3. **Q2 estimates table**: Jabal vs Consensus, rows for Revenue / EBITDA / EBITDA margin / Net income / EPS / Net debt / Dividend.
4. **Catalysts / Key Risks** — two side-by-side cards, 3 bullets each. Header band in card.
5. **What to watch on the print** — numbered list, 3 lines.
6. Footer.

### Slide 3 — Valuation & Positioning
1. Header strip (PAGE 3 · VALUATION & POSITIONING).
2. **Two-up chart row**: 52-week price chart (left), P/E multiple 5-year range (right).
3. **Peer comparables table**: header row + ~5 peer rows + the company.
4. **Market sentiment row**: three cards — Consensus distribution / Average target price / Last 3 broker actions.
5. Footer.

## What this replaces

The current renderer produces a 12-step PPTX with snapshot, summary, peers, ratings, income evolution, price action, etc. — but each in a separate visual idiom. The Jabal spec compresses this into 3 tightly designed slides that share one design system. Several existing renderers map cleanly:

| Existing renderer | Jabal slide | Notes |
|---|---|---|
| `render_cover.py` | Slide 1 header + title block | Reuse logo + meta line code |
| `render_snapshot.py` | Slide 1 key-data + performance | Refactor to grid + Jabal palette |
| `render_summary.py` | Slide 2 executive summary | Same content, new card style |
| `render_ratings.py` | Slide 3 sentiment cards | Reformat 3 cards across |
| `render_peers.py` | Slide 3 peer table | Reformat as borderless table |
| `render_price_action.py` | Slide 3 52-week chart | Compress to ~3.2" × 2.1" |
| `render_income_evolution.py` | Drop or relocate to optional slide 4 | Not in 3-slide spec |

## Data-source wire

Every datum on every slide must come from `canonical_store.get_all_fields(ticker)`. No direct provider calls inside renderers. The freshness/confidence tier of each cell is available alongside its value and can be surfaced in the footer ("Data as of 12 May 2026 · 8 of 9 fields High/Medium confidence").
