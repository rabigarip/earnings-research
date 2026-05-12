# Stage 1 Coverage Matrix — v1 (Week 1, free sources only)

**Panel:** 10 tickers across GCC, India, China/HK
**Providers:** Yahoo Finance, MarketScreener, World Bank (macro overlay)
**Cells:** 390 (10 × 3 × 13)
**Runtime:** 154s

## Combined coverage (Yahoo OR MarketScreener OR Macro)

| Field | Yahoo | MS | Macro | Combined |
|---|---|---|---|---|
| current_price | 6/10 | 6/10 | — | **10/10** |
| market_cap | 6/10 | 6/10 | — | **10/10** |
| company_profile | 6/10 | 6/10 | 10/10 | **10/10** |
| income_statement_annual | 6/10 | 6/10 | — | **10/10** |
| income_statement_quarterly | 6/10 | 6/10 | — | **10/10** |
| valuation_historical | 6/10 | 6/10 | — | **10/10** |
| dividend_yield | 6/10 | 5/10 | — | **10/10** |
| target_price | 6/10 | 5/10 | — | **10/10** |
| rating_split | 6/10 | 5/10 | — | **10/10** |
| historical_prices | 6/10 | — | — | **6/10** |
| balance_sheet | 6/10 | — | — | **6/10** |
| cash_flow | 6/10 | — | — | **6/10** |
| valuation_forward | 6/10 | 1/10 | — | **6/10** |

## Coverage per ticker (fields covered out of 13)

| Ticker | Coverage | Notes |
|---|---|---|
| 2222.SR (Aramco) | 13/13 | Both Yahoo and MS cover |
| 0700.HK (Tencent) | 13/13 | Both Yahoo and MS cover |
| 1398.HK (ICBC) | 13/13 | Both Yahoo and MS cover |
| 2899.HK (Zijin) | 13/13 | Both Yahoo and MS cover |
| JINDALSTEL.NS | 13/13 | Both Yahoo and MS cover |
| ICICIBANK.BO | 13/13 | Both Yahoo and MS cover |
| ADCB.AE | 9/13 | Yahoo blank; MS covers everything except BS/CF/history |
| ADNOCDRILL.AE | 9/13 | Same pattern as ADCB |
| BKMB.OM | 9/13 | Same pattern |
| OQEP.OM | 9/13 | Same pattern |

## What's working without Week 2 exchange work

**9 of 13 fields covered 10/10 across the panel** using just Yahoo + MS:
- Identity (price, mkt cap, profile)
- Historical income statement (annual + quarterly)
- Historical valuation multiples
- Dividend yield
- Analyst target price + rating split

For these 9 fields, **the free sources already match what a paid feed would provide for the panel size** (subject to accuracy validation against IR-page ground truth in Week 3).

## What's NOT working — and why exchange providers fix it

**4 fields × 4 tickers = 16 missing cells**, all on the same MENA names:

| Field | Missing tickers | Why |
|---|---|---|
| balance_sheet | ADCB / ADNOCDRILL / BKMB / OQEP | Yahoo has no `.AE` / `.OM` fundamentals at all; MS doesn't publish full BS |
| cash_flow | Same 4 | Same reason |
| historical_prices | Same 4 | Yahoo lacks coverage; MS doesn't publish daily history |
| valuation_forward | Same 4 + JINDALSTEL/ICICIBANK | MS `/summary/` rarely populates the `pe_2026` fields |

**Fix in Week 2:** ADX (covers ADCB + ADNOCDRILL), MSX (covers BKMB + OQEP), HKEX (rich filings for Hong Kong-listed Chinese names — even Yahoo-covered ones get a higher-trust secondary source).

After exchange providers, the matrix should hit **13/13 × 10/10 for backward-looking fields**, with `valuation_forward` still being the weakest cell because no free source replaces FactSet/Bloomberg for forward estimates.

## Observations worth flagging now

1. **Yahoo's MENA gap is uniform** — all 4 `.AE` / `.OM` tickers fail with `no price in info`. Not a parsing bug; Yahoo's database simply doesn't carry them. We rely entirely on MS + exchanges here.
2. **MS forward valuation is sparse** — `/summary/` populates `pe_2026` for only 1 of 10 tickers. The data exists on `/valuation/` (10/10) but `/summary/` is unreliable. **Action:** the Stage 1 reconciler should pull MS forward valuation from `/valuation/`, not `/summary/`.
3. **Macro overlay works cleanly** — 10/10 hits on country macro context (GDP, inflation, population, FX) via the World Bank API. Free and unblocked.
4. **No accuracy validation yet** — coverage shows *whether* a value came back, not *how accurate* it is. Week 3 reconciler will compare cross-source agreement and flag disagreements > 5%.

## Next: Week 2 — exchange providers

Priority order based on which MENA gap each closes:

1. **ADX** (Abu Dhabi) — fixes ADCB + ADNOCDRILL
2. **MSX** (Oman) — fixes BKMB + OQEP
3. **Tadawul** (Saudi) — secondary source for Aramco (already 13/13), but key for the ~200 Saudi tickers in our broader universe
4. **NSE / BSE** (India) — secondary, already covered
5. **HKEX** (HK filings) — secondary for HK names

Plus IR-page parsers for the 10-ticker panel as the high-trust ground truth in Week 3.
