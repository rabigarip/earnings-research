# Paid-data evaluation — closing the forward-estimate gap

**Purpose**: v3's coverage matrix proves the free-source stack is structurally complete for backward-looking financials, identity, and price/valuation history. The remaining gap is **forward analyst estimates** — EPS estimates, target prices, rating splits — where MarketScreener is the only free source and drifts materially vs. Bloomberg per the user's BBG comparison. This doc evaluates the paid options that could close that single gap without paying for the rest of a Bloomberg seat.

## The narrow gap we're solving

What the deck actually needs from a paid feed:

| Field | Per-ticker need | Refresh cadence |
|---|---|---|
| Forward EPS / revenue / EBITDA estimates | Mean / median / high / low / std-dev, FY1 + FY2 | Weekly is fine; daily is overkill |
| 12m target price | Mean + range | Weekly |
| Rating split | Buy / hold / sell counts | Weekly |
| Earnings surprise history | Last 4–8 quarters: estimate vs. actual | Quarterly |

That's it. We don't need fundamentals (have those), prices (have those), news (have those), filings (have those). The narrow scope is the key — it means we can shop for a single-feed contract rather than a terminal seat.

## The four options

### 1. LSEG (Refinitiv) I/B/E/S
**The historical incumbent.** 22,000+ companies in 100 countries, 950+ broker contributors, US data back to 1976, international data back to 1987. It IS what Bloomberg's `EE` and `BEST` screens are essentially benchmarked against.

- **Coverage on our panel**: Confirmed for all 10. The MENA, India and HK tickers all have I/B/E/S broker contributors.
- **Delivery**: LSEG Data & Analytics API (RESTful, returns JSON) or workspace API.
- **Pricing**: Not public. Industry chatter puts a full Workspace seat at $22–30K/user/year. **A data-feed-only license** (no workspace) typically runs $6–15K/year for a constrained tickers universe — but requires negotiation through LSEG sales.
- **Verdict for us**: Best emerging-markets coverage among the four. **Probably the right answer if the budget allows** ≥$10K/year.

### 2. FactSet Estimates
**The challenger that's the buy-side default.** Cleaner data model, more granular surprise/guidance fields, native Excel/Python connectors.

- **Coverage on our panel**: Strong for HK and India; mixed for MENA (FactSet relies on regional broker contributors who don't always cover Oman or Saudi banks).
- **Delivery**: FactSet API (REST + Python SDK), Snowflake share, or workstation.
- **Pricing**: A full FactSet Workstation runs $12–50K/user/year. **The "FactSet Open" data feed** is a slimmer option — pricing on application, typically $8–20K/year for a narrow Estimates-only license.
- **Verdict for us**: Marginally better data model than I/B/E/S; weaker MENA coverage. **Choose this if the panel skews India + HK and the analyst wants the cleanest surprise-history fields**.

### 3. S&P Capital IQ + Visible Alpha
**The cheapest entry point for institutional-grade consensus.** S&P Capital IQ Pro tiers start at **$1,000/user/year** (Essentials), up to $2,083/user/year (Advanced) per April 2026 list pricing. Visible Alpha consensus (the institutional standard among the four) is bundled into Capital IQ Pro Advanced or sold standalone.

- **Coverage**: Visible Alpha covers 7,000+ companies, 200M+ data points, broker-by-broker granularity. Panel coverage confirmed for all 6 of our most-followed tickers (Tencent, ICBC, Aramco, ICICI, BKMB, ADNOC Drilling); thinner for Zijin, Jindal, ADCB, OQEP.
- **Delivery**: RESTful API, file feed, or Snowflake.
- **Pricing**: **Massively cheaper** than the others on entry. $1–2K/year covers a single research analyst. Visible Alpha add-on typically $3–8K/year depending on universe size.
- **Verdict for us**: **The right answer if total budget is <$10K/year.** Coverage is comparable to FactSet for our panel, and the price-to-value ratio is unbeatable. Trade-off: less broker-by-broker transparency than I/B/E/S.

### 4. Free-source-only (status quo)
- **What it costs**: $0
- **What it delivers**: MarketScreener forward fields, with the known drift vs. BBG. Useable for narrative ("analysts expect Q1 EPS of X"), not citable for client-facing forecasts.
- **When this works**: Internal-use decks, learning/training, or building the rest of the pipeline before committing to paid data. **This is what we've shipped through Stage 1.**

## Decision matrix

| Criterion | Free (status quo) | Cap IQ + Visible Alpha | I/B/E/S | FactSet |
|---|---|---|---|---|
| Annual cost | $0 | **$4–10K** | $10–25K | $12–30K |
| Forward-estimate accuracy | Drifts | Bloomberg-grade | Bloomberg-grade | Bloomberg-grade |
| MENA coverage | MS only | Good | **Best** | Mixed |
| HK / India / EM coverage | Good (Yahoo + MS) | Very good | Very good | Very good |
| API quality | n/a | REST + Snowflake | REST | REST + SDK + Snowflake |
| Time-to-integration | 0 (shipped) | ~2 weeks | ~4 weeks | ~4 weeks |
| Negotiation required? | No | Sometimes | Yes | Yes |

## Recommendation

**Start with S&P Capital IQ Pro Essentials + Visible Alpha consensus add-on.** Reasoning:

1. **Cost-fit.** ~$5K/year all-in covers a single analyst seat with Bloomberg-grade forward consensus. That's an order of magnitude below FactSet/Bloomberg and matches the scale of an independent research practice.
2. **Coverage on our panel is sufficient.** 6 of 10 panel tickers are deep-coverage; the other 4 (Zijin, Jindal, ADCB, OQEP) have 3+ contributors per Visible Alpha's stated universe — enough for a meaningful consensus.
3. **Upgrade path is clean.** If panel-coverage gaps prove material, swap up to I/B/E/S at a known cost step; the API layer in the production pipeline (below) abstracts the difference.

If the analyst's workflow leans heavily MENA, consider I/B/E/S instead — better MENA broker coverage justifies the price premium.

**Do NOT recommend FactSet** for this use case: priced like Bloomberg without the coverage advantage on our specific panel.

## What this changes in the v3 → v4 pipeline

Once a paid feed is wired, the affected fields in the coverage matrix flip from "MS only / Low confidence" to "paid + MS / High confidence":

- `valuation_forward` — paid feed becomes canonical; MS retained as a sanity cross-check
- `target_price` — same
- `rating_split` — same
- Earnings surprise history — new field, paid-feed only

The production pipeline (next doc: `production_wireup.md`) is designed so swapping in the paid provider is one new file (`src/providers/paid_estimates.py`) plus one line in `_load_providers()` — no consumer code changes.

## Open questions before signing a contract

1. **Trial access.** All three vendors offer 2–4 week trials. Worth running our panel through the trial API before committing — confirms coverage matches their universe claims.
2. **Historical depth required.** I/B/E/S has 30+ years of US history; Visible Alpha has shorter history but more granularity. If we'll be doing surprise-vs-estimate trend analysis going back >5 years, I/B/E/S wins.
3. **Number of seats.** Pricing scales sublinearly per seat. If the practice grows to 3+ analysts, the per-seat cost on FactSet/I/B/E/S drops meaningfully; Cap IQ stays flat per seat. Inflection point is around 5 seats.
4. **Compliance / data redistribution.** If we publish numbered estimate figures into reports that leave the firm, the contract type changes (redistribution license is 2–4× the price). Display-only ("internal research" use) is cheaper.
