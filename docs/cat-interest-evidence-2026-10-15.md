# Caterpillar interest reconciliation evidence

October 15, 2026, 01:40 UTC. Offline source proof is complete. Production acquisition and preparation integration are **not implemented**, and no public metric or provider selection changed.

## Source gap and recovery

A new offline audit of the 20 checksum-verified companyfacts/submissions pairs captured on October 9 found four complete EBITDA preparations, ten incomplete and six held at core projection. This deliberately excludes later originals and issuer supplements, so these counts are not current production coverage. The audit identifies Caterpillar as missing only the interest component. Receipt: `artifacts/saved-ebitda-audit-oct15.json`.

The original consolidated income statements report two explicitly disjoint expenses: interest of Financial Products and interest excluding Financial Products. Both have `srt:ProductOrServiceAxis` contexts. The members are respectively `cat:FinancialProductsMember` and `cat:AllOtherExcludingFinancialProductsMember`. SEC companyfacts does not include those observations, and the existing generic original parser correctly excludes dimensional facts. That exclusion must remain.

| Period | Financial Products | Excluding Financial Products | Combined interest |
| --- | ---: | ---: | ---: |
| Year ended December 31, 2025 | $1,359 million | $502 million | $1,861 million |
| Six months ended June 30, 2026 | $707 million | $269 million | $976 million |
| Six months ended June 30, 2025, as reported in the current interim filing | $662 million | $242 million | $904 million |
| Trailing year through June 30, 2026 | $1,404 million | $529 million | **$1,933 million** |

The corresponding period-aligned components from the same original filings are consolidated net income $10.841 billion, tax expense $3.273 billion and D&A $2.379 billion. Adding the complete interest amount produces **$18.426 billion EBITDA** under the existing preparation definition. Selecting only the nonoperating line would omit $1.404 billion. This is a calculation from reported statements, not company-adjusted EBITDA or a claim of equivalence with FMP's definition.

Public originals, freshly retrieved with the research User-Agent and finite request/size limits:

- [2025 annual filing](https://www.sec.gov/Archives/edgar/data/18230/000001823026000008/cat-20251231.htm), filed February 13, 2026; 6,100,587 bytes; SHA256 `a599b3d51789d4bcbdd269192af26ab1e0ed1e3e7d8b2fc9f16f6d77508d325e`.
- [June 2026 interim filing](https://www.sec.gov/Archives/edgar/data/18230/000001823026000046/cat-20260630.htm), filed August 5, 2026; 4,580,618 bytes; SHA256 `7c417e54e8dda457a23d152104ddb537bad4e8da0019e9bfbc2cff6c7a139d63`.

Originals, source manifest, statement tables and replay receipt are in `artifacts/cat-interest-oct15/`. These are downloads of public SEC documents, separate from the production institutional snapshot whose export approval remains pending.

## Offline proof and checks

The research-only reader first runs the existing strict original parser, then validates the exact issuer namespace, CIK, USD unit, single reviewed axis/member, period, statement row label and complete two-part pair. It does not change the generic parser. Three annual and four interim periods are recovered; existing entity-wide parser outputs contain 43 and 42 facts respectively.

The replay matches all existing EBITDA companion inputs to actual original observations by tag, value, period, accession and filing date before attaching original provenance. It retains the source CIK rather than removing it to bypass the compatibility check. The original documents and unchanged companyfacts project to identical canonical financial values and non-leverage evidence. The complete projection repeats identically. The new interest amount changes the preparation from incomplete to components complete. There are zero replay network requests, production writes or email deliveries.

**Ten Python 3.14.2 checks pass**, including original pair recovery and rejection of altered source bytes, wrong issuer, wrong axis/member, a changed exclusion label, wrong namespace/currency, negative expenses and missing components. These checks validate the research prototype, not deployed code or acquisition/cache behavior. Test command: `backend/.research-ops-venv/Scripts/python.exe -m pytest artifacts/test_cat_interest_oct15.py -q -p no:cacheprovider`.

The initial prototype assumed the wrong Caterpillar namespace and stopped before accepting data. Inspection corrected it to the actual `http://www.cat.com/<reportdate>`. A subsequent compatibility failure exposed missing source CIK on the existing companyfacts-derived companion records. The final replay resolves that by matching those records to the actual originals and attaching their provenance; it does not weaken compatibility rules. Failed attempts produced no accepted replay receipt or production changes.

## Remaining implementation and release gates

Implement bounded acquisition of the required annual/current originals and an issuer-specific preparation adapter. Preserve caches, explicit observation/conflict priority, source availability, exact issuer/statement validation and generic dimensional exclusion. Test unavailable originals, identity changes, amendment conflicts, incomplete periods, missing components and unchanged other issuers before release. Verify ordinary scheduled persistence and full financial consumer behavior before selecting a canonical ratio. Do not deploy the prototype by copying its diagnostic monkey-patch or treat these two filings as proof for arbitrary future statements.

At 01:33 UTC, the read-only portfolio receipt still shows no new active, pending or continuing portfolio cycle and no new external request. The latest GitHub scheduled-ingest run is the completed 00:28 recent-Congress run, not portfolio. Receipt `artifacts/portfolio-cycle-1792028002.json`; no duplicate or manual job dispatched. Ordinary portfolio completion remains a gate.

The last verified deployment remains `12fd3668`. Core financials/prices/transcripts remain FMP and institutional publication remains disabled. The blocked institutional export was not retried. Current full institutional writer/consumer evidence, financial/transcript coverage, sustained performance, all alert/ranking consumers, final Massive subscription/validation, global zero-FMP requests and billing/access dates remain open. No account, host, schedule, subscription, customer-email, video or landing-page change.
