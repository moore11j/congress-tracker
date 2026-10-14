# Reviewed consolidated income statements

October 14, 2026. Implemented and locally verified; release/runtime/ordinary operation pending. Public fundamentals remain FMP. No canonical leverage policy is selected.

## Source finding

The v6 leverage preparation correctly refuses to assume missing noncontrolling interests are zero. However, NVIDIA and Johnson & Johnson report their total consolidated earnings under `NetIncomeLoss` without a complete `ProfitLoss` series. Their full original statements establish the basis more precisely than the standalone tag. This is an original-statement review, not a rule that any pretax-minus-tax arithmetic proves the absence of other allocations.

Reviewed sources are NVIDIA's [2026 annual report](https://www.sec.gov/Archives/edgar/data/1045810/000104581026000021/nvda-20260125.htm) and [July quarterly report](https://www.sec.gov/Archives/edgar/data/1045810/000104581026000075/nvda-20260726.htm), and Johnson & Johnson's [2025 annual report](https://www.sec.gov/Archives/edgar/data/200406/000020040626000016/jnj-20251228.htm) and [June quarterly report](https://www.sec.gov/Archives/edgar/data/200406/000020040626000153/jnj-20260628.htm). Four original HTML files, companyfacts and company submissions are saved with hashes. Five income tables expose14 annual/quarterly observations; only six needed trailing-period observations are approved. JNJ's2023 discontinued earnings explain a21.827billion difference and are deliberately excluded from the review registry; the2025 annual statement explicitly shows zero discontinued earnings.

| Issuer | Fiscal annual earnings | Current six months | Prior six months | Trailing earnings | Prepared EBITDA |
|---|---:|---:|---:|---:|---:|
| NVIDIA |120.067|118.010|45.197|192.880|233.894|
| Johnson & Johnson |26.804|10.769|16.536|21.037|33.959|

All amounts above are USD billions. Annual plus current minus prior matches all three original income observations exactly. EBITDA retains the existing aligned tax, interest and D&A observations; debt/cash values and all debt/lease holds are unchanged. These prepared candidates are not issuer adjusted EBITDA or proof of equivalence to an FMP metric.

## Implementation and boundaries

`sec_statement_income_reviews.json` records the six reviewed periods, issuer CIK, source URL/hash, filing accession/date, context/table identity and original amounts. The projector admits a reported net-income observation only when its filing, period, unit and value match. Inline observations must also match their original hash, URL and context. Reported consolidated facts and conflicts take priority; changed allocations, values, restatements, identities and unreviewed future filings remain held. Parent/shareholder metrics are untouched. The proof is carried into the leverage calculation evidence. Missing allocations alone never fill a value.

Only NVDA/JNJ preparation versions change, allowing their existing bounded schedule to refresh. No full-universe rebuild, extra source request, canonical write, score change or source selection is introduced. This registry is a bounded reviewed evidence bridge; it does not establish automatic future-quarter coverage. New filings require new original review or a separately validated general statement reader before this can be called a durable complete replacement.

## Validation

255 affected Linux Python3.12.3 tests pass, including exact reviews, source mismatch, conflicting allocations, preserved parent metrics, reported-consolidated priority, future-period rejection and existing leverage/parser/cache behavior. Complete-original replay for both issuers plus20 saved-source controls passes on Windows3.14.2 against deployed a9a94204. All canonical values and non-leverage evidence are identical, unchanged issuers match exactly, all debt/lease holds are identical and repeats are deterministic. Two current and two dated target controls recover complete EBITDA evidence; six previous coverage holds remain.

The actual preparation service persists both exact leverage panels in isolated SQLite on Linux3.12.3 and returns identical repeats with transport prohibited. No canonical rows, production writes, network requests or emails occur in these replays. The new JSON evidence registry is included by the backend Dockerfile's existing COPY instruction.


The first cache harness compared the companyfacts-only path with forced-inline provenance and failed. Its corrected reference retains each path's full provenance and separately requires identical original amounts and all six review proofs. NVIDIA's optional older cash-flow supplement is deliberately unavailable in this bounded fixture; that path is not a cash-flow coverage pass.

Receipts: `artifacts/income-statements-oct14/manifest.json`, `inventory.json`, `audit.json`, `source-replay.json`, `cache-replay.json`. The source audit's first read-only production check also confirms remaining unsupported foreign financial coverage; `artifacts/unsupported-financials-1792010209.json`. Presence of these gaps is not evidence that source content does not exist.

## Remaining gates

Release and exact deployed behavior, ordinary NVDA/JNJ refresh, broader/future consolidated source coverage, debt-policy acceptance, whole-universe financial and score/ranking/all-alert checks, institutions/transcripts, sustained capacity, final Massive, zero global FMP traffic and billing remain. FMP stays enabled. No change to schedules, hosting, subscriptions, customer emails, videos or the landing page.
