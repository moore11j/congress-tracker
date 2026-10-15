# Vale IFRS financial preparation, October 15, 2026

## Coverage and source meaning

Vale was not covered by the domestic USD US GAAP preparation. Its reviewed USD IFRS statements now support seven existing ratios: gross margin, operating margin, net margin, trailing revenue growth, operating margin change, current ratio and return on parent equity. The public fundamentals selector remains FMP. This package adds preparation, not a full source cutover or a claim that every retained field is available.

Complete original statements reviewed:

- [June2026 amended interim](https://www.sec.gov/Archives/edgar/data/917851/000129281426004322/valedfifrs2q26_6ka.htm), filedAugust24, SHA025a02d00b1b45cbc5232d2e372c3c7a1a25ef652eda589f523abbe2f05a6b32. Amendment adds interactive data, with no other change claimed by the issuer; original July31 report also captured.
- [FY2025 annual](https://www.sec.gov/Archives/edgar/data/917851/000129281426001844/valeform20f_2025.htm), filedMarch27, SHA8612cc1e0754469208f8cded5b8c8bc2d6d40e00bd499551982736815750c467.
- [June2025 amended interim](https://www.sec.gov/Archives/edgar/data/917851/000129281425003232/valedfifrs2q25_6ka.htm), filedSeptember9,2025. Full hash/acquisition timestamp in artifacts/vale-financials-oct15/prior-amendment-manifest.json; prior original also captured.

Revenue is reported consolidated net operating revenue. Net income and equity are parent amounts, not consolidated noncontrolling-interest totals. All amounts remain USD; no ADR EPS or conversion is inferred. Current trailing revenue is38403+19756-16923=41236million; prior trailing revenue38056+16923-18379=36600million. Resulting growth12.6666666667%, operating margin16.0636337181%, margin change-6.5265302163points, gross margin34.8190901154%, net margin5.1144630905%, current ratio1.1917262789 and ROE5.4584276933%.

Free cash flow remains explicitly unavailable. The current custom net-CFO concept reports4291million, whereas standard CashFlowsFromUsedInOperations is5597million before further operating payments. Reported acquisition cash2429million includes both PPE and intangible assets. Neither value may silently substitute for the established net-CFO-minus-PPE-only formula. Original tables and tags retained in cash-statements.json and cash-reconciliation.json. EBITDA/leverage and ADR EPS remain separate unresolved coverage; no adjusted issuer metric is substituted.

## Implementation and rollover

sec_vale_financials defines the reviewed accounting/issuer basis. SEC reporting policy accepts matching Vale IFRS quarterly filenames and annual report names only, excluding event-only and BR GAAP filings even if the directory supplies quarter-end dates. The existing parser independently checks issuer, form, period, namespace, units, dimensions and conflicts. A newer same-period amendment forces acquisition even when older companyfacts has complete revenue; a newer financial report without usable inline data fails closed rather than claiming the older period current. Filing-day availability remains the completed New York day. Unknown future naming/taxonomy conventions require review; no universal future rollover claim.

Only Vale's preparation version changes. The existing bounded scheduler and source transport remain unchanged. No new schedule, host size, subscription, source selector or canonical publication. The existing evidence contract now uses issuer-specific margin wording so Vale does not inherit Novartis's net-sales/other-revenue description. Novartis output remains unchanged.

## Verification

226 focused tests pass on Linux Python3.12.3, covering Vale, Novartis, inline facts, currencies, fundamentals, preparation and bounded scheduling. Source-derived minimal fixtures preserve every parsed original fact:22current,18annual,22prior. Full original-source replay checks all28 financial inputs with zero mismatches and exact projection repeat; fixture parity is independently asserted.

Complete public-original cache/confirmation/watchlist replay passes on Windows3.14.2 and Linux3.12.3: seven prepared values, exact cache repeat, zero canonical writes during preparation, zero false availability-transition alerts, one later synthetic threshold-crossing alert, zero repeat duplicates/rollback alerts. Twenty saved control issuers match baseline7bcc8b7c exactly. Network blocked, no production records or emails. Receipts under artifacts/vale-financials-oct15/: original-replay.json, fixture-parity.json, full-replay-win32.json and full-replay-linux.json. Initial report serialization needed datetime conversion; an initial control replay lacked the baseline because sandbox Git ownership blocked its read. Both harness issues were corrected before these successful receipts.

Installed-code verifier locally rehearses seven hashes, seven-ratio cache repeat, incompatible cash absence, wrong-issuer rejection and new-amendment acquisition. Exact release, installed verification and ordinary preparation remain pending. Other migration gates remain: broad financial/transcript coverage, institutional publication/current cohort, all scheduled consumers and sustained performance, paid Finnhub/final Massive, zero globalFMP traffic and billing retirement. Unilever PDF403 remains unresolved; transcript run607 progressed two pages without errors but remains partial.


## Deployed and ordinary preparation verified, October 15, 21:32 UTC

[PR687](https://github.com/moore11j/congress-tracker/pull/687), tested500cab53bc71bf4a26561931f5f3a88d5e1eeea5, merged/deployed0b134b087ee189f679271d6fb91d713fe5088813. Fresh fetched merge has zero backend diff from the tested head. All dependency audits, secret scan and Vercel preview pass. Guarded workflow38810159344/job116681082206 completed success, with readiness/Premium privacy checks at21:30:17UTC. Two diagnostic502 responses were followed on the same job; metadata confirmed it was still running, no duplicate deployment. Existing checkout cleanup submodule warning is nonfatal. Chat attachment cap100 prevented an attachment; ordinary PR URL remains available.

Installed Python3.12.15 matches all seven expected source hashes and passes isolated seven-ratio preparation/cache repeat, incompatible cash absence, wrong-issuer rejection and new-amendment acquisition, with no production-record reads/writes or provider requests. Receipt artifacts/vale-runtime-1792099857.json.226 Linux3.12.3 focused checks and complete-original consumer replays remain the release validation.

The ordinary21:31:02 batch planned/completed one due scope, VALE, with statusok, no transport stop, public fundamentals selectionfmp and zero canonical writes. Its21:31:03 prepared cache is current-version/partial with seven metrics. On-server comparison exactly matches all seven public-original expectations; reviewed IFRS evidence is present and free_cash_flow remains absent. Receipt artifacts/vale-financial-prepared-1792099918.json exports only one-issuer metadata/counts/boolean comparisons, not financial or customer payload. No preparation was forced. An initial local probe prefix replacement caused JSON parsing failure after a read; the corrected metadata read produced this receipt without rerunning the job.

At21:30:51 public response/RSC checks all return200 with no server-render errors: Feed0.536s, Insights0.199s, AAPL0.255s, NVDA0.332s. Receipt artifacts/public-render-1792099851780.json. These are unauthenticated spot checks, not full signed-in coverage or sustained-capacity proof.

This verifies deployed Vale preparation and one ordinary acquisition, not public activation, universal future report handling or complete migration. Cash-flow/EBITDA/ADR coverage remains explicit. Unilever's issuer PDF403 remains unresolved. Hourly transcript run607 earlier processed2/error0 but27pending/28unresolved. Broad financial/transcript and institutional coverage, all scheduled consumers, sustained performance, paid Finnhub/final Massive, global zero-FMP and billing retirement remain open. No provider activation, new subscription/schedule/host/customer email/video/landing changes.
