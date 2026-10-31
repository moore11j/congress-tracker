# Shared IFRS statement qualification, October 31, 2026

## Problem and source evidence

The existing HTML financial reader required explicit US GAAP preparation wording. That held foreign-company statements even when their originals explicitly declared IAS 34 or IFRS. The read-only 21:56 UTC discovery audit found 36 held qualifications, 29 with `table_accounting_basis_unproved`; this is not evidence that all 29 are recoverable. Receipt: `discovery-holds-1793483784.json`.

Ten complete public SEC documents/submissions for CINT, FVRR, ONON and SPOT were retrieved independently, 3,841,369 bytes, with original URLs, lengths and SHA-256 recorded. Compressed originals and the manifest are retained in `backend/tests/fixtures/sec_ifrs_declarations/`. No private production financial values were exported.

Spotify's October 22 filing explicitly names Spotify Technology S.A. as the owner of consolidated statements prepared under IAS 34. Its income and balance-sheet tables use EUR millions. The shared reader now qualifies 16 observations: four each of revenue, gross profit and operating income, plus two each of assets and liabilities. The two quarterly revenue values are EUR 4,969 million and EUR 4,272 million; the 2026 nine-month value is EUR 14,279 million. The panel exposes two quarters and does not annualize the nine-month period. Parent-holder income, cash flow, EPS, current ratio and TTM remain absent because this path does not establish their required evidence.

The other sampled complete sources remain held. CINT and ONON still lack accepted table ownership under this shared policy. FVRR's sampled release does not establish the required explicit accounting declaration. No company-specific allowlist or inferred accounting jurisdiction was added.

## Change

- `sec_statement_declarations.py` recognizes explicit IAS 34/IFRS preparation statements and exact named statement ownership. Negated, hypothetical, quoted and script/template declarations do not establish ownership or accounting basis.
- `sec_tabular_financials.py` supports standard financial-position/profit-or-loss headings and explicit currency-bearing million scales. Document ownership requires the actual named preparation declaration; conflicting company captions and unknown local context remain rejected.
- `sec_tabular_reporting.py` preserves native currency, requires a versioned IFRS declaration proof, and rejects mixed US GAAP/IFRS declarations. Existing US GAAP proof shape remains unchanged.
- `sec_financial_discovery_reader.py` retries only legacy accounting-basis holds once under the new policy. Successful receipts, unrelated holds, source bounds and the global preparation version are not reset.

Collection remains enabled and public discovery readers remain disabled. Core fundamentals, transcripts and prices stay on FMP. No provider activation, schedule/resource/subscription changes, customer messages, video restart or landing-page changes are part of this package.

## Validation and release status

The initial existing Windows regression passed 111 cases. New focused tests initially exposed two real parser defects: negation outside the matched declaration, and narrative financial references misclassified as company captions. Both are fixed. Two hold expectations were corrected to reflect the existing currency guard's earlier rejection. One initial command used the wrong working directory and collected no tests. The final focused Windows run passes 21 cases (`ifrs-focused-final-oct31.xml`), including complete originals, a one-original acquisition/repeat, public panel/cache compatibility, selective retry and truthful missing metrics. The panel contract test deliberately uses an identity-only companyfacts stub; it does not claim complete historical companyfacts acquisition.

Broader Linux regression, frozen deployed-source comparison across 49 retained source scopes, review/release and installed verification are pending until receipts are appended below. No production recovery is claimed from local qualification.

At 22:07 UTC, ordinary discovery still completes ten-scope batches with two or three legacy covers and three or four original requests per examined scope. There are 171 checkpoints, two qualified and 35 held receipts, with 2,037 pending qualifications. Receipt: `discovery-batches-observation-1793484451.json`. The prior PR748 cash-flow growth release is installed, but nine sampled caches still carry v2 and two already had growth through another path; ordinary v3 recovery is unobserved (`independent-cash-flow-observation-1793484457.json`). These are bounded metadata checks, not whole-product acceptance.

The FMP exit remains incomplete: broad financial coverage, current institutional cohort validation, transcripts, all alert/ranking/watchlist consumers, sustained performance, final Massive subscription/activation, zero-FMP traffic and billing closure remain gates. The separate institutional production snapshot/OneDrive transfer is still awaiting explicit permission; no capture or transfer occurred.


## Exact-source regression acceptance

Linux/Python3.12 passes 193 distinct cases across `ifrs-regression-linux-oct31.xml`, `ifrs-discovery-linux-oct31.xml` and the final `ifrs-final-linux-oct31.xml`, with no failures or skips. The broad run passes183 cases; final focused31 rechecks all new-source paths after tightening individual-standard/future-intention rejection and conflicting title suffixes. This covers parser declarations and ownership, existing native currencies, discovery storage/batching/delivery integrity, public financial cache contracts, and one actual IFRS qualification. Its incomplete scan still prevents public selection. The identity-only companyfacts input supplies no external financial values; all accepted observations come from the complete HTML original.

All49 retained source scopes produce exactly identical before/after results, including full panels/core projections, held reasons and request lists (`ifrs-cohort-parity-oct31.json`). The baseline is a frozen export of deployed9d10ca2d, run in a separate Python process. The candidate comparison was repeated after the final basis tightening. No monkeypatched accounting declaration was used for acceptance. CPU137.609s before/82.266s after is one local comparison, not a production speed claim. No network or production writes occurred in the comparison.

Read-only preflight `release-blocker-api-1793484682.json` found no active/protected/continuing jobs and the latest portfolio request succeeded. The guarded workflow must recheck before any rollout. Review/merge, deployment, installed-original verification and ordinary scheduled recovery remain pending at this checkpoint.
