# Approved research credit offer

November 24, 2026. Owner approved the four recommendations in the preceding product review. These changes apply to the separate Walnut 2.0 preview in draft PR846. They do not change live subscriptions, existing subscriber access or the homepage.

## Approved offer

| Plan | Monthly price | Credit allowance |
|---|---:|---:|
| Free | $0 | 250 total in the first month, then 100 monthly |
| Premium | $24.95 | 2,000 monthly |
| Pro | $39.95 | 4,000 monthly |

Premium annual billing remains $249.50 and Pro $399.95. Annual customers receive monthly allocations, not a year's credits upfront. Monthly credits expire at the cycle boundary; purchased credits carry forward. Purchased credits remain $0.02 each: $12.50 buys 625. Task charges remain 25 for a short answer, 100 for an analysis or comparison, and 400 for a focused report. Browsing companies/charts is free. Credits never unlock a higher subscription tier.

Every credit-charged answer includes one brief clarification using its existing evidence. New research, additional retrieval or expanded scope requires another visible quote. The included clarification and the original answer must share one provider-cost budget. This is an approved policy and a tested ledger claim, not a working AI generation feature yet.

## Implemented in the preview

- Shared backend allowance and sandbox catalog metadata now use Premium 2,000; catalog version is `walnut_research_v3`. Three products/five price amounts are unchanged. No Stripe catalog was created or modified.
- Free grants use a normal 100-credit month plus a one-time 150-credit welcome grant. The permanent per-account welcome identity prevents duplicate grants. A trusted verified-account creation timestamp must match the first cycle; request parameters must never supply it. The helper is internal. Verified onboarding, subscription invoices and the scheduler are still unconnected.
- A completed, credit-charged job can reserve one zero-credit clarification bound to its owner, original evidence fingerprint and question hash. Concurrent reservations serialize. Changed evidence, missing access, unfinished parents, second claims and clarification chains fail. Failure releases the slot; completed evidence cannot change. A future runner must reauthorize evidence and enforce no new retrieval, output bounds and the shared cost ceiling. The ledger cannot decide semantic scope by itself.
- The chat composer displays task cost and scope before sending. A configured sandbox balance displays available credits and the post-task balance. An unconnected balance stays explicitly unknown. Default browsing makes no credit-balance request. Paid Send and Enter are disabled until generation is connected; navigation shortcuts return to free browsing.
- Plans & Credits displays the approved allowances and one-time equivalents of $40 for Premium and $80 for Pro. No fake balance or successful-payment claim appears.

The new clarification table and job columns require a fresh isolated sandbox schema or a reviewed additive migration for an existing sandbox. `create_all` does not upgrade existing tables. No schema migration was executed. Older catalog versions/outstanding orders must be reconciled before changing a configured sandbox; do not silently repoint them.

## Economics, using prior scenario assumptions

Premium's 2,000 monthly credits cost $40 at the one-time rate, a 37.625% full-use saving for the monthly subscriber. Pro's corresponding saving remains 50.0625%. Alternative usage is 80 short answers, 20 analyses or five reports for Premium; 160/40/10 for Pro. These are not additive allocations.

At the previous proposed $0.002 variable-cost ceiling per redeemed credit, Premium has a $4 monthly budget, Pro $8, Free's introductory month $0.50 and subsequent months $0.20. With the earlier modeled payment/Billing fees, monthly contribution before shared costs is $19.75 Premium and $30.21 Pro. Premium annual contribution averages $16.02/month. These are modeled ceilings and contributions, not measured provider cost or net profit.

The 25/100/400 task budgets are $0.05/$0.20/$0.80 including the clarification. The old workload estimates excluded clarification; do not reuse them as measured total costs. Actual model quality, tokens, tools, retries, data licensing, hosting, support, acquisition, refunds and taxes still need validation. Reproducible offline Decimal calculations are in owner-workspace `artifacts/walnut-credit-economics-v3-nov24.py` and its JSON output. No model request or paid usage was made.

## Verification and build repair

Local Python 3.14: 53 credit/sandbox cases pass, including concurrency, duplicate grants, clarification ownership and evidence identity. The 18 focused chat/credit frontend cases pass. Browser checks at 390 and 1280 pixels show correct prices, explicit unconnected billing and disabled paid sending; mobile width does not overflow. Local auth proxy access was denied by the environment, so this was guest layout and disabled-state QA, not authenticated tier acceptance. Screenshots are saved in `artifacts/walnut-credit-approved-offer-nov24.jpg` and `artifacts/walnut-credit-approved-mobile-nov24.jpg` in the owner workspace.

Generated Next.js route checking exposed the existing named `TickerPageRenderer` export from a page module. The implementation was moved byte-for-byte, after line-ending and terminal-blank-line normalization, into `components/ticker/TickerPageRenderer.tsx`. The normal route reexports only its default and metadata; the anonymous route imports the same renderer with its existing anonymous headers. Authenticated, anonymous and Ask behavior is unchanged. Source-inspection tests now read the shared implementation. `next typegen` plus TypeScript passes; CI now generates route types before type checking.

Expanded source-inspection suite: candidate 180 passed/25 failed out of 205, compared with prior commit 6e8b8009 at 176 passed/25 failed out of 201. All 25 failing names are identical; four added cases pass. The baseline was exported separately and missing test dependencies restored from that exact commit. These pre-existing assertions remain unresolved, so this is not a claim that the whole repository suite passes. The renderer content exactly matches that commit. Exact CI and preview receipts are appended after completion. Hosted authentication, actual Stripe test payments, subscription lifecycle, PostgreSQL concurrency, AI execution/cost measurement and conversation persistence remain open. No production deployment, main-entry switch, provider activation, payment, model call or customer send occurred. Existing institutional replay was not changed or restarted.
