# House publication holds and reviewed member name

October 15, 2026. Local implementation; release and live publication not established at this checkpoint.

## Current complete held cohort

A bounded read-only audit captures all eight current House publication holds. All eight original PDFs are independently fetched from the official House source and their full hashes match the production source receipts. Parsing retains all137 rows. No source payloads or customer records were exported from production; independently downloaded public originals and extraction receipts are in artifacts/house-holds-oct15/. The original Allen and Meuser pages were rendered with Poppler and inspected visually.

| Filing | Rows | Current finding |
|---|---:|---|
| 20035562, Salazar | 42 | Original parses; staging is quarantined because existing canonical records differ. |
| 20035464, Fields | 5 | Original parses; existing canonical reconciliation remains required. |
| 20035549, Taylor | 5 | Legacy disclosure date mismatch and ambiguous lots/instrument/member/date identity. |
| 20035622, Cisneros | 57 | Includes an explicit THR exchange action; current publisher accepts only purchases/sales. Non-stock bonds also remain source-specific. |
| 20035588, Hern | 24 | AIO is stored as Other Investment Fund while the disclosed row is parsed as stock; retain classification hold. |
| 20035487, Meuser | 1 | Original explicitly discloses NVIDIA common stock, but the shared NVDA security row is Stock Option. Do not relabel shared security without preserving historical instrument semantics. |
| 20035648, Allen | 2 | Exact official Richard W. Allen name differs from the current directory's Rick W. Allen. Repaired locally below. |
| 20035651, McCormick | 1 | Original says Richard Dean Dr McCormick and GA06; current term is GA07. Name matching alone cannot resolve the district discrepancy; remains held. |

Audit receipts: artifacts/house-holds-1792107148.json, house-holds-1792107246.json; public original/hash/row manifest artifacts/house-holds-oct15/replay.json. These eight publication receipts do not represent every staging-only scanned or unresolved House document; broader coverage remains open.

## Reviewed identity repair

The [official Allen filing](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20035648.pdf), SHA25669ae5a2b6ab70b7fa3d68adb3c852343f8963599c43738a3cee6e01b5c5b918e, identifies Hon. Richard W. Allen as Member for GA12. The [House Clerk member record A000372](https://clerk.house.gov/Members/A000372) identifies Rick W. Allen as the representative for GA12, with the current oath date January3,2025. The reviewed directory identifies that same member/term. This supports an explicit reviewed name alias, not general Richard/Rick nickname inference.

Only the exact normalized name Richard W. Allen is mapped to A000372, and only in House resolution. Current district, chamber, term dates and uniqueness checks remain mandatory. Wrong district, expired terms, other middle initials, unreviewed abbreviated names and duplicate directory candidates still hold. Source names, original bytes, metadata, trade/disclosure/arrival dates, row hashes, deduplication and all other publication checks are unchanged. McCormick's contradictory district is not overridden; [his official member website](https://mccormick.house.gov/) identifies the current GA07 office.

The complete original contains two spouse sales: AZO on September22,2026 in the15001-50000 bracket, and COST on September18,2026 in the1001-15000 bracket. The source filing date remains October14. Source-backed tests preserve both lots and their published IDs/timestamps/payloads on repeat, with zero EmailDelivery rows and network blocked.

## Validation and state

Before repair, two source/identity checks fail and seven negative guards pass. The repaired run passes138 focused tests on LinuxPython3.12.3 across reviewed identity, Congress reconciliation/publication, non-stock repairs, dates and legacy repairs. An earlier expanded invocation named a nonexistent test file and ran no tests; the corrected existing paths produced the138 result. This is not a full-suite claim.

The installed-code verifier passes local rehearsal on WindowsPython3.14.2, binding six module/directory hashes and replaying the complete original, canonical repeat and four negative identity guards in isolated SQLite with autoflush disabled. Installed-runtime proof follows the reviewed release. No schedule/provider flag/subscription/hardware change, no private-export retry, no source-classification rewrite or customer email. FMP remains enabled. Exact release, ordinary publication and all wider coverage/consumer/capacity/final-provider/zero-FMP/billing gates remain.


## Merged release; guarded deployment deferred, October 15 23:49 UTC

[PR694](https://github.com/moore11j/congress-tracker/pull/694) testedf2a2fea0f8af4d88b87c9f9f298d1013242e50ce, mergedf87fa57bd301041db69e2fa5c14d507373649536. Exact tested/merged tree parity, both dependency audits, secret scan and Vercel preview pass. The normal PR URL remains available after the100-attachment cap refusal.

Read-only production candidate audit artifacts/house-alias-canonical-1792107604.json confirms the proposed alias resolves A000372 with two complete original rows, new plan/no reasons, zero existing source filings/transactions/document events, and zero cross-filing economic candidates. No canonical or registry writes, customer records or source payload exports. An earlier SSH observation expired; an exact-process check found no remaining audit process before the bounded read-only audit was repeated. The second attempt has a remote30second wall bound and succeeds. This is read-only plan evidence, not actual publication.

Guarded deployment workflow38825201402/job116730935442 reaches terminalfailure at23:43:26 because reserve-release returns portfolio_cycle_unfinished. The image built successfully but rollout never began. Do not call f87fa57b deployed, run its installed verifier against the older service, or bypass the guard. The existing production release remains the previously verified18de6ae1; no new source activation or product data mutation occurred. No deployment retry was dispatched.

Minimized receipts artifacts/release-blocker-1792107873.json and release-blocker-1792107927.json show one continuing protected cycle at510/1135scopes with zero recorded scope failures, no currently active protected process and17queued jobs. The latest external request's succeeded status is distinct from this continuing cycle and does not clear the guard. Future deployment must revalidate actual terminal state and use the guarded workflow again. Never alter or clear the checkpoint to make the guard pass.

Operational receipt artifacts/active-work-1792108116.json identifies the actual live data-lane job as outcome-ledger-hydrator: two live processes,599seconds since claim,48.22CPU seconds;19jobs pending, oldest805seconds. Its configured1000symbols/365day scope and900second budget explain why ordinary source jobs can queue behind substantial work. Current code refreshes prices before computing all requested bundles, with the time check only around the outer loops; this is a follow-up inspection target, not yet a confirmed cause of lost captures. The normal SEC collector is queued at23:47, not completed, per sec-index-live-1792108066.json. No jobs were interrupted or schedules changed.

Public response spot checks at23:48:41 remain200/no rendered errors: Feed0.541s,Insights0.230s,AAPL0.342s,NVDA0.632s (artifacts/public-render-1792108121225.json). These do not establish freshness, sustained capacity or full consumer completeness.

## Shared NVDA classification scope

Read-only aggregate artifacts/nvda-instrument-counts-1792107737.json finds the shared NVDA security classified Stock Option, referenced by265transactions in241filings across48members. Existing NVDA Congress event payloads include439Stock Option and39equity labels, with no source_asset_type recorded in those groups. These counts are not evidence that every historic trade is an option or stock, nor a duplicate audit.

Independently fetched [NVIDIA periodic SEC report](https://www.sec.gov/Archives/edgar/data/1045810/000104581026000075/nvda-20260726.htm), filedAugust26,2026, binds CIK0001045810, trading symbolNVDA, Common Stock/$0.001par, and Nasdaq Global Select Market in the same c-1 cover context. Full HTML SHA2563937f0508a1370d535d58249329ee8cf5a490eb389bb35c562fc9eab9452a43d; public submissions SHA256682c49d59116ae49b317eb4adddf3ef9142dfd97f59c5d8e5a5acad7f9058ab4. Evidence artifacts/nvda-security-evidence-oct15/manifest.json and cover-facts.json. This proves the listed security identity; it does not classify each historical transaction. No shared-security relabel or historical event rewrite was applied.

Next work: retain safe release deferral, verify normal source rotation, investigate per-transaction instrument provenance and heavy-job continuity, then release/verify Allen when the portfolio cycle permits. Financial/institutional/transcript coverage, all scheduled consumers, sustained capacity, final Massive/paid-provider validation, zero-FMP requests and billing closure remain. Goalactive; this turn produced full-source evidence and a merged repair, not a completed migration.


October15,23:50UTC observation close: artifacts/sec-collector-schedule-1792108222.json ends its four-minute log observation with no collector completion. This is not a terminal-job inference: the last authoritative collector state was queued, and the actual outcome hydrator was live at23:48. No job restart or cancellation. All local test/audit/log handles and the refused deployment workflow are terminal; ordinary collection and the guarded release remain outstanding.
