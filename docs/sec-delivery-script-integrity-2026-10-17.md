# SEC delivery-script integrity recovery, October 17, 2026 UTC

## Confirmed ordinary-source failure

The deployed BABA adapter ran in the ordinary00:36UTC SEC preparation batch and held its output with `baba_original_changed`. NVO subsequently ran at00:41 and held with `nvo_original_changed`. Both remain unavailable in the current preparation cache; local fixture success is not live-source acceptance. Public core fundamentals still use FMP.

Read-only status checks then identify the corresponding original-hash hold for ASML, SAP, TSM, CNQ, MGA, UL and SPCX. This affects nine specifically pinned financial adapters, rather than the entire SEC pipeline. The narrow status probe exports reasons, observation times and counts only, not financial or customer payloads. Receipt: primary artifacts/pinned-issuer-holds-1792197930.json.

## Complete-original evidence

All five BABA and four NVO documents were fetched from production using the existing identified serial SEC client. Their financial documents differ from the exact reviewed originals only in the path of an empty trailing external JavaScript tag. Replacing only that path with a fixed comparison marker produces identical full-document digests. No financial text, amount, period, namespace, whitespace, attribute, link or closing markup is normalized. The actual received-file hashes remain separate from the original reviewed-file hashes.

A broader inventory locates all nineteen additional exact reviewed originals for ASML4, SAP3, TSM3, CNQ3, MGA3, UL2 and SPCX1. A fresh serial public-source comparison confirms the same complete-document equality for all nineteen. Together this is28 complete originals across nine issuers. No financial revision was found in these28 comparisons; this does not establish freshness or completeness for other periods or issuers.

Evidence in primary artifacts:

- sec-delivery-originals-1792197582.json: nine BABA/NVO source URLs, old/received hashes, byte lengths, stable document hashes and observed delivery paths.
- sec-pinned-inventory-oct17.json: all nineteen additional reviewed original locations, raw-file pins and stable document hashes. Full source files were verified against current adapter pins before their comparison digests were accepted.
- sec-additional-delivery-originals-1792198096.json: fresh19/19 full-document equality, with received hashes and paths. No production records or writes.
- inventory-sec-pins-oct17.py and verify-additional-sec-delivery-oct17.py preserve the bounded inventory/comparison procedure. TSM current is in artifacts/yahoo-gaps-oct14/TSM-q2-original.html; SAP annual is in the reviewed foreign-source artifacts. Do not infer source identity from filenames; retain the exact hash matches.

## Local repair and validation

The managed branch codex/sec-delivery-script-integrity currently implements the repair for BABA and NVO only. It is not released. A shared helper accepts a rotated path only for the exact terminal empty script shape, root-relative bounded path syntax, and an explicitly pinned complete-document digest. Removed, moved, duplicated or nonempty scripts, additional attributes/content, financial changes, dates/units and other byte changes remain rejected. It never executes a script. Both issuer versions advance to v2 for normal refresh eligibility. Input facts and covers retain actual received hashes; source evidence also retains reviewed hashes, comparison digests and policy identifiers.

268 focused checks pass on Linux Python3.12.3 in241.48seconds. These include nine complete live-byte reconstructions verified against the actual received hashes, unchanged financial projections, nested received-source provenance and tamper rejection, plus existing issuer, financial, cache and custom-alert tests. No full-suite claim.

Complete-source BABA consumer replay passes on Windows Python3.14.2:18 annual facts match independent companyfacts, prepared-cache repeat is exact, score/summary consumers work, and availability transitions and delivery-hash-only changes produce zero false alerts. A later synthetic crossing triggers once, repeats produce no duplicate and rollback produces none. NVO replay passes on Linux Python3.12.3 with32 annual matches and the same cache/score/alert properties. Both are network-blocked isolated SQLite replays, with no production writes or emails.

The first NVO replay used a baseline predating the new BABA adapter and incorrectly included BABA among unchanged controls. That comparison was corrected to exclude both intentionally changed adapters, while requiring both older target controls to remain held. The corrected NVO run compares eighteen unaffected issuers exactly; the earlier successful BABA run compared nineteen including an unchanged held NVO control. The prepared BABA helper now also uses the eighteen-control scope but has not been rerun after that scope-only adjustment. Do not claim a new eighteen-control BABA receipt until executed. Receipts: artifacts/baba-financials-oct16/delivery-replay-win32.json and artifacts/foreign-facts-oct16/delivery-replay-linux.json.

Installed complete-variant verifier scripts for BABA/NVO are prepared but not yet rehearsed or run against a new release: artifacts/verify-{baba,nvo}-delivery-runtime-oct17.py. Current production remains f4f9ff0f, whose v1 adapters hold these rotated pages correctly.

## Required expansion before release

Extend the same verified source policy to ASML, SAP, TSM, CNQ, MGA, UL and SPCX using their nineteen exact originals. Preserve actual received provenance throughout the facts and source summaries; do not silently keep a reviewed raw hash labeled as the received file. UL's separately pinned restatement PDF is unrelated and must retain its existing policy. SPCX has a separately pinned parsed projection and comparative accounting bridge; preserve that financial-integrity guarantee while separating variable delivery metadata. Do not weaken financial projection validation simply to make a changing received hash pass.

Then run complete-original parser/consumer comparisons for the expanded scope, review/merge, use the guarded release, verify installed behavior and observe normal scheduled recovery for all affected issuers. No PR or deployment for this repair has been created yet. Broad financial coverage, institutional activation, transcripts/analyst gaps and all final acceptance gates remain open.

## Independent progress and reliability

Two free Finnhub requests prepared the October and November IPO caches before selection. October now has33 provider status observations; November is explicitly empty. Cache-only calendar reads retain all33 identities, qualified dates/currency and exact repeat behavior. No canonical events, customer data or email writes occurred; writes are limited to the two public caches and existing request-budget reservations. IPO remains unselected. Receipt: artifacts/ipo-live-prepared-1792197367.json. Recheck24-hour freshness before activation; current source records do not establish independent market completeness.

Portfolio diagnostic receipt portfolio-latest-category-1792197384.json confirms that the206/1135 checkpoint and the terminal external request have the same target run, finish time and exit code-15 at2026-10-16 07:05:21UTC. This is a verified failed cycle, not a stale receipt accidentally matched to another run. No reset/restart was performed. The later19:36 unrequested machine crash does not establish the cause of this earlier interruption. Successful subsequent full-cycle and sustained-capacity validation remain open.

Previous goal turn and this turn are concrete progress. FMP remains enabled, Massive personal Stocks remains last, and subscriptions, schedules, hosting, publication gates, customer sends, landing pages and video remain unchanged. Pending private export approvals were not retried. The goal is active and incomplete.


## Nine-issuer implementation and local acceptance, October17 01:02UTC

The repair now covers all nine affected issuers and all28 reviewed/received complete documents. ASML, SAP, TSM, CNQ, MGA, UL and SPCX were added to the earlier BABA/NVO repair. Each adapter version advances for normal scheduled refresh. Source metadata retains the received SHA256, reviewed SHA256 and narrowly scoped document pin. Separate covers which are not refetched remain reviewed-source evidence, not new acquisition claims. UL's archival PDF, publication/observation clock and PDF integrity checks are unchanged.

ASML/TSM parsed bundles retain the received bytes transiently and authenticate their receipts again in downstream projection. SPCX similarly authenticates the complete received original before normalizing only its verified delivery hash in the separately pinned financial projection. All amounts, periods, source locations and accounting bridge fields remain included in that projection. Its new reviewed projection pin is337468b3abf8dae61232d30cea1af0a89ffcd9c1b00df7d50aa991b7faac1e31. Two hard-coded downstream SPCX version checks were updated; existing accounting and tamper tests pass. Raw original bytes are not serialized into prepared cache evidence. Memoryview hashing avoids large temporary document copies, and five adapters no longer compute redundant raw hashes.

Validation:742 distinct Linux Python3.12.3 checks pass across the broad run and the corrected seven-consumer rerun. The broad run had740pass/two test-only failures because the new common alert scenario assumed operating margin for CNQ/MGA, which intentionally omit it. The shared scenario now uses supported current ratio; all seven complete-source projection/cache/score/alert cases pass in33.32seconds. No application change was needed for that correction. Earlier exact raw-key assertion errors were also test-only and corrected; reviewed_original metadata is allowed, transient _original payloads are not. Existing full-source parsers, amount/period/accounting/identity guards and133 additional non-delivery tamper cases remain enforced. Complete19-original projections preserve financial values and actual received provenance. Cache preparation writes no canonical rows; source recovery and hash-only rotation produce0false alerts, a later synthetic crossing produces1, repeats0duplicates and rollback0alerts for each of seven issuers.

All nine bounded installed-package verifier rehearsals pass locally. Receipt artifacts/sec-delivery-runtime-rehearsals-1792198777.json contains exact hashes and metric counts: ASML9,SAP7,TSM5,CNQ4,MGA7,UL7,SPCX6,BABA7,NVO9. Seven expanded verifier rehearsals use1.94-8.74local CPU seconds; these are local Windows timings, not production capacity evidence. Production verifiers are bounded, sequential and network-denied, using isolated SQLite and complete public fixtures. No actual installed proof is claimed yet.

Read-only release-blocker-1792198847.json has0active/continuing protected work and5ordinary pending jobs; latest portfolio request remains terminalfailed. No checkpoint reset/restart. The guarded workflow must revalidate/reserve atomically before deployment. This turn makes concrete implementation/validation progress after the previous percentage-only no-progress turn. Review/merge, exact guarded release and ordinary nine-issuer recovery remain next. FMP/core selection, institutional publication, IPO selection and all global retirement gates remain unchanged.
