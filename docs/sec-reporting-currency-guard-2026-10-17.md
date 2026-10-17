# Foreign statement currency ambiguity repair

## Confirmed source defect

The complete retained NAMI June 2026 SEC financial exhibit states that renminbi is the reporting currency and USD figures are convenience translations. The generic USD foreign-statement adapter admitted those USD columns because companyfacts contained standard USD concepts. PR713 therefore established exact byte/value parity, but did not establish a valid reporting-currency basis for NAMI. Its previously reported seven-source recovery must be corrected to six validated sources plus this currency hold. No native-CNY replacement or full migration completion is claimed.

The ordinary SEC research job at 03:34 on October17 did populate the seven historical panels before this defect was found, independently of queued general-enrichment requests. Receipts document-set-schedule-1792208548.json, document-set-ordinary-1792208308.json and document-set-public-1792208325.json show exact source projections and HTTP200. Earlier inference that queued general jobs meant those panels were still unpopulated is superseded. Queue priorities were not changed.

## Shared repair

The inline parser records current entity-wide standard monetary currencies before selecting USD facts. Wrong-entity, dimensional, old-period, nil and per-share columns do not establish another reporting currency. Generic foreign admission requires exactly one observed currency, USD. Explicit reviewed currency readers retain their existing accounting rules; no ticker eligibility list or exchange-rate inference is added. This detects ambiguity, not a universal proof that every single-currency document is a company's native reporting basis.

The proof travels through acquisition and source authorization. Prepared ratios, free-source combination and public historical readers reject old generic admissions without the new policy/proof. Only typed currency holds become cached unavailable results with retained original hashes, preventing repeated source requests and endless page warming. Other parsing/identity errors retain their prior behavior. Old financial history is not rewritten.

## Evidence and release

Offline replay of all48 retained affected sources recovers six sources and42 supported fields with exact repeats; NAMI is held for unreviewed_foreign_statement_currency. Remaining41 source holds remain. Artifact: document-set-cohort-oct17/replay-currency-guard.json. Zero provider requests or production writes. This is not whole-universe live acceptance.

Initial affected Linux run:172pass/seven failures from two incorrect new fixture inputs (missing XML namespace and invalid provider identity). Both fixtures were corrected. Final expanded tests and exact release are pending. One attempted expanded command exited before collecting tests because it referenced a nonexistent filename; the corrected command is running. Prepared isolated installed verifier covers six original sources, typed NAMI hold/repeat and old public-cache rejection.

Release preflight release-blocker-1792209074.json finds zero protected unfinished/active/continuing jobs and no pending scheduler jobs. The latest failed portfolio request is preserved, not reset or called successful. Current production remains0993a2a7; this repair is local on codex/sec-reporting-currency-guard. CoreFMP, institutions publication, source selections, schedules, hosting and subscription settings are unchanged. Broader coverage, all consumers, sustained operation, final Massive and zero-FMP/billing gates remain open.

Expanded Linux run completed291pass/one invalid nil-fixture failure. The nil fixture was corrected to contain no text and all17 new checks then passed locally onPython3.14.2. Added one net-interest revenue currency case; final Linux guard/parser rerun is active. No application failure was observed in the expanded source/history/native-currency/consumer cases. Local38-file installed-verifier rehearsal passes six source/cache/history repeats plus NAMI rejection and old-public-cache rejection (8.641CPU seconds, network denied). Release remains pending.
