# Financial consumer provider isolation, October 14, 2026

## Current-universe cutover rehearsal

Read-only production capture at17:27:15UTC covers1,751 tracked cached symbols using deployed SEC preparation/version/freshness checks, the real SEC-first Finnhub combiner, financial summary and Confirmation fundamentals-source conversion. Public source remains FMP. The rehearsal excludes market-derived fields and does not select prepared leverage candidates. It does not invoke providers, write caches/scores or send email.

1,361 legacy rows have enough financial inputs for a classification;1,066 free preparations do.367 previously classified symbols become unavailable and72 previously unavailable symbols become classified. There are1,433 changed fundamentals source summaries; this is not1,433 changed complete Confirmation Scores. Neutralizing legacy cache age isolates field/definition effects; these are not refreshed legacy observations or whole-product numerical parity. No existing acceptance threshold was relaxed.

Large affected names include TSM, BRK-B, ASML and BULL. Foreign reporting, absent/denied free metric coverage, missing operating-margin comparisons and unselected leverage remain real gaps. Prepared free cash flow and earnings yields also await independently verified Massive market inputs. Earlier financial-statement panel recovery does not imply current core financial classification coverage. Do not turn core fundamentals off on this evidence.

Receipt: artifacts/financial-consumer-readiness-1791998838.json, SHA256662b56628a493a4a18033c5eb83443f25d3a7146c2d5ca8bb9f1b341dbfbe55f. The first diagnostic stopped on incorrect dataclass serialization before producing a receipt; corrected read-only repeat passed. No production changes.

## Reproduced consumer defect and repair

Five live readers selected the newest fundamentals row across providers: custom watchlist rules, Research Memory suggestion evidence, research brief identity, research brief financial context and macro positioning. Ticker hydration separately hardcoded FMP. Preparing a newer canonical replacement cache could therefore change a consumer before explicit activation; a newer legacy refresh could reverse it afterward.

Six readers now reuse the existing selected-provider cache accessor already used by ticker summaries and screening. Missing or failed selected rows remain unavailable, rather than falling back to another provider. Explicit selection and rollback govern transitions. Imports stay local to the reader to avoid adding scheduler startup cost. No provider selector, source row, historical research, schedule, entitlement or delivery preference is changed by this release.

All30 new regression cases were run before repair:29 failed and one passed. Existing tests that used the fictitious provider test now use supported FMP; alert transition fixtures explicitly select providers. One SQL-count fixture was corrected so inspecting its expired ORM row did not add an unrelated query to the count.

## Captured-value watchlist replay

The actual evaluator runs200 financial rules for20 public companies in isolated SQLite on Python3.12.3.104 rules have legacy values and134 have replacement values; missing observations remain explicit. A newer unselected replacement, explicit switch, identical repeat, newer unselected legacy refresh and explicit rollback each create zero alerts. Synthetic later below/above-threshold observations produce134 triggers and134 monitoring records, all uniquely identified; repeat creates none. These later values are test inputs, not issuer observations. Both immediate/daily eligibility use the same retained monitoring records; actual email delivery and full ranking/score rebaseline are not claimed.

Receipt: artifacts/financial-consumer-replay-oct14.json. Network calls, emails and production writes arezero. Source snapshot and application hashes are recorded. No customer watchlists or private records were exported.

## Validation and release state

Final focused Linux3.12.3 selection/alerts/macro/hydration run93pass. Expanded Linux run initially200pass/10fail; fixture corrections resolve three, while the other seven research failures reproduce identically against exact unchanged3f69704d code (both comparison runs7fail/2pass). Windows3.14.2 research/monitoring/daily-weekly group212pass/one fixture failure, followed by15/15 passing affected alert checks after correcting instrumentation. This is not a full-suite pass. Deployment is verified below. Linux API TestClient collection is unavailable because its isolated environment lacks httpx2; the configured Windows environment covers that suite. No dependency manifest was changed.

CPU sample17:25UTC has zero current throttling and approximately0.087CPU against0.125sustained allowance, with10,871burst credits. The preceding two-hour window includes exhaustion and deployments, so sustained recovery remains unproved. Two hosts unchanged. All financial coverage, institutions/transcripts, full consumer/scheduled operation, final Massive and global FMP/billing gates remain.


## Deployed and runtime verified,17:41 UTC

PR636 head12c7ff6e4a288cf29a82113866574cd36c8236dd merged95acbbdccb7febdaf326f363455704e0caef8c7e at17:38:10UTC. Backend workflow38608344336/job116004945228 succeeds at17:39:40, including readiness and Premium privacy checks. Ten normalized runtime hashes match; deployedPython3.12.15 exercises all six readers under FMP/SEC/free selection plus missing and failed selected rows, using only an isolated in-memory database. Effective live provider remainsFMP; institutional publication remainsfalse. Receipt financial-consumer-runtime-1791999608.json. No cached source or history was rewritten.

Ordinary17:40 watchlist price-alert job completes exit0 in6.43wall/3.8562CPU seconds. This verifies normal daily-price execution, not all replacement-driven financial alerts or weekly delivery. News23/23 completes with42/42 fresh company caches. Institutional reference receipt retains484pending/466deferred scopes and partial status; it is not a crash or completed coverage. Runtime and source evidence: scheduled-job-cpu-1791999619.json and preload-receipts-1791999626.json.

Public reads all return200: Feed0.508s, Insights1.108s without research fallback,44briefs0.634s, AAPL3.469s and NVDA3.124s. These are immediate postrelease samples, not sustained performance proof. APIshared2CPU/2GB and DBperformance1CPU/2GB remain unchanged. No added cost or schedule.

CI secret scan and preview-comments pass. Both dependency audits fail in unchanged manifests: Pillow10.4.0 has33 reported advisories; frontend has4high/1critical production findings in existing Next/PostCSS/Sharp/nanoid/source-map-js dependencies. Inspected required CI diagnostics; no clean audit or completed Vercel-build claim. PR attachment attempt hits the existing100-item limit; no attachment was removed.

This release closes the six reproduced reader-selection defects. The broad financial source gap remains:367 lost classifications comprise158 unsupported current financial coverage,104 symbols absent from SEC directory,89 with missing scored inputs,11 missing companyfacts,3 expired and2 missing preparations. These are tracked symbols including non-common-stock instruments, not all verified eligible U.S. common equities. Market-pressure cap/profile cache semantics remain a separate final-Massive acceptance task. Do not select core financials or disable FMP until the remaining source and full-consumer gates pass.

## Missing-input diagnosis, October 14, 21:25 UTC

A bounded read-only capture of the 90 missing-input scopes from the 19:48 audit retains 179 SEC/Finnhub preparation rows and 90 legacy financial rows. Local analysis finds overlapping input gaps: 59 lack both current and prior operating income; nine lack prior operating income; nine lack usable revenue for operating-margin expansion; 11 lack equity observations; three lack parent income; 20 have nonpositive reported equity; 16 lack prior revenue; one has nonpositive prior revenue. These explain prepared-calculation holds, not all original-filing coverage or accounting validity. The three parent-income candidates with positive beginning/current equity are FLEX, FOX and JEF. Review actual originals before changing reconciliation. Never infer absent noncontrolling income is zero, remove the positive-equity safeguard, or treat bank pretax income as operating income to match a legacy score.

Receipts: artifacts/financial-loss-evidence-1792012914.json and artifacts/financial-loss-diagnosis-oct14.json. The earlier 1,751-symbol audit is artifacts/financial-consumer-readiness-1792007313.json; it reports 364 lost classifications before the subsequent ASML repair. Counts are dated scope evidence, not current global cutover readiness. No source requests, production writes or emails were performed by this cache capture. No public provider switch or application deployment occurred in this diagnosis.

CPU receipt artifacts/direct-feeds/free-replacements-2026-10-09/cpu-quota-1792012791.json shows zero current throttle at21:19, with earlier exhaustion at20:18 and peak throttle at20:22. Recent deployments and diagnostics confound the observation; sustained capacity remains open.
