# Guarded institutional publication

October 8, 2026. Continuation of the owner's instruction to keep working until FMP is off. This stage implements a locally tested publication job and expands real SEC evidence. It does not activate production or complete the migration.

## Publication and source ownership

The [13F worker](../backend/app/services/direct_13f_worker.py) commits canonical filings, holdings, derived events, enrichment queues, feed cache invalidation and publication receipts in one transaction. It uses durable feed control with a separate institutional transaction lock. Source selection remains FMP by default; updated workers require the additive staging/control schema before rollout. Selection requires an immutable filing-date boundary, expected generation and reason. Pause is the supported rollback, not an automatic return to an unreconciled legacy writer.

Canonical holding/change/summary/event helpers, SEC snapshot installation and provider enrichment writers check transaction-scoped ownership. Legacy latest-filings and enrichment entry points check before their FMP fetch. Ownership caches/scopes expire with the transaction. Maintenance scripts and raw administrative SQL still require release review; this is not a database-wide prohibition on all possible writes.

Receipts bind exact source bytes, metadata, filing/position fingerprints, holder changes/activity and public holder-event evidence. Missing or modified public holder events are held on replay. Shared cluster events can evolve when another holder files, so they are not treated as immutable holder evidence. The worker does not verify every historical cluster constituent. Contradictory peer evidence is rechecked on explicit replay. Failed queue/cache work rolls back publication. First holds are recorded to prevent unresolved filings monopolizing batches; prior publication receipts are never replaced by a later hold.

The [batch loader](../backend/app/services/direct_13f_batch.py) reads checksummed original bytes from staging and an operator-reviewed identifier manifest. Manifest files must stay in their directory, duplicates are rejected, and byte/document bounds apply. Up to 500 captured comparison filings are considered. Lack of matching independent peers is not proof of correct values. Parsing, identities and identifier availability are revalidated for each target filing.

The [job](../backend/app/jobs/publish_direct_13fs.py) defaults off with `DIRECT_13F_PUBLICATION_ENABLED=false`. Enabling requires the reviewed manifest and separately selecting SEC through [control_direct_feed](../backend/app/jobs/control_direct_feed.py) with `--feed sec_13f`. New documents and explicit `--retry-waiting` runs are separate, preventing old holds from starving new work. A late prior quarter can unlock a waiting current filing once. Late identifier repair of already imported unmapped holdings still requires explicit reconciliation. No institutional cron entry or production flag was activated.

## Real source evidence

Ten additional original N-PORT submissions were captured and reparsed offline with identical document hashes and identifier results. Exact accepted records include AAUC, CTAS, AVNS, NVO, TSM, ZS, CRWD, NVDA and NFLX; two captures contain no usable identifiers under the strict equity/ticker rules. No name guesses were added. An exploratory submission exceeded the existing 30 MB transport limit and was not used.

Primary examples: [AAUC, May 27](https://www.sec.gov/Archives/edgar/data/1976322/000089418926015964/xslFormNPORT-P_X01/primary_doc.xml), [AVNS, May 15](https://www.sec.gov/Archives/edgar/data/1650149/000119312526225936/xslFormNPORT-P_X01/primary_doc.xml), [CRWD and ZS, February 25](https://www.sec.gov/Archives/edgar/data/1976322/000089418926005249/xslFormNPORT-P_X01/primary_doc.xml). Evidence must predate the target filing; current mappings are not retroactively assumed valid.

The expanded replay uses five managers, ten 13Fs and 89 source rows. JIA's [July 16 Q2 filing](https://www.sec.gov/Archives/edgar/data/1966595/0001966595-26-000008.txt) and [October 6 Q3 filing](https://www.sec.gov/Archives/edgar/data/1966595/0001966595-26-000009.txt) add 46 rows. Existing Tetrad, Samson, Stephens and Lakehouse pairs are documented in the [earlier report](direct-13f-publication-2026-10-08.md).

| Result in copied baseline / SQLite memory | Count |
|---|---:|
| Inserted filings / aggregated holdings | 9 / 77 |
| Historical filings, no new alerts | 5 |
| Current pairs processed | 3 |
| Waiting for mapping / held for value discrepancies | 1 / 1 |
| Position-change records / symbol summaries | 30 / 30 |
| Institutional activity records / public feed events | 1 / 0 |
| Synthetic monitoring alerts / email deliveries | 0 / 0 |

Samson's mapping hold is resolved. The pipeline derives WBD's reported 140,000-share reduction and reported exits in AAUC, AVNS and CTAS. AVNS creates an activity record with materiality 73, below the unchanged public-feed thresholds. JIA's CRWD share count rises by 126,500, but holding value rises by $6,777,171 (16.57%), below existing material-change criteria for its holder weight. These are reported quarter differences, not verified executions; corporate-action reconciliation for this pair is not established. No thresholds or quality weights were changed to manufacture alerts.

Stephens still lacks three mappings. Lakehouse Q3 remains held for eight corroborated unit discrepancies, without rescaling. Tetrad yields three unchanged-share records and no false events. Real qualifying public-alert coverage remains open. Because this replay produced no qualifying public event, its monitoring/digest branch did not execute; earlier synthetic builder checks remain the evidence for that behavior.

Final local receipt: `artifacts/direct-feeds/13f-identifiers-2026-10-08/guarded-pairs-final.json`. Full database state, including publication receipts, is identical on repeat: SHA-256 `437c386df26206f3ff01bc0dcf3ae4c7e1e497cea0bae5925d3fc337c1e05eca`. Identifier manifest time: `2026-10-08T18:56:45.077565+00:00`; offline replay reproduces all ten documents exactly. Artifacts are local-only; these counts/limitations are the durable record.

## Checks and remaining work

Final combined run: **205 passed, one deselected**, ten test files, Python **3.14.2**. Covers both workers, parser/evidence rules, batch/out-of-order retry, changed/deleted public evidence, canonical drift, atomic rollback, ownership/pause/cache lifetime, manifest escape/checksum controls, institutional ingest/snapshot/correction regressions and existing monitoring/daily/watchlist/prepared Top Stocks checks. The disabled job cannot open the database or manifest. Two existing FastAPI deprecation warnings remain. This is not a full-suite or production-Python-3.12 validation.

The excluded `test_institution_profile_endpoints_are_locked_until_pro` is the previously reproduced HEAD failure described in the earlier report. A pre-existing indentation error in `test_institutional_ingest_job.py` prevented collection and was corrected without changing its assertion. Five dummy-session fixtures now use isolated SQLAlchemy sessions so ownership checks execute normally.

No commit, deployment, production schema/data/source change, customer email, purchase or FMP cancellation. Before activation: baseline repair, remaining writer/maintenance review, broader mappings/amendments/corporate actions, durable reviewed identifier corpus, a real qualifying alert, and observed scheduled collection/publication. Congress collection, fresh prices/fundamentals/rankings, reduced research scope and no-send digest validation remain separate gates. Starter activation remains unresolved; FMP stays on and the execution goal remains active.


## Current production baseline and prepared evidence

October 8 Pacific / October 9 UTC. The database-enforced read-only export at 02:23:26 UTC captures the current staged institutional corpus, retained mappings and relevant canonical/derived state. Queries use bounded statements and indexed identity filters. Baseline SHA-256: `6a8d7157a7ce8b7805a9c9a6e4985a542c6059e5985aa2b1732fcc8280853482`. An additional export binds 186 actual parent filings to mapping rows, SHA-256 `f02d9b8dc557eebf5d22d6766acc7ec567159027035329716b281d4d657e5cf2`. This fixes an isolated-fixture ID collision without changing production or weakening reconciliation. Future exports include these parents directly.

All eleven October 7 Q3 filings with fully mapped current equity rows were selected before evaluating changes. Eleven unamended Q2 SEC submissions were captured with identified, rate-limited requests and verified against source identities/counts. The combined 22 submissions contain 960 source rows. Local rehearsal loads the actual retained mappings and canonical state, blocks transport/mail, and preserves the full state on repeat. It inserts 20 filings/937 aggregated positions, derives 110 changes and 110 summaries, and creates nine activity records. Ten prior filings are historical/no-alert imports; six current pairs publish derived state, four wait for complete equity mapping. Two Steginsky filings are held for independent peers indicating roughly 1,000-fold value discrepancies. No rescaling occurs.

There are **zero qualifying public feed events** under unchanged thresholds, so this real-source replay does not exercise monitoring/digest builders. The four mapping waits are Searcy, KWMG, MFA and Baring. Current-only mapping completeness did not guarantee prior/current pair completeness. Institutional ownership remains FMP and production has no Q3 canonical filings in this captured baseline.

Repeatedly parsing the same large SEC/N-PORT corpus made the batch too slow. `Prepared13FEvidence` now parses it once per bounded batch into immutable serialized records, returns fresh copies, filters identifier availability for each target date, and checks raw checksums/metadata/URLs on reuse. It retains source and peer holds, has no process-wide cache, and changes no output or scoring policy. Prepared first/repeat completes in **75.99 seconds** locally. The uncached replay and prepared replay agree on **every report field except the independent run timestamp-dependent state hash**. Prepared repeat hash: `df28fc25b1533484929f4307b56197f58a8f66551fa51e746ca1585ebd634ea9`; uncached repeat hash: `ff02a1e4d287745f23f7ee16f873e9f654864c82afeee226087c9a6fb32027ca`.

Validation: **55 focused tests pass in primary and exact release checkout**, Python 3.14.2. Coverage includes availability, source mutation, no repeated peer parse, actual batch first/repeat and existing publication/evidence/worker regressions. No production-Python performance claim. Scripts: [export](../backend/scripts/export_13f_cutover.py), [rehearsal](../backend/scripts/rehearse_13f_cutover.py). Source artifacts remain local; this report retains the evidence and limits. Optimization is prepared but not yet deployed. No institutional production writes, source switch or customer email. Next: bounded prior-quarter acquisition, durable identifier corpus, wider pair/qualifying-alert coverage and guarded scheduled rollout.


### Bounded prior collector

The explicit `collect_direct_13f_priors` job accepts at most twenty reviewed current document IDs, shares the existing collector lock and requires shadow collection mode. It reparses checksummed current sources, verifies the SEC company history/current accession, requires one original prior known by the target filing date, and validates the downloaded prior identity/period/counts. Missing recent history or amendment conflicts remain held. It closes the database transaction before HTTP and rechecks current source state before staging. Captured history and prior original bytes have durable revisions; existing prior metadata/reconciliation is preserved. It never writes canonical holdings/events or sends mail. No automatic institutional schedule is enabled by this job.

Eleven focused tests pass, including refusal cooldown, conflicting identities, historical availability, wrong periods and repeated source reuse. Replay against all eleven captured real histories/prior sources retains 33 revisions (eleven current, eleven histories, eleven priors) with identical result identities on repeat, no network, public writes or email. This collector remains local/prepared pending release and a live staging receipt.


### Shared identifier evidence

The reviewed ten-source manifest now lives in `backend/config/sec_13f_identifiers.json`. The explicit `stage_13f_identifiers` job verifies the pinned raw SHA-256, SEC header/XML identity and original N-PORT type before writing shared staging revisions. It reuses unchanged sources; drift requires explicit reconciliation. The publisher can load these shared originals with `--staged-identifiers`, retaining per-target availability filtering. No filesystem volume affinity, runtime download or silently changing ticker provider is required during publication.

Offline staging of all ten actual source files stores **43,864,880 bytes and 977 qualifying identifier observations**, preserves exactly ten revisions on repeat and loads the identical originals. Five additional focused checks pass for hash/URL/duplicate-manifest rejection and shared-state reuse. No live database installation or publisher activation has occurred yet. The release package requires a live staging/repeat receipt before institutional source selection.

Final combined release validation: **71 passed**, seven focused test files, Python 3.14.2; report links and `git diff --check` pass. No full-suite claim.


## Approved release and live staging

[PR #484](https://github.com/moore11j/congress-tracker/pull/484) was created after automatic approval review rejected a direct push to the default branch as an unreviewed rollout risk. The owner then explicitly approved merging and backend deployment. Merge `f5921fc92251fb8b88d421ee97f2753bc737ed01` deployed through [workflow 37877089446](https://github.com/moore11j/congress-tracker/actions/runs/37877089446), which succeeded. All four machines use that image. At October 9 03:00:41 UTC, ten runtime file hashes and unchanged source ownership verify on Python 3.12.15. No additional approval is pending for this release.

The PR's secret scan and Vercel preview pass. Frontend/backend dependency audits report existing vulnerable pins: five frontend findings across Next.js/transitives and 33 reported Pillow 10.4.0 advisories. `frontend/package.json`, its lockfile and `backend/requirements.txt` are unchanged against the base. These are unresolved security limitations, not passing checks or regressions introduced by this package. The explicitly approved rollout proceeded through the normal workflow without disabling audits or using an admin override.

Live identifier installation captures all ten original sources, 43,864,880 bytes and 977 observations, exactly matching reviewed hashes. Repeat reuses all ten sources. Prior collection runs 21 and 22 each process all eleven reviewed current/prior pairs, with identical accession/document IDs and source/history hashes. Independent read-only verification at **03:04:52 UTC** confirms **32 documents and 32 revisions** (ten identifier originals, eleven prior submissions and eleven histories), verified original bytes and shared identifier loading. Institutional control remains unselected/default FMP, with zero institutional publication receipts and zero Q3 canonical filings. This is durable staging validation, not institutional cutover. No validation email was sent.

The four pair mapping holds involve REAX CUSIP 75585H206, CBRS 15675D103 and Q/Q-W 74743L100. A narrow current database query finds earliest retained mapping dates of 2023-08-14 for REAX, 2026-07-15 for CBRS and 2026-02-14 for Q. The original export intentionally retained one real representative per mapping without backdating it, so earlier REAX availability was underrepresented. The Q-W candidate comes from the identifier evidence rather than the current database query. A fresh complete mapping baseline and date/class-bound reconciliation are needed before claiming these holds resolved.

Research leads only, not installed mappings: [DuPont's SEC-filed announcement](https://www.sec.gov/Archives/edgar/data/2058873/000119312525240313/d21160dex992.htm) describes expected Qnity when-issued/regular trading in October/November 2025; [a later N-PORT](https://www.sec.gov/Archives/edgar/data/356476/000119312526358658/xslFormNPORT-P_X01/primary_doc.xml) pairs CBRS with its CUSIP but was filed too late to establish availability for Baring's July 8 prior. Do not substitute reporting-period dates for filing availability.

Next: accurate prior/mapping baseline, additional real qualifying public-alert/no-send coverage and unattended prior-quarter/publication scheduling. Neither complete institutional coverage nor global FMP retirement is established.
