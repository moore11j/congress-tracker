# Institutional feed reconciliation — October 5, 2026

**Final state:** backend `6c5453b2` is deployed on all four machines. Nineteen filings/12,325 positions match their SEC source totals; Amundi's five false exits are withdrawn and replaced by the verified changes below. Current institutional score inputs for the five sampled tickers are unchanged. Nine ambiguous amendments remain unresolved. The dated checkpoints below explain intermediate states, not outstanding rollout work.

## Scope and evidence

Approved follow-up to [the research integrity release](institutional-research-integrity-2026-10-05.md). The live database still classified Amundi's Q2 NVIDIA, Apple, Microsoft, AppLovin and Nebius positions as exits because the August 26 NEW HOLDINGS supplement had replaced its original report. T. Rowe Price Associates' five corresponding stored changes were already correct and are outside this repair.

The read-only audit checked all 98 active 2026 amendments' SEC covers: 70 RESTATEMENT and 28 NEW HOLDINGS. Three of the latter are 13F-NT/A notices, with zero stored positions/changes. Of the 25 holdings reports, 16 reconcile into complete snapshots; nine contain conflicting overlapping securities and remain unresolved. This is not a completeness claim for older years or the 70 restatements' contents.

All 28 NEW HOLDINGS rows have zero non-superseded activity events inside the current 30-day score window. Correcting their historical projections does not justify a current score boost or recalculating recorded alerts, forward studies, portfolio decisions or historical performance.

## Implementation

- SEC quarter resolution stops at the canonical accession; an original plus NEW HOLDINGS is combined, a RESTATEMENT replaces the base, identical repeated tables are deduplicated, and conflicting overlaps/missing base/unknown covers fail closed.
- Future amendment ingest uses this resolver. Raw supplemental/provider extracts cannot overwrite verified effective snapshots or enter change processing. Research comparisons share the overlapping-table guard.
- Snapshots retain every source accession and primary table URL in filing metadata; source rows and prior database projections are backed up by the maintenance job. Options are excluded from equity change matching.
- Withdrawn activity/feed rows retain their IDs and become superseded. Public feeds and current score inputs exclude them; current projections can be corrected without deleting historical event identities. Reprocessing a superseded original does not withdraw canonical events.
- The repair job is dry-run by default, requires the reviewed plan and source hashes, checks current state, backs up before each per-holder transaction and rebuilds the following quarter when needed. Missing prior-quarter coverage does not become fabricated new positions. It sends no emails and writes no strategy, prediction, alert-delivery or return records.

## Verified source candidates

| Filing ID | CIK | Canonical accession | Effective SEC rows | Snapshot SHA-256 |
| --- | --- | --- | ---: | --- |
| 23 | 0001998597 | 0000902664-26-002957 | 11 | `b29efdad29056d811cd9c40721db786ecd4b0d1d88c59ee46f40bab34100cafd` |
| 57 | 0001535784 | 0000908834-26-000288 | 191 | `8b86696aaf92c3cebc6488c03d59469e91ffb5cb2b51163f845063403dc8748a` |
| 91 | 0001721757 | 0001721757-26-000009 | 462 | `dcceb9e2c74286f50181bbd4722c48ad608e39fa19a36fc5310f9692dba2d5f7` |
| 106 | 0001566307 | 0001566307-26-000071 | 67 | `583be90a28f072665dab3cdfa279d488a19e059d2cc5b12ca3be59bd7274ecfb` |
| 187 | 0001632833 | 0001632833-26-000004 | 2 | `01cf9f10c737148707bfb51d88870039b5e9cbe9a046179ed1b5ee8743748940` |
| 257 | 0001738126 | 0001738126-26-000005 | 7 | `8d9a6fdba427f36c710c29639cc52b4b6974ddb121833a61481d1a3a281854a0` |
| 594 | 0001736225 | 0001736225-26-000007 | 1495 | `1edd146af61f05f6dfc832e194578c6f891994849214e6b44b87eab669e8fb4b` |
| 1142 | 0001682475 | 0001682475-26-000004 | 22 | `0a243de80e39bf37396caaa96f9e842074bba3132c4bb07a9c063d1470f99fa1` |
| 1171 | 0001535784 | 0000908834-26-000442 | 213 | `e3b5ebe4865adc973513ee1f73cf6fe67a8eb4de99027ec7c296da5e329e3c8f` |
| 1181 | 0001024896 | 0000929638-26-003170 | 334 | `eb69e42c297a9311289c68cb3ee4071f965b381d952825ad40d5a4852de142ac` |
| 1201 | 0001847739 | 0001847739-26-000004 | 21 | `c873d356fa68375703c4105db80bce8d36567d1e283d8979153728dbe3833bca` |
| 1209 | 0001546007 | 0001546007-26-000007 | 1159 | `d8929f13d676de86996f2f208e5f0bf7e5dab1d79485e393a0d7d8c4c21e9d44` |
| 1240 | 0000011544 | 0000899140-26-000955 | 344 | `7410ffd5c952a886bf04e50dc443ec4f2b94b37cb3bbc0d36dac7ba36a23795d` |
| 1750 | 0001462245 | 0001085146-26-000657 | 3798 | `2d080e409ff3f13872075f8522fb53293be4a2736fc4f2e316de4b5f8805918d` |
| 2646 | 0001169581 | 0001062993-26-004449 | 48 | `3398f7299e299037f50f0793e707e61ad77b5ed7504772dd24c7bb5e1082eb1a` |
| 2875 | 0001330387 | 0001172661-26-004047 | 5530 | `2bdc42fa858adf63e33ff459c5fa54a21feb97663d52a674f317f8f31f017ed1` |

## Held for source review

Conflicting overlaps cannot safely be treated as either additions or replacements merely from a NEW HOLDINGS label. Existing records are not silently replaced with an invented total. Future refreshes fail closed for these conflicts.

| Filing ID | CIK | Accession |
| --- | --- | --- |
| 103 | 0001103887 | 0001103887-26-000005 |
| 1191 | 0001513193 | 0001062993-26-004507 |
| 146 | 0001791827 | 0001791827-26-000004 |
| 264 | 0001491072 | 0001193125-26-229328 |
| 27 | 0001911876 | 0001911876-26-000019 |
| 271 | 0001952142 | 0001420506-26-001177 |
| 44 | 0001009012 | 0001420506-26-001189 |
| 77 | 0001351991 | 0001140361-26-024580 |
| 88 | 0001632968 | 0001632968-26-000003 |

## Checks and rollout

Local Python 3.14.2: the institutional/research suite ran 88 passing tests and one unrelated entitlement assertion failure (public holder name expected null). That same failure was reproduced with the unchanged HEAD service. After adding the missing-baseline regression, all nine focused snapshot/repair tests pass. The changed amendment fixtures explicitly represent verified restatements; raw-amendment rejection has separate tests. No full-suite pass is claimed.

Implementation is locally verified; deployment and production repair receipts will be recorded below. The reviewed 16-source bundle is local audit evidence, not yet a claim of applied repairs. Local read-only evidence lives under ignored `artifacts/institutional-repair-2026-10-05`; durable identities, checksums and limitations are recorded here.

### Dependent-quarter validation and final guards

Six dependent Q2 reports were fetched from SEC before applying anything. Stored rows for filings 2379, 2414 and 2443 match the SEC totals. Filings 2675, 2829 and 2868 have discrepancies and were added as source-verified originals, bringing the repair plan to 19 snapshots. Several Q2 amendment rows also stored values at one-thousandth of the SEC dollar amounts; the snapshot repair uses the exact dollar-denominated SEC tables, not an inferred blanket multiplier. Mixed-case SEC CUSIPs are normalized before mapping, matching or removing old positions; matching position IDs are retained.

The public provider-ranking fallback now recognizes reconciled snapshots as authoritative. A fetched in-memory snapshot is consumed once and its rows must match its checksum. Eighteen relevant ingest/feed/source checks passed; the final snapshot and prior SEC correction suite passes 16 tests, including CUSIP identity retention. An additional unchanged ingest-job test file cannot collect because of a pre-existing syntax error at line 149; it is not counted as passing coverage.

Rollout `f16daa1e` and follow-up `e53c72db` completed successfully. The initial 16-filing production dry run matched all identities and produced plan hash `ec920eeb8b18fe1962f5098061ad8f5a8714d0e8bf386adac92b28cdbd21ea09`; no data was changed. The expanded 19-source plan and final production application remain pending at this checkpoint.

### Production maintenance checkpoint

Final identity-safe source installation deployed as `dd6eeb5e`; [workflow 37412037843](https://github.com/moore11j/congress-tracker/actions/runs/37412037843) succeeded and all four worker images plus readiness/database checks were verified. The expanded production plan matched 19 targets with hash `362e5ae0426e7d4ea57d2d6f76e0e0856425cabbace74c7b00f30cad089f402a` and bundle hash `4aeed696dde625de5c1f22eacded273537919b08fd3e90c40abef05f8f2dcd2d`.

The first run revealed an expensive split-part event-ID scan. It was stopped by exact maintenance-process identity; filing 23 completed before the stop, while the interrupted transaction rolled back. Release `e21b620d` replaces that lookup with exact canonical source IDs against the existing index; 11 focused tests pass. [Workflow 37412549744](https://github.com/moore11j/congress-tracker/actions/runs/37412549744) succeeded. The same reviewed plan resumed and reported filing 23 already applied. Backups remain on the existing worker under `/data/institutional-reconciliation`; no schema migration or new compute service was added.

Before correction, the actual institutional score sources for NVDA, AAPL, MSFT, APP and NBIS were all absent/neutral with zero contribution and latest filing date August 26. This is an observed current-input result, not merely an inference from individual event ages. Their after-state and full position comparisons remain to be recorded.

The resumed run committed filings 57, 91, 106, 187 and 257 in addition to 23. Five of these lacked a stored adjacent-quarter baseline; no fabricated new-position changes were generated. The run was stopped before the large remaining reports while scope was narrowed to the changed manager plus aggregate events. Release `80303399` adds that bounded rebuild and passes 31 relevant tests, including preservation of an unrelated manager's event. Maintenance backups were narrowed to the same mutation scope in `91cb80f5`; all 12 focused snapshot/repair tests pass. The next resumption uses the identical reviewed plan and skips the six completed filings.

Release `91cb80f5` [workflow 37413236322](https://github.com/moore11j/congress-tracker/actions/runs/37413236322) succeeded; all four machines were independently verified running its image, and `/ready` returned API/database `ok`. The resumed plan has also committed filings 594, 1142, 1171, 1181 and 1201. An intermediate independent comparison found zero share/value differences for every completed snapshot it observed. This checkpoint is not the final all-target receipt.

The shared cron worker made slow but observable query progress without a database lock wait. Filing 1209 also committed before the maintenance process was stopped by its exact identity. The same plan was then resumed on the existing idle video worker, whose configuration already included one performance CPU; no resource setting or subscription changed. It skipped all twelve completed reports and continued the remaining seven. Their backups are initially under `/tmp/institutional-reconciliation` and must be copied to durable storage before the final receipt.

### Amundi source comparison

The original Q2 report and NEW HOLDINGS supplement must be treated as one effective holdings report. The five prior `exit` projections are contradicted by the SEC share counts:

| Symbol | Q1 shares | Q2 shares | Share change |
| --- | ---: | ---: | ---: |
| NVDA | 133,768,018 | 129,523,550 | -4,244,468 |
| AAPL | 73,082,616 | 70,758,208 | -2,324,408 |
| APP | 1,115,419 | 1,332,633 | +217,214 |
| MSFT | 41,675,076 | 46,694,778 | +5,019,702 |
| NBIS | 1,343,954 | 2,002,187 | +658,233 |

Primary evidence: [Q1 information table](https://www.sec.gov/Archives/edgar/data/1330387/000117266126002409/infotable.xml), [Q2 original information table](https://www.sec.gov/Archives/edgar/data/1330387/000117266126003278/infotable.xml), [supplement cover](https://www.sec.gov/Archives/edgar/data/1330387/000117266126004047/primary_doc.xml). These are quarter-end share differences, not real-time transaction claims.

### Remaining coverage limits

The nine conflicting supplements above remain unresolved and unchanged. The 70 restatement covers were classified, but their full position contents were not audited in this task. Older-year amendments and provider-wide value-unit parity remain outside the verified 19-snapshot repair. Existing historical predictions, delivered alerts, strategy decisions and returns are not rewritten. A complete-data or complete-history claim would be unsupported.

### Completed source repair and mapping follow-up

All 19 filings committed successfully. An independent database/source comparison verified every position's aggregated shares and reported dollar value, with zero differences across all 19 snapshots. All five Amundi false-exit activity records are superseded with `feed_visible=false`; their five feed records remain stored, and zero pass the live router's supersession filter. Backup audits found no missing activity/event identities or altered feed source IDs: the cron backup set covers 30,558 activities/5,193 events, and the final seven backups cover 9,620 activities/3,869 events (these sets can overlap).

The actual current institutional score-input objects for NVDA, AAPL, MSFT, APP and NBIS are identical before and after: absent/neutral, strength/quality/contribution zero, freshness 41 days. This is not a claim about a newly improved total Confirmation Score.

The final seven backups were downloaded locally and uploaded to the persistent cron volume as `/data/sec-repair-20261005-remaining-backups.tar.gz`, SHA-256 `f44e0ce4e2605b83ee3a70ba94357df29f97caaca9371d7d74ea9d54575cdeee`. Earlier backups remain under `/data/institutional-reconciliation`.

Verification caught one additional mapping conflict: Amundi's correct 2,002,187 Nebius shares had no normalized ticker because stored CUSIP `N97284108` mappings contain both NBIS and former ticker YNDX. [The issuer confirms](https://nebius.com/newsroom/nebius-group-n-v-announces-official-name-change-and-new-ticker-symbol) the ticker change effective August 21, 2024. Release `6c5453b2` resolves only that exact CUSIP/alias set for Q3 2024 or later; earlier periods and unrelated ambiguous mappings are preserved. Thirteen focused tests pass. A separate one-position dry run identifies position 2108254 in filing 2875; plan SHA-256 `4ccf8d7317d06883ea1e4635d54776fc13f5be31187e28a0ed28fb1e9bb8475b`. Deployment/application verification follows below.

### Final receipt

- [Workflow 37416657729](https://github.com/moore11j/congress-tracker/actions/runs/37416657729) succeeded for `6c5453b2`. Both API machines, cron and video workers report its exact image; API/database readiness is `ok`.
- The one-position mapping correction applied on the cron worker after verifying the durable backup archive checksum and the exact SEC share pair. Its own before-state backup is `/data/institutional-reconciliation/nebius-mapping-4ccf8d7317d06883ea1e4635d54776fc13f5be31187e28a0ed28fb1e9bb8475b.json`.
- Final independent verification again finds all 19 applied, 12,325 positions and zero source differences. All five Amundi share deltas match the table above, including NBIS +658,233; no false exit remains active for those five symbols.
- All five actual institutional score-input objects remain identical to the before-state after the mapping correction. Recorded predictions, alerts, strategy decisions and returns were not targeted by either maintenance operation.
- Re-running the original hash-guarded plan reports all 19 `already_applied`, with no further writes. The separate mapping repair also reports `already_applied`. Thirteen final focused regression tests pass on Python 3.14.2; the broader baseline failure and unrelated collection error remain documented above. No full-suite claim.
- No new resource/subscription, email, article or social publication. Shared workspace edits were preserved. The nine source conflicts, older history and provider-wide value-unit audit remain follow-up work.
