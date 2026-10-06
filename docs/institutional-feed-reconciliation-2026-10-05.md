# Institutional feed reconciliation — October 5, 2026

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
