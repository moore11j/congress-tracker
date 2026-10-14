# SEC daily-index closure handling, October 14, 2026

## Problem and repair

Production collector run476 at15:47 reported Form4/13F discovery errors for master.20261012.idx (HTTP403). The [official EDGAR calendar](https://www.sec.gov/submit-filings/filer-support-resources/edgar-calendar), reviewed October14 and last updated April28, lists October12 as a closure day. Current code excluded weekends only. It continued to later dates; this was not proof of a total SEC outage.

The collector now excludes the eleven SEC-published 2026 closure dates and saves an additive sec_index_calendar receipt: reviewed version/source, skipped closed/weekend dates, requested weekdays and unreviewed years. Other weekday requests/errors are unchanged. Years beyond the reviewed calendar retain requests and explicitly list the missing review; review the 2027 SEC calendar before year end. This is EDGAR's filing calendar, not the exchange calendar. Existing documents remain eligible for processing even when the discovery window is closed. No source flags, schedules, freshness thresholds, transport retries, publication or canonical identities change.

## Verification

The original first regression fails because it requests January1's missing index. Linux Python3.12.3 then passes66 checks across calendar, SEC collection, scheduling/retry and cover quarantine. Cases cover all eleven official dates, observed July3, Columbus/Veterans Day, Good Friday as a request day, real weekday403s, unknown-year visibility, mismatched filing dates, prior backlog processing and repeated identities.

Two bounded public index captures support an isolated baseline/candidate replay:

| Date | Bytes | SHA256 |
| --- | ---: | --- |
| October9 |288374|2e558fd5812c569768927b3e91a38b36c826105b63a8f4fbf0b4122074f53d70|
| October13 |403340|a17ef25c4d1e8417352d89336cdfc889d1d15bc9b51c5afb4fd958adaf1468a1|

Both baseline9d7280e1 and candidate discover814Form4 and494Form13F documents,1308total. Complete document-state hash matches d10dbfdd79af21c2ff282f608e14be85cd85a92d8c64a366873e20447d949d32; both repeats preserve it. The replay reproduces the previously observed holiday403 from a stub and uses the two captured open-day originals without network; it is not another live holiday request. Requests fall3to2 per pass, only the two holiday-related error entries disappear, and canonical event/insider/institutional tables remain empty. Primary artifacts/sec-calendar-oct14/replay.json retains results and source hashes.

Release and ordinary production observation remain pending at this checkpoint. FMP stays enabled; institutions/core/transcripts/all consumers/Massive/global-egress/billing and sustained capacity remain separate gates. No new hosting cost or subscription.


## Deployment recovery verified, October14 16:13 UTC

PR632 headfb991602 merged as9ec6b4d727fee0ef9394f8a085952add7c132335. Required CI tool confirms backend workflow38595666178/job115961644321 completed successfully. Runtime receipt calendar-runtime-1791994363.json verifies13 normalized hashes and the deployed October9-13 calendar projection: requests October9/13, skips the reviewed October12 closure and weekend. Process queue is empty at16:12:43; this is not enrichment-backlog or sustained-capacity proof. Ordinary16:47 collector observation remains pending. No deployment was repeated during recovery.

Exact-head CI had unchanged Pillow and frontend dependency-audit failures, with secret/Vercel success; no full-suite or full-CI pass claimed. Prior cached inventory confirms October13 was collected despite holiday errors, with separate canonical reconciliation/amendment/ownership holds remaining. Review2027 official calendar before year end. FMP stays enabled and remaining migration gates are unchanged.
