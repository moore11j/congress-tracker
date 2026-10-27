# Financial discovery retry ordering, October 27, 2026

## Problem and bounded repair

The ordinary financial worker mixed twenty-minute discovery retries and twenty-hour refreshes but sorted them by their previous attempt time. Read-only production observations at10:45 and11:25 left PAGS105th despite its overdue retry. At11:49 it was107th among116 eligible scopes. New routine refreshes repeatedly displaced pending discovery work.

The repair orders already eligible broad-universe work by its actual next-eligible deadline. Changed issuer evidence and watch/research priority remain first; the existing changed-version reserved slot remains. Uniform twenty-hour work preserves oldest-attempt ordering. Invalid timestamps have a finite deterministic fallback. Eligibility, lease/load/source-denial guards, ten-job limit, five-minute trigger, source intervals and all financial parsing/output functions are unchanged. Non-broad mode retains its existing order.

## Validation

Final frozen-source Windows checks:59 pass. Linux Python3.12 checks:105 pass, no failures/errors/skips. Receipts are `artifacts/deadline-windows-oct27.xml` and `artifacts/deadline-linux-oct27.xml`; nine source/test hashes are pinned in `artifacts/deadline-test-sources-oct27.json`.

The actual SQLite worker regression exercises overdue retry before a newly due refresh, future work held until due, normal twenty-minute retry, unchanged batch, nested lease protection, unchanged preparation arguments and zero canonical events/emails/model calls. Additional checks cover mixed TTLs, timezone offsets, uniform intervals, malformed timestamps, stable ties and priority/version reservations. Affected discovery/store/public-cache suites also pass.

Exact proposed-function evaluation against the installed f6ee88cd planner at11:49:56UTC moves PAGS107to1, AMX108to2 and DAO109to8. The ten selected slots remain ten, with the same one changed-version reservation and no displaced watched or changed-dependency scopes. NBIS remains not due. The pure result repeats exactly with unchanged inputs; execution stops before lease acquisition. No provider requests, writes or customer identifiers are involved. Receipt: `artifacts/discovery-deadline-implementation-1793101796.json`. Two sorts measured below clock resolution; this is not a performance benchmark.

Seven parser/acquisition/panel files retain the exact hashes from the preceding49-complete-source parity receipt. This change only alters scheduling order; it does not claim fresh parser coverage from rerunning an unchanged suite.

## Release and acceptance

Release pending. After exact-head CI/manual review, use the existing guarded backend workflow, verify installed file hashes/flags and observe a worker run beginning after deployment. Verify actual ordinary discovery checkpoint advancement, not merely the proposed queue position. No forced source refresh or new schedule is authorized by this repair.

Collection remains enabled, public discovery readers disabled and core fundamentals/transcripts/prices FMP. Qualification, public-reader acceptance, broad coverage, all alerts/rankings/digests, sustained capacity, institutional capture approval, Meta normal retry, Massive last and global zero-FMP/billing closure remain open. No hosts/subscriptions/sends or landing-page changes.
