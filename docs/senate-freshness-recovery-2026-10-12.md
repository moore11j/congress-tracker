# Senate stale-source recovery, October 12, 2026

## Purpose and verified behavior

The free secondary Senate source sometimes updates less frequently than the existing24hour freshness guard. Validate safe delayed recovery without weakening that guard, rewriting historical evidence or duplicating feed/alert identities. The public official source still returns403; this does not authorize bypassing its access controls. Quiver is not used.

Two actual revision-pinned2026 snapshots span39.8994hours:

| Source check UTC | Revision | Manifest SHA256 | Filings / rows |
|---|---|---|---|
| September29 23:59:17.765 | db14acb2126c0745f6b00bc525a5516a0ad8b6be | 2aa320e55c98d032ab8872356849616035196f49a64b6eac78a4c2092447e24d | 133 /2169 |
| October1 15:53:15.440 | 20478dc3be63a1416cc090b59040e412bffa1d8c | 3925e4c8c0d2cb33f39dfc98f9923e31f45c2049789f8ec1297f585e75068c65 | 133 /2169 |

Both complete checksum-verified partitions total298963bytes per snapshot. No filing is added or removed between these snapshots. Historical clocks are used solely in the isolated rehearsal; neither snapshot is represented as current production evidence. Receipt senate-historical-gap-oct12/receipt.json. The latest October11 snapshot separately contains135filings/2176rows.

## Actual saved-source rehearsal

Python3.14.2/in-memorySQLite, all provider requests and SMTP forbidden during replay. First snapshot yields82published/51held filings,455transactions/353events. At25hours after its source check, collection and publication both reject freshness and leave the entire application database unchanged. The later actual snapshot preserves each existing evidence hash and first observation, publishes zero additional transactions/events, and leaves all public state unchanged.

Staging the latest actual snapshot then adds precisely two filings, seven transactions and five stock events; the133 prior filings do not duplicate. A repeat adds zero, and all-table state settles to SHA25625aff7ccdef32bd0d41ba677f17208cfe8e782708687cbd1fe3b98b319e5aa28. Final84published/51held,462transactions/358events, explicit secondary-extraction provenance on every event, zero emails/production writes. Receipt senate-actual-freshness-recovery-1791782462.json. This proves recovery behavior for retained sources, not continuous official completeness or timely future provider updates.

## Regression and current production checks

156affected Senate preparation/publication/consumer checks pass. New tests verify40hour stale holds, unchanged original observations after fresh-manifest recovery, preserved public rows/bindings, and a changed economic amount remaining held. Initial fixture draft added an unused field; replacing it first accidentally retained the original bracket. The final test asserts its original values and changes the actual amountBracket/amountLow/amountHigh fields before validating the hold. No application guard was changed to pass a test.

Read-only production plan receipt senate-production-plans-1791782717.json at05:25:15UTC confirms both current-period filings existing with no holds and identical repeat: Fetterman two bonds, Whitehouse five stock rows. The source-owner boundary remainsOctober1 and generation4; backup publication remainsoff as of this check. The ordinary05:17 collection was already verified in the [preparation report](senate-backup-preparation-2026-10-12.md).

The complete no-send consumer extension passes: monitoring/signal/watchlist counts12/8/353 remain identical across stale-source recovery, then12/8/358 after exactly five new stock events. All358 duplicate alert attempts are refused, intraday event keys remain stable, zero emails. Full-state SHA2567ae95a9cf6f6b900a5cefc9c15a7589d4d713b7068266d31f42b0a687dd3a7a5; receipt senate-actual-freshness-consumers-1791782778.json. Its first helper failed because it treated Event as having security_id; after terminal confirmation, the helper was corrected to resolve Security by Event.symbol and rerun successfully. No source selection or configuration has yet changed.

## Acceptance boundary

Preserve24hour freshness, completed publication-day boundary, explicit secondary provenance, independent member identity and whole-filing holds. Historical data can remain visible during a source outage, but it must not be relabeled as fresh. A guarded activation can only publish current validated snapshots; delayed catch-up must retain arrival-time availability and existing alert identities. Full-source continuity, unresolved51historical holds, other FMP capabilities and global retirement remain separate gates.


## Guarded production selection, October 12, 05:30 UTC

The ordinary05:27 official publisher completes run33/exit0 before selection. Actual40hour recovery and full no-send consumer evidence above establishes safe stop/recovery behavior without assuming continuous source freshness. The selected backup remains subject to the24hour guard; a source gap will hold new publication until a valid fresh snapshot arrives, retaining previously published history.

Set SENATE_BACKUP_PUBLICATION_ENABLED=true through a successful rolling configuration update. Existing code/imagece0ce857, hourly schedule and both host sizes are unchanged. A guarded in-place operation then requires official_senate generation4, immutableOctober1 boundary, exact snapshot5769a981, the complete current-period population of exactly two reviewed filings, and both plans existing/noholds. It selects community_congressional_disclosures atgeneration5 and publishes only those two existing evidence bindings/receipts. Zero transactions/events/emails inserted; canonical SHA fingerprints remainidentical. Repeat processeszero. Receipt senate-backup-activation-1791782899.json, observed05:28:18UTC. No raw canonical rows leave production. Automatic failure handling would pause atgeneration6; it was not needed.

The unchanged hourly publisher is then manually enqueued once through the existing serialized data lane: keyfd937f3bafa7d88ca6628e54, targetrun34. It completesexit0; current state shows two backup publication receipts, collection/publicationtrue and selectedgeneration5. This verifies the actual queued job path, not yet a subsequent calendar-trigger run. Receipt direct-scheduled-state-1791782977.json; next ordinary06:27 publication and source freshness rollover remain observational gates.

55runtimehashes match, APIshared2CPU/2048MB and DBperformance1CPU/2048MB remainunchanged, queueempty. News50items0.500s, feeds0.391/0.491s, freshAAPL1.551s/NVDA1.205s. Receipt senate-selected-runtime-1791782971.json; postrestart samples are not sustained capacity proof. Existing rows retain their original official provenance, not relabeled secondary. New secondary rows retain explicit secondary provenance and publication-time availability.

Rollback is the existing generation-checked source pause or separately verified official recovery; do not restore an unguarded legacy FMP writer. No purchase, customer email, video or schedule/hardware change. Institutions remainoff, public core/transcripts/prices and complete consumer/global-FMP/billing gates remain unresolved. The new regression tests and this report do not require another backend deployment.


## Evidence PR merged,05:32UTC

[PR602](https://github.com/moore11j/congress-tracker/pull/602), head563a028ace7fc8f11b28cc7727a95a6daa563bfb, merged asfbd8c43efd8584de6fc638ea5006d732c004e354. Tests and documentation only; backend remainsce0ce857, with no new deployment. Secret scan passes; purpose-built diagnostics confirm unchanged Pillow/frontend dependency audit failures. Vercel preview status isfailed with no detailed cause exposed in CI metadata. No frontend or dependency files changed; no CI-clear claim or preview retry. The successful source activation/queuedrun34/runtime receipts remain as recorded.
