# SEC supplemental cash-flow failure isolation, October 15, 2026

## Confirmed defect and change

NVIDIA's free SEC fundamentals require three supplemental original filings for the company's reviewed cash-flow definition. A transport error in that optional step previously escaped the helper, aborting both source-cache preparation and selected core acquisition even when revenue, margins, equity and other financial facts had already passed their own checks. The existing transport-error test explicitly expected this exception; it did not verify preservation through a complete refresh.

The helper now removes all cash-flow values and calculation labels before attempting the supplement, as before, then returns the independently verified financial fields on an expected source/stream failure. Cash-flow evidence records unavailable, a stable sanitized reason and retryable=true. No partial sum, old value or inferred zero survives. A whitespace/malformed optional HTML response is an explicit evidence failure rather than an uncaught parser exception. Unexpected programming errors are not broadly swallowed.

The successful calculation, reviewed source identity, original hashes, period matching and cash-flow methodology remain unchanged. This is reliability work, not a new cash-flow definition, new issuer coverage or proof that the remaining EBITDA component gaps are solved. Microsoft's aggregate D&A reconciliation remains open.

## Preserve source backoff and ordinary recovery

A partial cache must not silently defeat the SEC worker's existing stop-on-transport-failure behavior. The warmer records the retained metrics, marks the run partial and stops the remaining SEC scopes when the supplemental transport-failure marker is present. Independent Finnhub preparation retains its existing behavior. The existing broad SEC failure backoff is20hours; the partial cache becomes retryable at that same boundary. No new timer, schedule, batch size, provider budget or shorter cooldown is introduced.

Only NVIDIA's source-preparation version gains an acquisition-recovery suffix. Its existing statement-policy version is preserved, and the cash-flow calculation-methodology identity remains unchanged. Other issuer versions remain unchanged. Planning the new version does not import the cash-flow parser. Public source selection remains FMP and no canonical financial/event history is rewritten.

## Validation

Initial Python3.12.3 selection:77 tests pass. The expanded run passes265 checks with three expected cache-version assertion failures; those assertions now verify the exact composed statement/recovery version. All36 affected tests pass on rerun, closing those three failures:268 distinct selected checks pass across the broad and corrected focused runs, with no application change between them. Receipts: artifacts/cash-flow-isolation-final-oct15.log and cash-flow-isolation-corrected-oct15.log. A separate Windows3.14.2 rehearsal of the isolated runtime probe passes. New fault tests cover failure at each of the three filing requests, exact preservation of every unrelated field and original input object, removal of preexisting cash-flow values, sanitized streaming failures, malformed HTML, full selected-core acquisition, isolated real preparation-cache persistence/repeat/recovery, and ordinary batch-stop/lease release/backoff behavior. Recovery restores the complete cash-flow result and retains every other value. No network requests, production records, customer deliveries or canonical financial/event writes occur in these tests.

The original mathematical/source fixture suite still applies. This change does not claim fresh original-source numeric parity, complete SEC coverage, sustained production capacity or all-alert readiness from synthetic failure checks. Final release/runtime and ordinary recovery observation remain pending. Existing aggregate/CAT/institutional production-verification permissions remain pending and their rejected operations were not retried.

Previous goal turn deployed PR674's default-off Finnhub preparation and was concrete progress. This turn addresses a separate confirmed acquisition failure. Full migration remains active: core coverage, institutional publication/current-cohort consumers, transcripts/analysts/valuation, all scheduled consumers and sustained capacity, final Massive, zero FMP traffic and billing remain open.
