# Fair institutional publication, October 11, 2026

## Problem and repair

The institutional publisher previously handled either new filings or held/waiting retries. Separate scheduled invocations would scan the complete source inventory twice, while selecting only one mode would leave the other backlog unprocessed.

The optional `--include-waiting` mode shares a single evidence scan. It reserves half the target slots for rotating retries and the remainder for new filings, gives unused slots to the other group, and publishes new filings first so an arriving prior can unblock a waiting current filing immediately. Retry progress uses the existing generation-bound compare-and-set cursor. At least two slots are required; retry-only and combined modes are mutually exclusive. Existing default and retry-only behavior remain unchanged.

Disabled publisher startup now stops before importing database, ORM or source parser modules. Enabled source ownership, connection-pressure guard, manifest checks, per-filing transactions, complete peer evidence, value holds, target byte bound, duplicate prevention and partial exit status remain intact.

## Validation and release state

Local Python 3.14.2: 39 batch/CLI tests pass, plus the unchanged 75 institutional streamed-evidence, worker and publication tests passed in the preceding expanded run. These cover one-scan prior arrival/current publication, event identity on repeat, both backlogs progressing, capacity refill, rotating cursor, source generation/CAS, invalid modes and disabled startup importing no database or service modules.

The first sandbox run had three temporary-directory permission errors and an incorrect fixture expectation: newly held incoming filings also enter the rotating retry queue. An escalated run resolved permissions; the corrected test verifies the original three held filings all receive a retry while new filings keep arriving. A subsequent 39-test batch/CLI run passes. This is focused coverage, not a full-suite claim.

Captured 718-original mixed-batch validation is running; no completion claim yet. The separate full streamed cohort replay remains running. No publisher schedule or source activation is included in this repair; live capacity, unattended operation, coverage and alert-consumer acceptance remain gates. No frontend changes, subscriptions or resource changes.

## Captured-source blocker and complete reference repair

The actual mixed batch exceeded the existing 5,000-identity reference loader bound. This is a real retained-product blocker, not a reason to shrink the acceptance batch. Reference reads now use chunks of at most 5,000 scope keys, with a 100,000-key complete-scope guard and the existing 20 MB original-evidence limit applied across every chunk. The per-filing worker retains its 5,000-document and 20 MB limits. Every selected reference is still hash/URL/identity verified before publication; there is no truncation, HTTP request or symbol-identity relaxation.

Tests cover 5,501 scopes spanning multiple queries, cross-chunk total bytes, late-chunk tampering and explicit scope rejection. The first rehearsal helper failed on a missing import after its initial batch; the corrected second attempt reproduced the real reference bound before mixed publication. The third attempt is running. No failed run touched production.

## Actual-source acceptance, October 11, 22:12 UTC

Corrected replay completes against all 718 staged originals in isolated SQLite with HTTP/email transports forbidden. The initial 100-target default batch exactly equals the previously accepted complete result. The next batch selects exactly 50 new and 50 waiting/held IDs, without duplicates, and advances the retry cursor to the last selected retry. Its 5,304 reference scopes now load completely. Result: 100 processed, 28 published, 63 waiting and nine held; source/value/mapping holds remain explicit.

Mixed-batch CPU140.016 seconds/wall141.539, RSS647.32 to656.63 MB. Peak884.05 MB occurred during fixture restoration and is not a batch-only peak. This mixed cohort differs from the earlier first 100 targets, so these timings do not establish a regression or improvement. They also do not establish production shared-CPU capacity. Receipt: institutional-fair-batch-v3-oct11.json; baseline SHA2565762d171fbf53a617b9b0681cb488b376c89950065fadd95bb002c5f38ee066b. Zero source calls, production writes and emails.

All151 focused tests pass on Python3.14.2. Automated review of f8444c6b requested memory updates; those are included. Final reference repair received direct diff review and the expanded tests/actual-source check; no claim of an automated review on the later head. Secret scan and Vercel pass; unchanged dependency audits fail. PR585 is prepared for backend release; publication remains off and no schedule change is included.
