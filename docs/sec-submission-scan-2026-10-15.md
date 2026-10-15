# SEC submission scanning compatibility, October 15, 2026

Implemented and locally validated on `codex/sec-submission-scan`, not released. Production remains the prior verified `544b893b` release. Institutional publication remains disabled; core fundamentals, prices and transcripts remain FMP.

## Change and measured bottleneck

A fresh Linux Python 3.12.3 profile of complete peer preparation from all 718 checksum-verified saved originals takes 18.066 CPU seconds. Regular-expression search/findall calls account for approximately 4.56 seconds. The optimization replaces only literal DOCUMENT and XML section boundary scans with string searches. It preserves nonoverlapping section boundaries, first XML selection, whitespace trimming and complete originals. XML parsing and issuer, accession, date, amendment, count, value and numeric validation are unchanged. No new cache, truncation, source fallback or relaxed hold is introduced.

The saved baseline module exactly equals deployed `544b893b:backend/app/clients/direct_sources.py`; normalized SHA256 `434716ad0bb95541a59ddb6e94f8add5d2a8655a338ec34c397eb2549f4cc32d`.

## Local validation

170 focused checks pass on Linux Python 3.12.3. They cover submission delimiter behavior, malformed and duplicate attachments, Unicode whitespace, XML field validation, source collection/quarantine, complete peers, prepared evidence and institutional publication/batches. The new differential tests include 1,010 deterministic boundary cases and full accepted-result or rejected-error comparisons. No full-suite or production capacity claim is made.

Four complete offline passes, in baseline/candidate/candidate/baseline order, verify all 718 original documents and parsed fields/holds. Full result SHA256 is identical: `3edc19018c6f3040bd714ed70a2b6b40ad61f4015d408e9a8710eb1d938827f8`. Complete September peer preparation for 1,560 CUSIPs from the first 20 captured targets retains all 402 peer records, metadata, values and summary bytes. Its complete SHA256 is `6d5f901efd88e5353f17c4bb9efed790909a27d566166e48c62dde90345e82a9`, also matching the preceding profile receipt. These first 20 captured targets are not asserted to be the current production selection.

| Pass | Parsing CPU seconds | Complete preparation CPU seconds |
|---|---:|---:|
| Baseline 1 | 22.389 | 35.980 |
| Candidate 1 | 19.693 | 11.534 |
| Candidate 2 | 8.389 | 7.054 |
| Baseline 2 | 22.946 | 48.761 |

Both candidate passes use less CPU, but the large variation and initial overlap with the isolated test run prevent a dependable fixed saving estimate. These timings exclude database publication and do not establish production throughput or sustained capacity. The original profile and this replay use different measurement procedures and must not be mixed into one speedup calculation. Networking is blocked; there are zero source requests, production writes or customer deliveries.

Receipts in the primary workspace: `artifacts/submission-scan-tests-oct15.log`, `artifacts/submission-scan-replay-oct15.log`, and `artifacts/direct-feeds/free-replacements-2026-10-09/submission-scan-replay-oct15.json`. Original capture SHA256: `5762d171fbf53a617b9b0681cb488b376c89950065fadd95bb002c5f38ee066b`. The baseline profile is `stream-preparation-current-oct15.json` in the same artifact directory. All local test/replay processes are terminal; none was restarted for silence.

## Session recovery and remaining gates

The owner screenshot reports a historical Codex remote-turn HTTP429 retry failure. Current execution and tools work, and the account reports ordinary usage allowed with 95% of its weekly window consumed. No model, retry, account or subscription setting was changed. The historical cause and permanent service repair remain unverified. Official [error guidance](https://developers.openai.com/api/docs/guides/error-codes) describes multiple 429 causes; the screenshot alone does not identify which applied.

At 01:03 UTC, the read-only production queue is empty and has no new portfolio cycle or external receipt. Earlier failed receipts are preserved, not replayed. Receipt: `artifacts/portfolio-cycle-1792026191.json`.

Next: exact release review and runtime verification, then bounded institutional writer and scheduled capacity validation before publication. Full ordinary portfolio completion, financial definitions/coverage and propagation, transcripts, all monitoring/watchlist/daily/weekly/ranking consumers, final Massive subscription/validation, zero global FMP traffic and billing/access dates remain gates. No production deployment, source activation, recurring schedule, host/subscription change, customer email, video, landing change or FMP cancellation occurred in this validation turn.
