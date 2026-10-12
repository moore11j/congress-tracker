# Align institutional reference planning with publication

October 12, 2026. Implementation and read-only production comparison are complete; release is pending. Institutional publication remains disabled and FMP remains selected.

## Confirmed cause

The identifier collector considered only canonical mappings from the two requested reporting quarters. The publisher considers all independently sourced mappings known by the filing date, excluding positions whose parent uses reference evidence. The mismatch hides older ticker conflicts from collection. Production evidence confirms six current candidates with older aliases: CCIF/VCIF, UPBD/RCII, IGPT/PSJ, ACIC/AAIC, SPHR/MSGE and GOVI/PLW. Collection treated each recent singleton as complete, while publication correctly continued waiting for dated identity evidence.

The same quarter restriction also queued redundant requests for securities already resolved by independent older mappings. Those unnecessary entries filled the 5,000-scope window and excluded other needed identifiers. Separately, accepting reference-derived positions as independent mappings could suppress collection for another quarter after direct publication starts.

## Repair

The planner now uses the publisher's independent-parent predicate and all historical mappings, retaining the actual filing-date boundary for each source. It does not change identity resolution, accept ambiguous symbols, backdate observations or publish records. Plan version5 expires the old cached candidate selection. The500-document scan,5,000-scope cap,15-minute plan cache,two-request budget,daily attempt retention,provider cooldown and serialized scheduling are unchanged.

## Evidence

Three regression cases fail on the old implementation: an older symbol conflict incorrectly skipped, a reference-derived position suppressing another period's collection, and an independent older mapping generating a redundant request. All70 reference/warming/preflight checks pass after the fix on local Python3.14.2. Existing source-date, source-status, cooldown, quarter fairness, scope cap, source rotation and no-public-write checks remain included. No full-suite claim.

A first offline audit restores718 checksum-verified originals and579 captured reference documents. It admits only8 of129 not-yet-captured candidate scopes. This local snapshot contains fewer canonical mappings than production and is not valid evidence of production omissions; its limitation prompted the actual read-only comparison.

Production read-only audit at01:22:23UTC covers both500/218 source windows. The current code admits84 of136 investigated scopes;52 are absent. A bounded follow-up finds seven already verified and111 historical mapping rows across the omitted CUSIPs. The proposed code, executed only in an isolated subprocess with read-only database access at01:25:20UTC, admits all129 remaining scopes. The seven excluded keys exactly equal the already-verified keys. This gains45 useful admissions and removes enough redundant entries to reduce the first window from its5,000 cap to1,848 and the second from428 to394. These are planning counts, not verified new identities or completed filings.

The two-window application CPU measurements are3.068seconds before and2.995seconds after; wall times5.025 and13.262seconds differ and do not prove a latency improvement. Each query has a10-second statement limit, with background-pressure and collector guards retained. These separate read-only observations do not establish sustained capacity. No provider requests, database writes, emails or source activation occurred.

Receipts under artifacts/direct-feeds/free-replacements-2026-10-09: reference-planner-coverage-oct12.json, reference-queue-1791768070.json, reference-plan-coverage-1791768148.json, reference-omission-causes-1791768189.json and reference-fixed-plan-1791768333.json. The queue receipt also shows a nearly exhausted cached older-source window; no early-rotation change is included because candidate parity is the confirmed defect repaired here.

## Remaining gates

Review, deploy and verify exact runtime code plus ordinary collection under version5. Full574-source replay46444 continues independently with a frozen579-reference input and unchanged publication code. Completion, provenance/duplicate/consumer repeats, actual bounded publication cost, source coverage and security transitions remain required before institutional selection. Senate access, core financials, transcripts, all monitoring/watchlist/daily/weekly/ranking consumers, final Massive Stocks entitlement checks, zero global FMP traffic and billing cancellation remain open.
